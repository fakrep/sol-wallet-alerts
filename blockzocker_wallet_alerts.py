#!/usr/bin/env python3
"""Hourly Solana wallet trade/transfer notifications via Helius + Telegram.

Designed for cron in a small LXC:
- fetch recent Helius enhanced transactions for one wallet
- filter to trading-relevant activity
- send Telegram alerts
- persist minimal state to avoid duplicate alerts
- append JSONL history for later analysis/PnL work
"""

from __future__ import annotations

import argparse
import base64
import html
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"
STATE_PATH = DATA_DIR / "state.json"
HISTORY_PATH = DATA_DIR / "events.jsonl"
LOG_PATH = LOG_DIR / "blockzocker_wallet_alerts.log"

DEFAULT_LABEL = "BlockZocker Trading Wallet"
HELIUS_LIMIT = 25
JUPITER_PERPS_PROGRAM = "PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu"
ANCHOR_IX_INSTANT_INCREASE_POSITION = bytes.fromhex("a47e44b6dfa640b7")
ANCHOR_IX_INSTANT_DECREASE_POSITION = bytes.fromhex("2e17f02c1e8a5e8c")
RELEVANT_TYPES = {
    "SWAP",
    "TOKEN_TRANSFER",
    "TRANSFER",
    "NFT_SALE",
    "UNKNOWN",  # Keep UNKNOWN if tokenTransfers exist; Helius may classify some DEX txs oddly.
    "CLOSE_ACCOUNT",  # Some real DEX/perp movements are classified this way by Helius.
}

BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

KNOWN_MINT_SYMBOLS = {
    "So11111111111111111111111111111111111111112": "SOL/WSOL",
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": "USDC",
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkYxEfrW6pX2C3oW": "USDT",
    "3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh": "WBTC",
    "cbbtcf3aa214zXHbiAZQwf4122FBYbraNdFqgw4iMij": "cbBTC",
}


class ConfigError(RuntimeError):
    pass


def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = RotatingFileHandler(
        LOG_PATH,
        maxBytes=2_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logging.basicConfig(level=level, handlers=[file_handler, console_handler])


def load_config() -> dict[str, str]:
    load_dotenv(BASE_DIR / ".env")
    cfg = {
        "helius_api_key": os.getenv("HELIUS_API_KEY", "").strip(),
        "telegram_bot_token": os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
        "telegram_chat_id": os.getenv("TELEGRAM_CHAT_ID", "").strip(),
        "wallet": os.getenv("WALLET_ADDRESS", "").strip(),
        "wallet_label": os.getenv("WALLET_LABEL", DEFAULT_LABEL).strip(),
        "helius_network": os.getenv("HELIUS_NETWORK", "mainnet").strip(),
    }
    missing = [k for k in ("helius_api_key", "telegram_bot_token", "telegram_chat_id", "wallet") if not cfg[k]]
    if missing:
        raise ConfigError(f"Missing required env values: {', '.join(missing)}")
    if cfg["helius_network"] not in {"mainnet", "devnet"}:
        raise ConfigError("HELIUS_NETWORK must be 'mainnet' or 'devnet'")
    return cfg


def empty_wallet_state() -> dict[str, Any]:
    return {"processed_signatures": [], "last_seen_signature": None, "updated_at": None}


def load_state(configured_wallet: str) -> dict[str, Any]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not STATE_PATH.exists():
        return {"wallets": {}}
    try:
        state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        backup = STATE_PATH.with_suffix(f".broken-{int(time.time())}.json")
        STATE_PATH.rename(backup)
        logging.warning("State file was invalid JSON; moved to %s and starting fresh", backup)
        return {"wallets": {}}

    # Migration from v1 single-wallet state to v2 wallet-scoped state. The
    # current configured wallet is used instead of keeping any project-specific
    # wallet address hardcoded in this distributable source file.
    if "wallets" not in state:
        legacy_wallet = state.get("wallet") or configured_wallet
        state = {"wallets": {legacy_wallet: {
            "processed_signatures": state.get("processed_signatures", []),
            "last_seen_signature": state.get("last_seen_signature"),
            "updated_at": state.get("updated_at"),
        }}}
    return state


def save_state(state: dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(STATE_PATH)


def append_history(event: dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with HISTORY_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")


def request_with_retry(method: str, url: str, *, timeout: int = 20, **kwargs: Any) -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(2):
        try:
            resp = requests.request(method, url, timeout=timeout, **kwargs)
            if resp.status_code >= 500 and attempt == 0:
                time.sleep(2)
                continue
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            if attempt == 0:
                time.sleep(2)
    raise RuntimeError(f"Request failed after retry: {last_exc}")


def fetch_transactions(cfg: dict[str, str], limit: int = HELIUS_LIMIT) -> list[dict[str, Any]]:
    url = f"https://api.helius.xyz/v0/addresses/{cfg['wallet']}/transactions"
    params = {"api-key": cfg["helius_api_key"], "limit": str(limit)}
    resp = request_with_retry("GET", url, params=params)
    if not resp.ok:
        raise RuntimeError(f"Helius error {resp.status_code}: {resp.text[:500]}")
    data = resp.json()
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected Helius response: {data!r}")
    return data


def fetch_raw_transaction(cfg: dict[str, str], signature: str) -> dict[str, Any] | None:
    """Fetch raw Solana transaction details for best-effort protocol parsing."""
    url = f"https://{cfg['helius_network']}.helius-rpc.com/?api-key={cfg['helius_api_key']}"
    payload = {
        "jsonrpc": "2.0",
        "id": "blockzocker-wallet-alerts",
        "method": "getTransaction",
        "params": [signature, {"encoding": "json", "maxSupportedTransactionVersion": 0}],
    }
    try:
        resp = request_with_retry("POST", url, json=payload, timeout=30)
        if not resp.ok:
            logging.warning("Raw tx lookup failed for %s: HTTP %s", signature, resp.status_code)
            return None
        body = resp.json()
        if body.get("error"):
            logging.warning("Raw tx lookup failed for %s: %s", signature, body["error"])
            return None
        result = body.get("result")
        return result if isinstance(result, dict) else None
    except Exception as exc:
        logging.warning("Raw tx lookup failed for %s: %s", signature, exc)
        return None


def tx_signature(tx: dict[str, Any]) -> str | None:
    sig = tx.get("signature")
    return sig if isinstance(sig, str) and sig else None


def is_relevant(tx: dict[str, Any]) -> bool:
    """Keep swaps and meaningful token transfers; ignore pure fee-only SOL movements."""
    tx_type = str(tx.get("type") or "").upper()
    token_transfers = tx.get("tokenTransfers") or []
    native_transfers = tx.get("nativeTransfers") or []

    if tx_type == "SWAP":
        return True
    if token_transfers:
        # Prefer token movement over Helius' high-level type. Helius may classify
        # DEX/perp settlement activity as CLOSE_ACCOUNT while tokenTransfers still
        # show meaningful USDC/SOL movement for the watched wallet.
        return tx_type in RELEVANT_TYPES or tx_type == ""

    # Native SOL transfers can matter, but ignore tiny fee-like movements.
    # Helius nativeAmount is lamports.
    for transfer in native_transfers:
        try:
            lamports = abs(int(transfer.get("amount") or 0))
        except (TypeError, ValueError):
            continue
        if lamports >= 10_000_000:  # >= 0.01 SOL
            return True
    return False


def fmt_amount(value: Any) -> str:
    if value is None:
        return "?"
    try:
        n = float(value)
        if abs(n) >= 1:
            return f"{n:,.4f}".rstrip("0").rstrip(".")
        return f"{n:,.8f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return str(value)


def fmt_usd(value: float | None) -> str | None:
    if value is None:
        return None
    if abs(value) >= 1_000:
        return f"${value:,.0f}"
    return f"${value:,.2f}"


def base58_decode(value: str) -> bytes:
    n = 0
    for char in value:
        n = n * 58 + BASE58_ALPHABET.index(char)
    decoded = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    padding = len(value) - len(value.lstrip("1"))
    return (b"\0" * padding) + decoded


def base58_encode(value: bytes) -> str:
    n = int.from_bytes(value, "big")
    encoded = ""
    while n:
        n, rem = divmod(n, 58)
        encoded = BASE58_ALPHABET[rem] + encoded
    padding = len(value) - len(value.lstrip(b"\0"))
    return ("1" * padding) + (encoded or "")


def parse_log_scaled_usd(logs: list[str], label: str) -> float | None:
    pattern = re.compile(rf"{re.escape(label)}:\s*(\d+)")
    for line in logs:
        match = pattern.search(line)
        if match:
            return int(match.group(1)) / 1_000_000
    return None


def parse_log_scaled_price(logs: list[str], label: str) -> float | None:
    pattern = re.compile(rf"{re.escape(label)}:\s*(\d+)")
    for line in logs:
        match = pattern.search(line)
        if match:
            return int(match.group(1)) / 1_000_000
    return None


def raw_account_keys(raw_tx: dict[str, Any]) -> list[str]:
    message = ((raw_tx.get("transaction") or {}).get("message") or {})
    keys: list[str] = []
    for key in message.get("accountKeys") or []:
        if isinstance(key, str):
            keys.append(key)
        elif isinstance(key, dict) and isinstance(key.get("pubkey"), str):
            keys.append(key["pubkey"])
    loaded = (raw_tx.get("meta") or {}).get("loadedAddresses") or {}
    keys.extend(loaded.get("writable") or [])
    keys.extend(loaded.get("readonly") or [])
    return keys


def jupiter_perps_instruction_data(raw_tx: dict[str, Any]) -> list[bytes]:
    keys = raw_account_keys(raw_tx)
    instructions = (((raw_tx.get("transaction") or {}).get("message") or {}).get("instructions") or [])
    decoded_instructions: list[bytes] = []
    for instruction in instructions:
        try:
            program_id = keys[int(instruction.get("programIdIndex"))]
        except (TypeError, ValueError, IndexError):
            continue
        if program_id != JUPITER_PERPS_PROGRAM:
            continue
        data = instruction.get("data")
        if not isinstance(data, str):
            continue
        try:
            decoded_instructions.append(base58_decode(data))
        except (ValueError, IndexError):
            continue
    return decoded_instructions


def parse_perp_size_delta_usd(raw_tx: dict[str, Any]) -> float | None:
    candidates: list[float] = []
    for decoded in jupiter_perps_instruction_data(raw_tx):
        if len(decoded) < 16:
            continue
        # Anchor instruction data starts with an 8-byte discriminator. For the
        # Jupiter Perps InstantIncreasePosition variants, the next u64 is the
        # position size delta in USD with 6 decimals. This gives us the notional
        # added, which is enough for a leverage estimate when paired with logs.
        size_delta = int.from_bytes(decoded[8:16], "little") / 1_000_000
        if 1 <= size_delta <= 10_000_000:
            candidates.append(size_delta)
    return max(candidates) if candidates else None


def parse_borsh_option_u64(data: bytes, offset: int) -> int | None:
    """Return the offset after a Borsh Option<u64>, or None if malformed."""
    if offset >= len(data):
        return None
    tag = data[offset]
    offset += 1
    if tag == 0:
        return offset
    if tag == 1 and offset + 8 <= len(data):
        return offset + 8
    return None


def side_name(value: int) -> str | None:
    # Jupiter Perps Side enum in the public Anchor IDL: None=0, Long=1, Short=2.
    return {1: "Long", 2: "Short"}.get(value)


def rpc_request(cfg: dict[str, str], method: str, params: list[Any]) -> Any:
    url = f"https://{cfg['helius_network']}.helius-rpc.com/?api-key={cfg['helius_api_key']}"
    payload = {
        "jsonrpc": "2.0",
        "id": "blockzocker-wallet-alerts",
        "method": method,
        "params": params,
    }
    resp = request_with_retry("POST", url, json=payload, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"RPC {method} failed: HTTP {resp.status_code}: {resp.text[:500]}")
    body = resp.json()
    if body.get("error"):
        raise RuntimeError(f"RPC {method} failed: {body['error']}")
    return body.get("result")


def fetch_account_data(cfg: dict[str, str], account: str) -> bytes | None:
    try:
        value = (rpc_request(cfg, "getAccountInfo", [account, {"encoding": "base64"}]) or {}).get("value") or {}
        encoded = (value.get("data") or [None])[0]
        return base64.b64decode(encoded) if isinstance(encoded, str) else None
    except Exception as exc:
        logging.warning("Account lookup failed for %s: %s", account, exc)
        return None


def parse_position_account_side(cfg: dict[str, str], account: str) -> str | None:
    data = fetch_account_data(cfg, account)
    # Position layout from the Jupiter Perps Anchor IDL:
    # discriminator(8) + owner/pool/custody/collateralCustody(4*32)
    # + openTime/updateTime(2*8) + Side enum(u8).
    if data and len(data) > 152:
        return side_name(data[152])
    return None


def position_accounts_from_perp_ix(raw_tx: dict[str, Any]) -> list[str]:
    keys = raw_account_keys(raw_tx)
    instructions = (((raw_tx.get("transaction") or {}).get("message") or {}).get("instructions") or [])
    positions: list[str] = []
    for instruction in instructions:
        try:
            program_id = keys[int(instruction.get("programIdIndex"))]
        except (TypeError, ValueError, IndexError):
            continue
        if program_id != JUPITER_PERPS_PROGRAM:
            continue
        accounts = instruction.get("accounts") or []
        data = instruction.get("data")
        if not isinstance(data, str):
            continue
        try:
            decoded = base58_decode(data)
        except (ValueError, IndexError):
            continue
        position_index = None
        if decoded.startswith(ANCHOR_IX_INSTANT_INCREASE_POSITION):
            position_index = 6
        elif decoded.startswith(ANCHOR_IX_INSTANT_DECREASE_POSITION):
            position_index = 7
        if position_index is None or position_index >= len(accounts):
            continue
        try:
            key_index = int(accounts[position_index])
            positions.append(keys[key_index])
        except (TypeError, ValueError, IndexError):
            continue
    return positions


def parse_perp_side(cfg: dict[str, str], raw_tx: dict[str, Any]) -> str | None:
    for decoded in jupiter_perps_instruction_data(raw_tx):
        if not decoded.startswith(ANCHOR_IX_INSTANT_INCREASE_POSITION):
            continue
        # Current instant-increase payload observed on-chain:
        # u64 sizeUsdDelta, Option<u64> collateralTokenDelta, Side side,
        # u64 priceSlippage, i64 requestTime. Older IDLs include extra optional
        # swap fields, so also try that longer layout below.
        payload = decoded[8:]
        if len(payload) < 10:
            continue
        offset = 8
        compact_offset = parse_borsh_option_u64(payload, offset)
        if compact_offset is not None and compact_offset < len(payload):
            side = side_name(payload[compact_offset])
            if side:
                return side

        offset = 8
        for _ in range(3):
            next_offset = parse_borsh_option_u64(payload, offset)
            if next_offset is None:
                break
            offset = next_offset
        else:
            if offset < len(payload):
                side = side_name(payload[offset])
                if side:
                    return side

    for account in position_accounts_from_perp_ix(raw_tx):
        side = parse_position_account_side(cfg, account)
        if side:
            return side
    return None


def extract_perp_details(cfg: dict[str, str], signature: str) -> list[str]:
    raw_tx = fetch_raw_transaction(cfg, signature)
    if not raw_tx:
        return []
    logs = (raw_tx.get("meta") or {}).get("logMessages") or []
    if not any("InstantIncreasePosition" in line or "InstantDecreasePosition" in line for line in logs):
        return []

    size_delta_usd = parse_perp_size_delta_usd(raw_tx)
    collateral_added_usd = parse_log_scaled_usd(logs, "Collateral added in USD")
    swap_usd = parse_log_scaled_usd(logs, "swap_usd_amount")
    current_price = parse_log_scaled_price(logs, "Current price")
    fee_usd = parse_log_scaled_usd(logs, "Collected fee")

    lines: list[str] = []
    side = parse_perp_side(cfg, raw_tx)

    if side:
        lines.append(f"Side: {side}")
    if size_delta_usd and collateral_added_usd and collateral_added_usd > 0:
        leverage = size_delta_usd / collateral_added_usd
        lines.append(f"Leverage: ≈{leverage:.1f}x")
    if size_delta_usd is not None:
        lines.append(f"Position size Δ: {fmt_usd(size_delta_usd)}")
    if collateral_added_usd is not None:
        lines.append(f"Collateral added: {fmt_usd(collateral_added_usd)}")
    if current_price is not None:
        lines.append(f"Mark price: {fmt_usd(current_price)}")
    if fee_usd is not None:
        lines.append(f"Fee: {fmt_usd(fee_usd)}")
    if swap_usd is not None:
        lines.append(f"Pre-swap value: {fmt_usd(swap_usd)}")

    return [line for line in lines if line]


def token_symbol(transfer: dict[str, Any]) -> str:
    mint = str(transfer.get("mint") or "unknown mint")
    return str(transfer.get("tokenSymbol") or KNOWN_MINT_SYMBOLS.get(mint) or mint[:6] + "…")


def summarize_trade(tx: dict[str, Any], wallet: str) -> str | None:
    """Return a compact, human-first trade/action summary.

    Helius' top-level type/description can be misleading for DEX/perp actions
    (for example CLOSE_ACCOUNT with tokenTransfers). This summary focuses on
    token flow for the watched wallet and calls out same-token in/out legs,
    which often indicate wrapped SOL or leveraged/perp routing.
    """
    by_symbol: dict[str, dict[str, float]] = {}
    for t in tx.get("tokenTransfers") or []:
        src = str(t.get("fromUserAccount") or "")
        dst = str(t.get("toUserAccount") or "")
        if src != wallet and dst != wallet:
            continue
        try:
            amount = abs(float(t.get("tokenAmount") or 0))
        except (TypeError, ValueError):
            continue
        if amount == 0:
            continue

        symbol = token_symbol(t)
        bucket = by_symbol.setdefault(symbol, {"in": 0.0, "out": 0.0})
        if dst == wallet and src != wallet:
            bucket["in"] += amount
        elif src == wallet and dst != wallet:
            bucket["out"] += amount

    if not by_symbol:
        return None

    spent: list[str] = []
    received: list[str] = []
    round_trips: list[str] = []

    for symbol, amounts in by_symbol.items():
        incoming = amounts["in"]
        outgoing = amounts["out"]
        net = incoming - outgoing

        # Equal-ish in/out on the same asset is usually a wrapped-token or perp
        # leg, not a real portfolio net change. Show it explicitly instead of
        # hiding it, because it explains leverage/routing activity.
        if incoming and outgoing and abs(net) <= max(incoming, outgoing) * 0.001:
            round_trips.append(f"{fmt_amount(max(incoming, outgoing))} {symbol} in/out")
        elif net > 0:
            received.append(f"{fmt_amount(net)} {symbol}")
        elif net < 0:
            spent.append(f"{fmt_amount(abs(net))} {symbol}")

    parts: list[str] = []
    if spent and received:
        parts.append(f"Swap: {', '.join(spent)} → {', '.join(received)}")
    elif spent:
        parts.append(f"Out: {', '.join(spent)}")
    elif received:
        parts.append(f"In: {', '.join(received)}")
    if round_trips:
        parts.append(f"Lever/perp leg: {', '.join(round_trips)}")

    return " | ".join(parts) if parts else None


def summarize_transfers(tx: dict[str, Any], wallet: str) -> list[str]:
    lines: list[str] = []
    for t in tx.get("tokenTransfers") or []:
        symbol = token_symbol(t)
        amount = fmt_amount(t.get("tokenAmount"))
        src = str(t.get("fromUserAccount") or "")
        dst = str(t.get("toUserAccount") or "")
        if dst == wallet and src != wallet:
            direction = "IN"
        elif src == wallet and dst != wallet:
            direction = "OUT"
        else:
            direction = "MOVE"
        lines.append(f"{direction}: {amount} {symbol}")

    if not lines:
        for t in tx.get("nativeTransfers") or []:
            try:
                sol = int(t.get("amount") or 0) / 1_000_000_000
            except (TypeError, ValueError):
                continue
            if abs(sol) < 0.01:
                continue
            src = str(t.get("fromUserAccount") or "")
            dst = str(t.get("toUserAccount") or "")
            if dst == wallet and src != wallet:
                direction = "IN"
            elif src == wallet and dst != wallet:
                direction = "OUT"
            else:
                direction = "MOVE"
            lines.append(f"{direction}: {fmt_amount(sol)} SOL")
    return lines[:8]


def detail_value(details: list[str], prefix: str) -> str | None:
    for detail in details:
        if detail.startswith(prefix):
            return detail.split(":", 1)[1].strip()
    return None


def compact_action(summary: str | None) -> str | None:
    if not summary:
        return None
    text = summary
    text = text.replace("Swap: ", "")
    text = text.replace("Out: ", "")
    text = text.replace("In: ", "")
    text = text.replace(" | Lever/perp leg: ", " out · ")
    text = text.replace(" in/out", " route")
    if "→" not in text and " out" not in text and " route" not in text:
        text = f"{text} moved"
    return text


def parse_description_action(description: str, wallet: str) -> str | None:
    clean = description.strip()
    if not clean:
        return None

    swap_match = re.search(
        rf"{re.escape(wallet)} swapped ([0-9.,]+) ([A-Za-z0-9/]+) for ([0-9.,]+) ([A-Za-z0-9/]+)",
        clean,
    )
    if swap_match:
        amount_in, symbol_in, amount_out, symbol_out = swap_match.groups()
        return f"{amount_in} {symbol_in} → {amount_out} {symbol_out}"

    sent_match = re.search(
        rf"{re.escape(wallet)} transferred ([0-9.,]+) ([A-Za-z0-9/]+) to ",
        clean,
    )
    if sent_match:
        amount, symbol = sent_match.groups()
        return f"OUT {amount} {symbol}"

    received_match = re.search(
        rf" transferred ([0-9.,]+) ([A-Za-z0-9/]+) to {re.escape(wallet)}",
        clean,
    )
    if received_match:
        amount, symbol = received_match.groups()
        return f"IN {amount} {symbol}"

    transfer_match = re.search(
        rf"{re.escape(wallet)} transferred a total ([0-9.,]+) ([A-Za-z0-9/]+)",
        clean,
    )
    if transfer_match:
        amount, symbol = transfer_match.groups()
        return f"{amount} {symbol} transferred"

    return None


def compact_wallet_flow(tx: dict[str, Any], wallet: str) -> str | None:
    """Return only direct watched-wallet flow, ignoring Jupiter route internals."""
    flows: list[str] = []
    for t in tx.get("tokenTransfers") or []:
        src = str(t.get("fromUserAccount") or "")
        dst = str(t.get("toUserAccount") or "")
        if src != wallet and dst != wallet:
            continue
        symbol = token_symbol(t)
        amount = fmt_amount(t.get("tokenAmount"))
        if dst == wallet and src != wallet:
            flows.append(f"IN {amount} {symbol}")
        elif src == wallet and dst != wallet:
            flows.append(f"OUT {amount} {symbol}")

    if not flows:
        for t in tx.get("nativeTransfers") or []:
            src = str(t.get("fromUserAccount") or "")
            dst = str(t.get("toUserAccount") or "")
            if src != wallet and dst != wallet:
                continue
            try:
                sol = int(t.get("amount") or 0) / 1_000_000_000
            except (TypeError, ValueError):
                continue
            if abs(sol) < 0.01:
                continue
            direction = "IN" if dst == wallet and src != wallet else "OUT"
            flows.append(f"{direction} {fmt_amount(sol)} SOL")

    return " · ".join(flows[:3]) if flows else None


def compact_swap_fallback(tx: dict[str, Any], wallet: str) -> str | None:
    flow = compact_wallet_flow(tx, wallet)
    if not flow:
        return None
    if " · " not in flow and flow.startswith("IN "):
        return "Received " + flow.removeprefix("IN ")
    if " · " not in flow and flow.startswith("OUT "):
        return "Sent " + flow.removeprefix("OUT ")
    return flow


def format_timestamp(ts: Any) -> str:
    try:
        dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except (TypeError, ValueError, OSError):
        return "unknown time"


def build_message(tx: dict[str, Any], cfg: dict[str, str]) -> str:
    sig = tx_signature(tx) or "unknown"
    raw_tx_type = str(tx.get("type") or "Unknown")
    raw_source = str(tx.get("source") or "Unknown")
    description = str(tx.get("description") or "").strip()
    when = html.escape(format_timestamp(tx.get("timestamp")))
    link = f"https://solscan.io/tx/{sig}"
    trade_summary = summarize_trade(tx, cfg["wallet"])
    perp_details = extract_perp_details(cfg, sig) if sig != "unknown" else []
    transfers = summarize_transfers(tx, cfg["wallet"])
    transfer_text = "\n".join(f"• {html.escape(x)}" for x in transfers) if transfers else "• No clear transfer summary"

    if perp_details:
        side = detail_value(perp_details, "Side")
        leverage = detail_value(perp_details, "Leverage")
        size_delta = detail_value(perp_details, "Position size Δ")
        collateral = detail_value(perp_details, "Collateral added")
        mark_price = detail_value(perp_details, "Mark price")
        fee = detail_value(perp_details, "Fee")
        pre_swap = detail_value(perp_details, "Pre-swap value")
        action = compact_action(trade_summary)

        lines = [
            f"🚨 <b>{html.escape(cfg['wallet_label'])} · Jupiter Perps</b>",
        ]
        if side and leverage:
            emoji = "🟢" if side == "Long" else "🔴" if side == "Short" else "⚪️"
            lines.append(f"<b>{emoji} {html.escape(side.upper())} · {html.escape(leverage)} leverage</b>")
        elif side:
            emoji = "🟢" if side == "Long" else "🔴" if side == "Short" else "⚪️"
            lines.append(f"<b>{emoji} {html.escape(side.upper())}</b>")
        elif leverage:
            lines.append(f"<b>{html.escape(leverage)} leverage</b>")
        if action:
            lines.append(f"💸 {html.escape(action)}")
        if size_delta or collateral:
            position_parts = []
            if size_delta:
                position_parts.append(f"Size Δ {size_delta}")
            if collateral:
                position_parts.append(f"Collateral {collateral}")
            lines.append(f"📊 {html.escape(' · '.join(position_parts))}")
        if mark_price or fee:
            market_parts = []
            if mark_price:
                market_parts.append(f"Mark {mark_price}")
            if fee:
                market_parts.append(f"Fee {fee}")
            lines.append(f"💱 {html.escape(' · '.join(market_parts))}")
        if pre_swap:
            lines.append(f"🔁 Pre-swap {html.escape(pre_swap)}")
        lines.append(f"🕒 {when}")
        lines.append(f"<a href=\"{html.escape(link)}\">Open in Solscan</a>")
        return "\n".join(lines)

    if raw_tx_type.upper() == "SWAP":
        action = parse_description_action(description, cfg["wallet"]) or compact_swap_fallback(tx, cfg["wallet"]) or compact_action(trade_summary)
        lines = [
            f"🔄 <b>{html.escape(cfg['wallet_label'])} · Jupiter Swap</b>",
        ]
        if action:
            lines.append(f"💸 <b>{html.escape(action)}</b>")
        lines.append(f"🕒 {when}")
        lines.append(f"<a href=\"{html.escape(link)}\">Open in Solscan</a>")
        return "\n".join(lines)

    if raw_tx_type.upper() in {"TRANSFER", "TOKEN_TRANSFER"}:
        action = parse_description_action(description, cfg["wallet"]) or compact_wallet_flow(tx, cfg["wallet"])
        lines = [
            f"↔️ <b>{html.escape(cfg['wallet_label'])} · Transfer</b>",
        ]
        if action:
            lines.append(f"💸 <b>{html.escape(action)}</b>")
        lines.append(f"🕒 {when}")
        lines.append(f"<a href=\"{html.escape(link)}\">Open in Solscan</a>")
        return "\n".join(lines)

    if raw_tx_type.upper() == "UNKNOWN":
        action = compact_wallet_flow(tx, cfg["wallet"])
        lines = [
            f"⚠️ <b>{html.escape(cfg['wallet_label'])} · Wallet Activity</b>",
        ]
        if action:
            lines.append(f"💸 <b>{html.escape(action)}</b>")
        lines.append(f"🕒 {when}")
        lines.append(f"<a href=\"{html.escape(link)}\">Open in Solscan</a>")
        return "\n".join(lines)

    protocol_line = "Jupiter Perps" if perp_details else f"{raw_tx_type} via {raw_source}"
    action_text = f"<b>Action:</b> {html.escape(trade_summary)}\n" if trade_summary else ""
    description_text = f"<i>{html.escape(description)}</i>\n\n" if description else ""
    perp_text = ""
    if perp_details:
        perp_text = "<b>Perp details</b>\n" + "\n".join(f"• {html.escape(x)}" for x in perp_details) + "\n\n"

    return (
        f"🚨 <b>{html.escape(cfg['wallet_label'])}</b>\n"
        f"<b>{html.escape(protocol_line)}</b>\n"
        f"{action_text}\n"
        f"{perp_text}"
        f"<b>Transfers</b>\n{transfer_text}\n\n"
        f"{description_text}"
        f"<b>Time:</b> {when}\n"
        f"<a href=\"{html.escape(link)}\">Open in Solscan</a>"
    )


def mint_symbol(mint: str) -> str:
    return KNOWN_MINT_SYMBOLS.get(mint) or mint[:6] + "…"


def parse_custody_mint(cfg: dict[str, str], custody: str) -> str | None:
    data = fetch_account_data(cfg, custody)
    # Custody account layout from Jupiter Perps Anchor IDL:
    # discriminator(8) + pool(32) + mint(32) + ...
    if data and len(data) >= 72:
        return base58_encode(data[40:72])
    return None


def position_snapshot(cfg: dict[str, str], account: str, data: bytes) -> dict[str, Any] | None:
    if len(data) < 209:
        return None
    custody = base58_encode(data[72:104])
    collateral_custody = base58_encode(data[104:136])
    side = side_name(data[152]) or f"Unknown({data[152]})"
    price = int.from_bytes(data[153:161], "little") / 1_000_000
    size_usd = int.from_bytes(data[161:169], "little") / 1_000_000
    collateral_usd = int.from_bytes(data[169:177], "little") / 1_000_000
    realised_pnl_usd = int.from_bytes(data[177:185], "little", signed=True) / 1_000_000
    open_time = int.from_bytes(data[136:144], "little", signed=True)
    update_time = int.from_bytes(data[144:152], "little", signed=True)
    mint = parse_custody_mint(cfg, custody)
    collateral_mint = parse_custody_mint(cfg, collateral_custody)
    return {
        "account": account,
        "side": side,
        "mint": mint,
        "symbol": mint_symbol(mint) if mint else "unknown",
        "collateral_mint": collateral_mint,
        "collateral_symbol": mint_symbol(collateral_mint) if collateral_mint else "unknown",
        "size_usd": size_usd,
        "collateral_usd": collateral_usd,
        "leverage": size_usd / collateral_usd if collateral_usd else None,
        "price": price,
        "realised_pnl_usd": realised_pnl_usd,
        "open_time": open_time,
        "update_time": update_time,
    }


def fetch_open_positions(cfg: dict[str, str]) -> list[dict[str, Any]]:
    result = rpc_request(cfg, "getProgramAccounts", [
        JUPITER_PERPS_PROGRAM,
        {
            "encoding": "base64",
            "filters": [
                {"dataSize": 216},
                {"memcmp": {"offset": 8, "bytes": cfg["wallet"]}},
            ],
        },
    ])
    positions: list[dict[str, Any]] = []
    for item in result or []:
        encoded = (((item.get("account") or {}).get("data") or [None])[0])
        if not isinstance(encoded, str):
            continue
        pos = position_snapshot(cfg, str(item.get("pubkey") or "unknown"), base64.b64decode(encoded))
        if pos and pos["size_usd"] > 0:
            positions.append(pos)
    return positions


def fetch_wallet_token_balances(cfg: dict[str, str]) -> list[dict[str, str]]:
    result = rpc_request(cfg, "getTokenAccountsByOwner", [
        cfg["wallet"],
        {"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
        {"encoding": "jsonParsed"},
    ])
    balances: list[dict[str, str]] = []
    for item in (result or {}).get("value") or []:
        info = (((item.get("account") or {}).get("data") or {}).get("parsed") or {}).get("info") or {}
        token_amount = info.get("tokenAmount") or {}
        try:
            amount = float(token_amount.get("uiAmount") or 0)
        except (TypeError, ValueError):
            amount = 0.0
        if amount == 0:
            continue
        mint = str(info.get("mint") or "unknown")
        balances.append({
            "symbol": mint_symbol(mint),
            "amount": str(token_amount.get("uiAmountString") or amount),
            "mint": mint,
        })
    return balances


def build_snapshot_message(cfg: dict[str, str]) -> str:
    sol_lamports = int((rpc_request(cfg, "getBalance", [cfg["wallet"]]) or {}).get("value") or 0)
    token_balances = fetch_wallet_token_balances(cfg)
    positions = fetch_open_positions(cfg)
    when = html.escape(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    wallet_short = cfg["wallet"][:4] + "…" + cfg["wallet"][-4:]

    lines = [
        f"📊 <b>{html.escape(cfg['wallet_label'])} · Current Snapshot</b>",
        f"👛 Wallet: <code>{html.escape(wallet_short)}</code>",
        f"🕒 {when}",
        "",
        "<b>Balances</b>",
        f"• SOL: {fmt_amount(sol_lamports / 1_000_000_000)}",
    ]
    for bal in token_balances:
        lines.append(f"• {html.escape(bal['symbol'])}: {html.escape(bal['amount'])}")

    lines.extend(["", "<b>Open Jupiter Perps positions</b>"])
    if not positions:
        lines.append("• None")
    for pos in positions:
        side = str(pos["side"])
        emoji = "🟢" if side == "Long" else "🔴" if side == "Short" else "⚪️"
        leverage = pos.get("leverage")
        lev_text = f" · {leverage:.2f}x" if isinstance(leverage, float) else ""
        lines.append(f"• <b>{emoji} {html.escape(side.upper())} {html.escape(str(pos['symbol']))}{lev_text}</b>")
        lines.append(f"  Size {html.escape(fmt_usd(pos['size_usd']) or '?')} · Collateral {html.escape(fmt_usd(pos['collateral_usd']) or '?')} {html.escape(str(pos['collateral_symbol']))}")
        lines.append(f"  Price {html.escape(fmt_usd(pos['price']) or '?')} · Realised PnL {html.escape(fmt_usd(pos['realised_pnl_usd']) or '?')}")
        lines.append(f"  <a href=\"https://solscan.io/account/{html.escape(str(pos['account']))}\">Position account</a>")

    return "\n".join(lines)


def send_snapshot(cfg: dict[str, str], *, dry_run: bool = False) -> None:
    send_telegram(cfg, build_snapshot_message(cfg), dry_run=dry_run)


def send_telegram(cfg: dict[str, str], message: str, dry_run: bool = False) -> None:
    if dry_run:
        print(message)
        return
    url = f"https://api.telegram.org/bot{cfg['telegram_bot_token']}/sendMessage"
    payload = {
        "chat_id": cfg["telegram_chat_id"],
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    resp = request_with_retry("POST", url, json=payload)
    if not resp.ok:
        raise RuntimeError(f"Telegram error {resp.status_code}: {resp.text[:500]}")


def process_transactions(cfg: dict[str, str], *, dry_run: bool = False, bootstrap: bool = False) -> int:
    state = load_state(cfg["wallet"])
    wallets = state.setdefault("wallets", {})
    wallet_state = wallets.setdefault(cfg["wallet"], empty_wallet_state())
    processed = list(dict.fromkeys(wallet_state.get("processed_signatures") or []))
    processed_set = set(processed)

    txs = fetch_transactions(cfg)
    if not txs:
        wallet_state["updated_at"] = datetime.now(timezone.utc).isoformat()
        if not dry_run:
            save_state(state)
        logging.info("No transactions returned by Helius")
        return 0

    newest_sig = tx_signature(txs[0])
    new_txs = [tx for tx in reversed(txs) if (sig := tx_signature(tx)) and sig not in processed_set]

    if bootstrap:
        sigs = [tx_signature(tx) for tx in txs if tx_signature(tx)]
        wallet_state["processed_signatures"] = list(dict.fromkeys(sigs + processed))[:500]
        wallet_state["last_seen_signature"] = newest_sig
        wallet_state["updated_at"] = datetime.now(timezone.utc).isoformat()
        save_state(state)
        logging.info("Bootstrap complete; marked %d recent txs as processed without alerts", len(sigs))
        return 0

    sent = 0
    for tx in new_txs:
        sig = tx_signature(tx)
        if not sig:
            continue

        event = {
            "signature": sig,
            "timestamp": tx.get("timestamp"),
            "type": tx.get("type"),
            "source": tx.get("source"),
            "description": tx.get("description"),
            "relevant": is_relevant(tx),
            "seen_at": datetime.now(timezone.utc).isoformat(),
        }
        append_history(event)

        if is_relevant(tx):
            message = build_message(tx, cfg)
            send_telegram(cfg, message, dry_run=dry_run)
            sent += 1
            logging.info("Alerted on %s", sig)
        else:
            logging.info("Recorded non-relevant tx %s", sig)

        processed.insert(0, sig)
        processed = list(dict.fromkeys(processed))[:500]
        wallet_state["processed_signatures"] = processed
        wallet_state["last_seen_signature"] = newest_sig or sig
        wallet_state["updated_at"] = datetime.now(timezone.utc).isoformat()
        if not dry_run:
            save_state(state)

    if not new_txs:
        logging.info("No new transactions")
    elif dry_run:
        logging.info("Dry run complete; would update state for %d new txs", len(new_txs))
    else:
        save_state(state)

    return sent


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BlockZocker Solana wallet Telegram alerts")
    parser.add_argument("--dry-run", action="store_true", help="Print alerts instead of sending/updating state")
    parser.add_argument("--bootstrap", action="store_true", help="Mark recent txs processed without sending alerts")
    parser.add_argument("--once", action="store_true", help="Run one check and exit (default; cron-friendly)")
    parser.add_argument("--snapshot", action="store_true", help="Send current wallet balances and open Jupiter Perps positions")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    setup_logging()
    try:
        cfg = load_config()
        if args.snapshot:
            send_snapshot(cfg, dry_run=args.dry_run)
            logging.info("Snapshot sent")
            return 0
        sent = process_transactions(cfg, dry_run=args.dry_run, bootstrap=args.bootstrap)
        logging.info("Done; sent %d alert(s)", sent)
        return 0
    except ConfigError as exc:
        logging.error("Configuration error: %s", exc)
        return 2
    except Exception:
        logging.exception("Run failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
