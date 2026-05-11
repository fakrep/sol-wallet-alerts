# BlockZocker Wallet Alerts

Cron-friendly Python script that monitors a Solana wallet with Helius and sends concise Telegram alerts for relevant activity.

The project is intentionally small: one script, a `.env` file for private configuration, JSON state for duplicate prevention, and JSONL history for later analysis.

## Features

- Fetches recent Solana enhanced transactions from Helius.
- Sends Telegram alerts for trading-relevant activity:
  - Jupiter swaps
  - Jupiter Perps actions with best-effort leverage/collateral details
  - token transfers
  - meaningful native SOL transfers (`>= 0.01 SOL`)
- Ignores tiny fee-only movements.
- Prevents duplicate alerts with `data/state.json`.
- Appends a machine-readable audit trail to `data/events.jsonl`.
- Writes rotating logs to `logs/blockzocker_wallet_alerts.log`.
- Keeps secrets out of source code via environment variables.

## Repository layout

```text
blockzocker_wallet_alerts.py  # main cron-friendly script
.env.example                  # safe config template; copy to .env
requirements.txt              # Python dependencies
.gitignore                    # excludes secrets/runtime data
README.md                     # this guide
data/.gitkeep                 # keeps runtime data directory in git
logs/.gitkeep                 # keeps runtime log directory in git
```

Runtime files are created automatically and should not be committed:

```text
.env
data/state.json
data/events.jsonl
logs/*.log
```

## Requirements

- Python 3.10+
- A Helius API key
- A Telegram bot token
- The Telegram chat id that should receive alerts
- The Solana wallet address to monitor

## Setup

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
nano .env
```

Fill in the required values in `.env`:

```bash
HELIUS_API_KEY=your_helius_api_key_here
TELEGRAM_BOT_TOKEN=your_telegram_bot_token_here
TELEGRAM_CHAT_ID=your_telegram_chat_id_here
WALLET_ADDRESS=your_solana_wallet_address_here
```

Optional values:

```bash
WALLET_LABEL=BlockZocker Trading Wallet
HELIUS_NETWORK=mainnet
LOG_LEVEL=INFO
```

## Telegram bot setup

1. Message `@BotFather` on Telegram.
2. Run `/newbot` and follow the prompts.
3. Copy the generated token into `TELEGRAM_BOT_TOKEN`.
4. Send a normal message to the new bot once, so Telegram allows it to message you.
5. Put the target chat id into `TELEGRAM_CHAT_ID`.

For a group, add the bot to the group, send a message in the group, then call Telegram `getUpdates` and use the group's `chat.id` value. Group ids are usually negative and often start with `-100`.

## First run

Bootstrap once before enabling cron. This records recent transactions as already processed, so the bot does not spam old activity:

```bash
. .venv/bin/activate
python blockzocker_wallet_alerts.py --bootstrap
```

Preview alert formatting without sending Telegram messages or updating state:

```bash
python blockzocker_wallet_alerts.py --dry-run
```

Run one real check:

```bash
python blockzocker_wallet_alerts.py --once
```

`--once` is the default behavior, so this is equivalent:

```bash
python blockzocker_wallet_alerts.py
```

## Cron example

Edit crontab:

```bash
crontab -e
```

Add an hourly job. Adjust the path to match where you cloned the repo:

```cron
@hourly cd /path/to/blockzocker_wallet_alerts && . .venv/bin/activate && python blockzocker_wallet_alerts.py --once >> logs/cron.log 2>&1
```

## Monitoring and debugging

Recent application log:

```bash
tail -n 100 logs/blockzocker_wallet_alerts.log
```

Cron wrapper output:

```bash
tail -n 100 logs/cron.log
```

Duplicate-prevention checkpoint:

```bash
cat data/state.json
```

Recent event history:

```bash
tail -n 20 data/events.jsonl
```

## Security notes

- Do not commit `.env`.
- Do not hardcode API keys, bot tokens, chat ids, or private wallet-specific config in the script.
- Treat `data/` and `logs/` as runtime data. They may reveal wallet activity and should stay out of public commits.
- `.env.example` must contain placeholders only.

## Future ideas

- Multiple wallet support
- USD pricing snapshots
- Holdings reports
- PnL/performance summaries
- Telegram commands such as `/last` or `/pnl`
- SQLite storage if JSONL history becomes too limiting
