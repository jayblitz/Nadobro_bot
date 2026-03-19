# LOWIQPTS Relay Service

A FastAPI + Telethon relay that proxies DM conversations between Nadobro users and the @lowiqpts Telegram bot for points lookups.

## How It Works

1. Nadobro sends `POST /sessions/start` with a user's wallet address
2. The relay opens a DM with @lowiqpts and sends `/nado <wallet>`
3. @lowiqpts replies with points data
4. Nadobro polls `GET /events/poll` to receive those replies
5. When done, Nadobro sends `POST /sessions/close`

## Prerequisites

### 1. Get Telegram API Credentials

1. Go to [https://my.telegram.org](https://my.telegram.org)
2. Log in with the phone number of the Telegram account that will DM @lowiqpts
3. Go to **API development tools**
4. Create a new application (any name/description)
5. Copy your **API ID** (a number) and **API Hash** (a hex string)

### 2. First-Time Telethon Login

Before deploying, you need to create a session file by logging in once locally:

```bash
cd relay
pip install telethon

python -c "
from telethon.sync import TelegramClient
client = TelegramClient('relay_session', YOUR_API_ID, 'YOUR_API_HASH')
client.start()
print('Logged in as:', client.get_me().username)
client.disconnect()
"
```

This will prompt for your phone number and a verification code (and 2FA password if enabled). It creates a `relay_session.session` file — this is your persistent login.

### 3. Verify DM Works

```bash
python -c "
from telethon.sync import TelegramClient
client = TelegramClient('relay_session', YOUR_API_ID, 'YOUR_API_HASH')
client.start()
entity = client.get_entity('lowiqpts')
client.send_message(entity, '/nado 0x0000000000000000000000000000000000000000')
print('Message sent!')
client.disconnect()
"
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `TELEGRAM_API_ID` | Yes | — | From my.telegram.org |
| `TELEGRAM_API_HASH` | Yes | — | From my.telegram.org |
| `DATABASE_URL` | Yes | — | PostgreSQL connection string (your Supabase URL) |
| `RELAY_AUTH_TOKEN` | Yes | — | Shared secret for Bearer auth (same as `LOWIQPTS_RELAY_AUTH_TOKEN` in Nadobro) |
| `LOWIQPTS_USERNAME` | No | `lowiqpts` | Telegram username to DM |
| `SESSION_PATH` | No | `/data/relay.session` | Path to Telethon session file |
| `PORT` | No | `8080` | HTTP server port |

## Deploy to Fly.io

### One-time setup

```bash
cd relay

# Create the Fly app
fly apps create nadobro-relay

# Create a persistent volume for the Telethon session file
fly volumes create relay_data --size 1 --region ams

# Set secrets
fly secrets set \
  TELEGRAM_API_ID="your_api_id" \
  TELEGRAM_API_HASH="your_api_hash" \
  DATABASE_URL="postgresql://user:pass@host:port/db" \
  RELAY_AUTH_TOKEN="your_shared_secret"

# Upload your session file to the volume
# Option A: Deploy first, then SSH in and copy the file
fly deploy
fly ssh console
# Inside the machine:
# mkdir -p /data
# then paste/upload your .session file to /data/relay.session

# Option B: Use Telethon StringSession (advanced)
# Generate a string session locally, set it as TELETHON_STRING_SESSION env var,
# and modify telegram_client.py to use StringSession instead of file-based session.
# See Telethon docs: https://docs.telethon.dev/en/stable/concepts/sessions.html
```

### Deploy

```bash
fly deploy
```

### Verify

```bash
curl https://nadobro-relay.fly.dev/health
# {"ok":true,"service":"lowiqpts-relay"}
```

## Configure Nadobro

Set these environment variables on your Nadobro bot:

```
LOWIQPTS_RELAY_BASE_URL=https://nadobro-relay.fly.dev
LOWIQPTS_RELAY_AUTH_TOKEN=your_shared_secret
LOWIQPTS_RELAY_TIMEOUT_SECONDS=15
LOWIQPTS_RELAY_POLL_SECONDS=2
```

## Local Development

Run from the **repo root** (not from inside `relay/`):

```bash
pip install -r relay/requirements.txt

export TELEGRAM_API_ID="..."
export TELEGRAM_API_HASH="..."
export DATABASE_URL="postgresql://..."
export RELAY_AUTH_TOKEN="dev-token"
export SESSION_PATH="./relay_session.session"

python -m uvicorn relay.main:app --host 0.0.0.0 --port 8080 --reload
```

## API Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/health` | No | Health check |
| `POST` | `/sessions/start` | Yes | Start a new DM session with @lowiqpts |
| `POST` | `/sessions/reply` | Yes | Send user reply into active DM |
| `GET` | `/events/poll` | Yes | Poll buffered @lowiqpts messages |
| `POST` | `/sessions/close` | Yes | Close a session |

## Architecture Notes

- **Single instance only** — Telethon MTProto sessions conflict if multiple instances connect with the same account. `fly.toml` is configured with `min_machines_running = 1` and `auto_stop_machines = "off"`.
- **Session persistence** — The Telethon `.session` file is stored on a Fly volume at `/data/`. If the volume is lost, you'll need to log in again.
- **Idle cleanup** — Sessions inactive for >5 minutes are automatically marked as expired.
- **Event buffer** — All @lowiqpts messages are stored in PostgreSQL and served via cursor-based pagination. Events are never deleted (for audit), but idle session cleanup prevents indefinite growth.
