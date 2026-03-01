# Deploy Nadobro on Fly.io

Step-by-step guide to run the Nadobro Telegram bot 24/7 on Fly.io. The app uses Docker and Fly secrets.

---

## Prerequisites

- A [Fly.io](https://fly.io) account.
- **flyctl** installed. See [Install flyctl](https://fly.io/docs/hands-on/install-flyctl/).
- Your project root must contain: `main.py`, `Dockerfile`, `fly.toml`, `pyproject.toml`.
- Required values ready: `TELEGRAM_TOKEN`, `DATABASE_URL`, `ENCRYPTION_KEY`. Optional: `XAI_API_KEY`, `OPENAI_API_KEY`, `ADMIN_USER_IDS`.

**Note:** Fly.io deployment requires its own PostgreSQL database (not Replit's). You can use Fly Postgres or any external PostgreSQL provider.

---

## 1. One-time setup

```bash
cd /path/to/Nadobro_bot

fly auth login
fly launch --no-deploy
```

When prompted:
- **App name:** Accept the default or enter another.
- **Region:** Pick a non-US region (e.g. `ams` for Amsterdam) to avoid Nado DEX geo-restrictions.
- **Postgres:** Answer **Yes** if you want Fly-managed Postgres, or **No** if using an external provider.

---

## 2. Set secrets

```bash
fly secrets set TELEGRAM_TOKEN="your_bot_token_from_botfather"
fly secrets set DATABASE_URL="postgresql://user:pass@host:5432/dbname"
fly secrets set ENCRYPTION_KEY="your_fernet_key"
```

Optional:

```bash
fly secrets set XAI_API_KEY="your_xai_key"
fly secrets set OPENAI_API_KEY="your_openai_key"
fly secrets set ADMIN_USER_IDS="123456789"
```

---

## 3. Deploy

```bash
fly deploy
```

---

## 4. Verify

- `fly status` — machine should be in `started` state
- `fly logs` — look for "Nadobro is live! Pure bot mode running."
- In Telegram, send `/start` to your bot

---

## Useful commands

- `fly logs` — stream logs
- `fly status` — app and machine status
- `fly deploy` — rebuild and deploy after changes
- `fly apps destroy nadobro-bot` — delete the app
