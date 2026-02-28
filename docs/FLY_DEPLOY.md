# Deploy Nadobro on Fly.io

Step-by-step guide to run the Nadobro Telegram bot 24/7 on Fly.io. The app uses Docker and Fly secrets (no `.env` file on the server).

---

## Prerequisites

- A [Fly.io](https://fly.io) account (sign up at [fly.io/app/sign-up](https://fly.io/app/sign-up)).
- **flyctl** (Fly CLI) installed. See [Install flyctl](https://fly.io/docs/hands-on/install-flyctl/).
  - macOS (Homebrew): `brew install flyctl`
  - Linux: `curl -L https://fly.io/install.sh | sh`
- Your project root must contain: `main.py`, `Dockerfile`, `fly.toml`, `pyproject.toml`, and `uv.lock`.
- Required values ready (do not paste in chat): `TELEGRAM_TOKEN`, `SUPABASE_URL`, `SUPABASE_KEY`, `ENCRYPTION_KEY`. Optional: `XAI_API_KEY`, `OPENAI_API_KEY`, `ADMIN_USER_IDS`.

---

## 1. One-time setup

From the **project root** (the folder that contains `main.py` and `fly.toml`):

```bash
cd /path/to/Nadobro_bot

fly auth login
```

Log in in the browser when prompted.

Create the app **without** deploying yet (so you can set secrets first):

```bash
fly launch --no-deploy
```

When prompted:

- **App name:** Accept the default (`nadobro-bot`) or enter another (must be unique on Fly).
- **Region:** Pick one close to you or your users (e.g. `iad` for US East).
- **Postgres / Redis:** Answer **No** (the bot uses Supabase, not Fly Postgres).

This creates the app in your Fly account and updates `fly.toml` if needed.

---

## 2. Set secrets

Fly injects these as environment variables. Do not commit or paste them in code.

**Required:**

```bash
fly secrets set TELEGRAM_TOKEN="your_bot_token_from_botfather"
fly secrets set SUPABASE_URL="https://your-project.supabase.co"
fly secrets set SUPABASE_KEY="your_supabase_service_role_key"
fly secrets set ENCRYPTION_KEY="your_fernet_or_32_char_secret"
```

**Optional (AI support and admin):**

```bash
fly secrets set XAI_API_KEY="your_xai_key"
fly secrets set OPENAI_API_KEY="your_openai_key"
fly secrets set ADMIN_USER_IDS="123456789"
```

Replace the quoted values with your real ones. To change a secret later, run `fly secrets set NAME="new_value"` again and redeploy.

---

## 3. Deploy

```bash
fly deploy
```

Fly builds the image from the Dockerfile and deploys the app. When it finishes, the bot process is running and listening for Telegram updates.

Optional: open the app URL in a browser (you’ll see a simple “OK” or health response; the bot has no web UI):

```bash
fly open
```

---

## 4. Verify

- **Machine status:**  
  `fly status`  
  You should see a machine in `started` state.

- **Logs:**  
  `fly logs`  
  Look for lines like “Nadobro is live! Pure bot mode running.” and “Health check listening on port 8080”. Errors (e.g. missing or invalid secrets) will appear here.

- **Telegram:**  
  In Telegram, send `/start` or another command to your bot. It should reply. If it doesn’t, check `fly logs` for errors.

---

## 5. Spending limit (recommended)

Fly bills by usage; they often waive small bills (e.g. under about $5/month), but you can cap spend:

1. Open [fly.io/dashboard](https://fly.io/dashboard).
2. Go to **Account** (or **Organization**) → **Billing** / **Spending**.
3. Set a limit (e.g. **$5**) and save.

A single small machine (256MB) usually stays within a few dollars per month.

---

## Troubleshooting

| Problem | What to do |
|--------|------------|
| **Build fails** (e.g. `Missing workspace member nadobro-bot` or `uv sync` error) | The repo uses a `.dockerignore` that excludes a stale `uv.lock` so the image builds from `pyproject.toml` only. If you see a different uv error, ensure `pyproject.toml` is valid and try `fly deploy` again. For reproducible builds, install [uv](https://docs.astral.sh/uv/), run `uv lock`, commit `uv.lock`, then remove `uv.lock` from `.dockerignore` and add it back to the Dockerfile `COPY` line. |
| **App exits or won’t start** | Run `fly logs` and look for Python tracebacks or “missing required environment variables”. Fix secrets with `fly secrets set ...` and redeploy. |
| **Bot doesn’t respond in Telegram** | Confirm `TELEGRAM_TOKEN` is correct and the bot is not stopped in @BotFather. Check `fly logs` for Telegram API errors. |
| **Wrong or updated secrets** | Run `fly secrets set VAR="value"` for each changed variable, then `fly deploy`. |

---

## Useful commands

- `fly logs` — stream logs
- `fly status` — app and machine status
- `fly ssh console` — open a shell in the running VM (debugging)
- `fly deploy` — rebuild and deploy after code or config changes
- `fly apps destroy nadobro-bot` — delete the app (only when you want to remove it)

After deployment, the bot runs 24/7 until you stop or destroy the app.
