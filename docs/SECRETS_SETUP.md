# Step-by-step: Providing API keys and secrets securely

Follow these steps to set up your API keys and secrets **without** putting them in code or sharing them in chat.

---

## Replit-style: Secure paste in the terminal (recommended)

To avoid typing or pasting keys into any file in your editor, use the setup script. It prompts for each secret in the terminal and **masks your input**; values are written only to `.env`.

From the **project root** (the folder that contains `main.py`):

```bash
python run_setup_secrets.py
```

- You’ll be prompted for each required key (TELEGRAM_TOKEN, SUPABASE_URL, SUPABASE_KEY, ENCRYPTION_KEY), then optional ones (XAI_API_KEY, OPENAI_API_KEY, ADMIN_USER_IDS).
- **Paste each value and press Enter.** The terminal will not echo what you paste (when run in a normal terminal).
- The script writes only to `.env` in the same folder as `main.py`. Nothing is printed or logged.
- If `.env` already exists, you’ll be asked whether to overwrite it.

**Important:** Run this from the **same directory where you run `main.py`**. If your repo is in `/Users/jerry/Nadobro_bot`, `cd` there first, then run `python run_setup_secrets.py`. If you use a worktree, run it from that worktree’s root.

After it finishes, run the bot with `python main.py`. No need to open `.env` in the IDE.

---

## 1. Use a local `.env` file (never committed)

The bot loads secrets from environment variables. For local development, use a `.env` file in the project root.

### 1.1 Create `.env` from the example

```bash
cd /path/to/Nadobro_bot   # or your project directory
cp .env.example .env
```

### 1.2 Confirm `.env` is ignored by Git

```bash
git check-ignore -v .env
```

You should see something like `.gitignore:39:.env`. If not, add this line to `.gitignore`:

```
.env
```

**Rule:** Never run `git add .env` or commit `.env`. Your secrets stay only on your machine (or in your deployment’s secret store).

---

## 2. Get each required value

### 2.1 `TELEGRAM_TOKEN`

1. Open Telegram and search for **@BotFather**.
2. Send `/newbot` (or use an existing bot and send `/token`).
3. Follow the prompts (name, username).
4. Copy the token BotFather sends (e.g. `7123456789:AAH...`).
5. In `.env`, set:
   ```env
   TELEGRAM_TOKEN=paste_here_without_quotes
   ```

### 2.2 `SUPABASE_URL` and `SUPABASE_KEY`

1. Go to [supabase.com](https://supabase.com) and sign in.
2. Open your project (or create one).
3. Go to **Project Settings** (gear) → **API**.
4. Copy:
   - **Project URL** → `SUPABASE_URL`
   - **Project API keys** → **service_role** (secret) → `SUPABASE_KEY`
5. In `.env`:
   ```env
   SUPABASE_URL=https://xxxxxxxx.supabase.co
   SUPABASE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
   ```
   **Important:** Use the `service_role` key, not the public `anon` key. Never expose `service_role` in frontend code or in chat.

### 2.3 `ENCRYPTION_KEY`

This key is used to encrypt linked signer private keys in the database. Generate it once and keep it secret.

**Option A – Generate a Fernet key (recommended):**

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Copy the output (e.g. `xYz123...=`) into `.env`:

```env
ENCRYPTION_KEY=xYz123...=
```

**Option B – Use a long random secret:**

Use at least 32 random characters (e.g. from a password manager). The app will derive an encryption key from it:

```env
ENCRYPTION_KEY=your_32_or_more_character_secret
```

**Rule:** If you lose this key, encrypted wallet data cannot be decrypted. Store a backup in a secure place (e.g. password manager), not in the repo.

---

## 3. Optional variables (in `.env`)

Add these only if you need the feature.

| Variable | How to get it | Purpose |
|----------|----------------|---------|
| `XAI_API_KEY` | xAI / Grok API dashboard | “Ask Nado” with xAI |
| `OPENAI_API_KEY` | platform.openai.com → API keys | “Ask Nado” with OpenAI |
| `ADMIN_USER_IDS` | Your Telegram user ID (e.g. from @userinfobot) | Comma-separated list, e.g. `123456789,987654321` |

Example:

```env
# Optional: at least one needed for AI support
XAI_API_KEY=xai-...
OPENAI_API_KEY=sk-...

# Optional: Telegram user IDs that can use admin commands
ADMIN_USER_IDS=123456789
```

---

## 4. Check that secrets are loaded

From the project root:

```bash
# Load .env and run the bot’s config check (if you have a check command)
python -c "
from dotenv import load_dotenv
load_dotenv()
import os
required = ['TELEGRAM_TOKEN', 'SUPABASE_URL', 'SUPABASE_KEY', 'ENCRYPTION_KEY']
missing = [k for k in required if not os.environ.get(k)]
print('Missing:', missing if missing else 'None – all required keys set.')
"
```

If you see `Missing: None`, all required keys are set. Then start the bot (e.g. `python main.py`); it will validate `ENCRYPTION_KEY` at startup.

---

## 5. Production / deployment

**Do not** put real secrets in code or in a committed file. Use your platform’s secret store and map them to the same variable names.

| Environment | Where to set secrets |
|-------------|------------------------|
| **Docker** | `docker run -e TELEGRAM_TOKEN=...` or a `--env-file` pointing at a **non-committed** file; or Docker Secrets. |
| **Docker Compose** | `env_file: .env` (keep `.env` out of the image and repo) or list variables under `environment:` with values from a secret manager. |
| **Cloud (e.g. Render, Fly, Railway)** | Project → Environment / Variables → add each variable; mark as “secret” if available. |
| **VPS / server** | Create a `.env` on the server (e.g. `nano .env`), paste values, then run the app with `load_dotenv()` or `export $(cat .env | xargs)` in a script that is not committed. |

Use the same variable names as in `.env.example` so the app does not need code changes.

---

## 6. Security checklist

- [ ] `.env` is in `.gitignore` and never committed.
- [ ] You have not pasted secrets in chat, issue trackers, or screenshots.
- [ ] Supabase key in use is `service_role` and is only in server-side env.
- [ ] `ENCRYPTION_KEY` is generated once, stored in a password manager or secret store, and not shared.
- [ ] In production, secrets are in the platform’s secret manager or a non-committed env file, not in the repo.

If you follow these steps, your API keys and secrets are provided securely to the bot.
