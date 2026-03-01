# Step-by-step: Providing API keys and secrets securely

Follow these steps to set up your API keys and secrets.

---

## Replit: Use the Secrets panel

On Replit, secrets are managed via the **Secrets** tab in the sidebar (lock icon). Add each variable there — they are injected as environment variables automatically.

---

## 1. Required secrets

### 1.1 `TELEGRAM_TOKEN`

1. Open Telegram and search for **@BotFather**.
2. Send `/newbot` (or use an existing bot and send `/token`).
3. Follow the prompts (name, username).
4. Copy the token BotFather sends (e.g. `7123456789:AAH...`).

### 1.2 `DATABASE_URL`

On Replit, this is auto-provided when you provision a PostgreSQL database. No manual setup needed.

For external deployments (Fly.io, VPS), provide a standard PostgreSQL connection string:
```
postgresql://user:password@host:5432/dbname
```

### 1.3 `ENCRYPTION_KEY`

This key is used to encrypt linked signer private keys in the database. Generate it once and keep it secret.

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Copy the output (e.g. `xYz123...=`) into your secrets.

**Rule:** If you lose this key, encrypted wallet data cannot be decrypted. Store a backup in a secure place.

---

## 2. Optional secrets

| Variable | How to get it | Purpose |
|----------|----------------|---------|
| `XAI_API_KEY` | xAI / Grok API dashboard | "Ask Nado" with xAI (primary) |
| `OPENAI_API_KEY` | platform.openai.com → API keys | "Ask Nado" with OpenAI (fallback) |
| `ADMIN_USER_IDS` | Your Telegram user ID (e.g. from @userinfobot) | Comma-separated list |

---

## 3. Check that secrets are loaded

Start the bot and check the logs. You should see:
- "Encryption key validated successfully"
- "Configuration check passed" (no warnings about missing keys)
- "Nadobro is live! Pure bot mode running."

---

## 4. Security checklist

- [ ] Secrets are in Replit Secrets panel or deployment secret store, not in code.
- [ ] You have not pasted secrets in chat, issue trackers, or screenshots.
- [ ] `ENCRYPTION_KEY` is backed up in a password manager.
- [ ] In production, secrets are in the platform's secret manager, not in the repo.
