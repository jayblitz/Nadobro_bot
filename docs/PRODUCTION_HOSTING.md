# Hosting Nadobro 24/7 — Free options

The bot uses **long polling** (it must run continuously). These platforms can keep it online 24/7 for free or very low cost.

---

## Option 1: Oracle Cloud Always Free (recommended for $0 forever)

**Pros:** Truly free, no time limit, 4 ARM cores / 24 GB RAM or 2 AMD VMs.  
**Cons:** You manage a VM (SSH, install Python, set up a process manager). Credit card required for signup (verification only; $0 charge if you stay in free tier).

### Steps

1. **Sign up**  
   [oracle.com/cloud/free](https://www.oracle.com/cloud/free/) → Create account (pick a region and keep it; it can’t be changed later).

2. **Create a VM**  
   - Compute → Instances → Create Instance  
   - Name: e.g. `nadobro-bot`  
   - Image: **Ubuntu 22.04**  
   - Shape: **VM.Standard.A1.Flex** (ARM) or **VM.Standard.E2.1.Micro** (AMD, 1 GB RAM)  
   - Add your SSH public key  
   - Create

3. **Open SSH and install dependencies**
   ```bash
   ssh ubuntu@<instance-public-ip>

   sudo apt update && sudo apt install -y python3.11 python3.11-venv python3-pip git
   ```

4. **Clone and run the bot**
   ```bash
   cd ~
   git clone https://github.com/YOUR_USER/Nadobro_bot.git
   cd Nadobro_bot

   python3.11 -m venv .venv
   source .venv/bin/activate
   pip install -e .   # or: pip install -r requirements.txt if you use that

   # Set secrets (paste each when prompted; no echo)
   export TELEGRAM_TOKEN="your_token"
   export SUPABASE_URL="https://xxx.supabase.co"
   export SUPABASE_KEY="your_service_role_key"
   export ENCRYPTION_KEY="your_fernet_key"
   # Optional: XAI_API_KEY, OPENAI_API_KEY, ADMIN_USER_IDS

   python main.py
   ```

5. **Keep it running 24/7 (choose one)**  
   - **screen (simplest):**  
     `screen -S nadobro` then run `python main.py`; detach with Ctrl+A, D. Reattach with `screen -r nadobro`.  
   - **systemd (recommended):** Create `/etc/systemd/system/nadobro.service` with your env vars and `ExecStart=/home/ubuntu/Nadobro_bot/.venv/bin/python /home/ubuntu/Nadobro_bot/main.py`, then `sudo systemctl enable --now nadobro`.

Use **secrets in env only** (no `.env` in repo). You can put them in the systemd service file (restrict permissions: `chmod 600`) or in a separate env file that the service loads.

---

## Option 2: Fly.io (simple deploy, often under $5/month)

**Pros:** Docker-based, one-command deploy, good docs.  
**Cons:** No guaranteed free tier; they waive bills under ~$5/month. A single small machine often stays under that.

For a full step-by-step, see [Fly.io deployment guide](FLY_DEPLOY.md).

### Steps

1. **Install Fly CLI**  
   [fly.io/docs/hands-on/install-flyctl](https://fly.io/docs/hands-on/install-flyctl/)

2. **Log in and create app**
   ```bash
   cd /path/to/Nadobro_bot   # project root (has Dockerfile and main.py)
   fly auth login
   fly launch --no-deploy
   ```
   When prompted: pick app name, region; say no to Postgres/Redis if offered.

3. **Set secrets**
   ```bash
   fly secrets set TELEGRAM_TOKEN="your_token"
   fly secrets set SUPABASE_URL="https://xxx.supabase.co"
   fly secrets set SUPABASE_KEY="your_service_role_key"
   fly secrets set ENCRYPTION_KEY="your_fernet_key"
   # Optional:
   fly secrets set XAI_API_KEY="..."
   fly secrets set OPENAI_API_KEY="..."
   fly secrets set ADMIN_USER_IDS="123456789"
   ```

4. **Deploy**
   ```bash
   fly deploy
   fly open   # optional: open app (bot has no web UI; just confirms deploy)
   ```

5. **Check logs**
   ```bash
   fly logs
   ```

The app runs 24/7. Set a **spending limit** in the Fly dashboard to avoid surprises.

---

## Option 3: JustRunMy.App (Telegram-focused free tier)

**Pros:** Built for Telegram bots, free tier with 24/7 uptime, no credit card.  
**Cons:** Less control than a VM; confirm they support Python + your dependencies (Supabase, etc.).

- [JustRunMy.App Telegram bots](https://justrunmy.app/telegram-bots)  
- Sign up, connect repo or upload project, set env vars in their UI, deploy. Check their docs for Python and long-polling.

---

## Option 4: Render (free but sleeps — use webhooks later)

Render’s **free** web services **spin down after ~15 minutes**. Long polling will stop when the app sleeps, so the bot won’t stay 24/7 on free tier with polling.

To use Render for free with a bot you’d need to:
- Expose an HTTPS endpoint and switch the bot to **webhooks** (Telegram sends updates to your URL), and  
- Use a free “cron” or external pinger to wake the service when needed, or accept cold starts.

So Render free is **not** recommended for 24/7 polling; consider it only if you later add webhook support.

---

## Summary

| Platform           | Cost              | 24/7 on free? | Effort   |
|--------------------|-------------------|----------------|----------|
| **Oracle Always Free** | $0               | Yes            | SSH + setup |
| **Fly.io**         | Often &lt; $5/mo waived | Yes            | Low (Docker) |
| **JustRunMy.App**  | Free tier         | Yes (claimed)  | Low      |
| **Render**         | Free tier         | No (sleeps)    | Only with webhooks |

**Practical choice:** Use **Oracle Always Free** for $0 and 24/7, or **Fly.io** for the easiest deploy (and set a spending limit). Use **JustRunMy.App** if you want a managed Telegram-bot host and their stack fits your app.
