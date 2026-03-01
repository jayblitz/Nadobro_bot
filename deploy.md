# Deploying Nadobro Bot to Fly.io

## Prerequisites

- [Fly CLI](https://fly.io/docs/flyctl/install/) installed
- Fly.io account (free tier works)
- Supabase project with PostgreSQL database
- Your bot secrets ready (Telegram token, encryption key, API keys)

## Step 1: Log in to Fly.io

```bash
fly auth login
```

## Step 2: Launch the app

From the project root directory:

```bash
fly launch --no-deploy
```

When prompted:
- App name: `nadobro-bot` (or your preferred name)
- Region: select **Amsterdam (ams)** or another European region
- Skip creating a Postgres database (we're using Supabase)
- Skip creating a Redis database

## Step 3: Set your secrets

```bash
fly secrets set \
  DATABASE_URL="postgresql://postgres:YOUR_PASSWORD@db.YOUR_PROJECT.supabase.co:5432/postgres" \
  TELEGRAM_TOKEN="your-telegram-bot-token" \
  ENCRYPTION_KEY="your-encryption-key" \
  XAI_API_KEY="your-xai-api-key" \
  OPENAI_API_KEY="your-openai-api-key" \
  SESSION_SECRET="your-session-secret"
```

## Step 4: Deploy

```bash
fly deploy
```

This builds the Docker image and deploys it to the Amsterdam region.

## Step 5: Check logs

```bash
fly logs
```

You should see the bot starting up and polling for Telegram updates.

## Updating the bot

After making code changes in Replit:

1. Download your updated files (or push to GitHub)
2. Run `fly deploy` from the project directory

If using GitHub, you can set up auto-deploy:
```bash
fly launch --from-repo https://github.com/your-username/nadobro-bot
```

## Useful commands

```bash
fly status          # Check app status
fly logs            # View live logs
fly logs -a nadobro-bot  # View logs for specific app
fly ssh console     # SSH into the running machine
fly secrets list    # List configured secrets
fly restart         # Restart the app
fly scale count 1   # Ensure 1 machine is running
```

## Troubleshooting

**Bot not responding:**
- Check logs: `fly logs`
- Verify secrets are set: `fly secrets list`
- Make sure the Telegram token is correct

**Database connection errors:**
- Verify your Supabase DATABASE_URL is correct
- Make sure the Supabase project is active
- Check that the connection string uses port 5432

**Health check failures:**
- The bot exposes a health endpoint on port 8080 (configured via PORT env var)
- Fly.io uses this to verify the app is running

**Changing regions:**
- Edit `primary_region` in `fly.toml` and redeploy
