#!/bin/sh
set -e
export PYTHONPATH=/app
export TELEGRAM_WEBHOOK_PORT="${TELEGRAM_WEBHOOK_PORT:-8082}"
export BOT_DISABLE_MINIAPP="${BOT_DISABLE_MINIAPP:-true}"

nginx -c /etc/nginx/nginx.conf &

exec python3 main.py
