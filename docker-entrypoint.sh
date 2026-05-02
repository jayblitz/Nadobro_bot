#!/bin/sh
set -e
export PYTHONPATH="${PYTHONPATH:-/app/site-packages:/app}"
export TELEGRAM_WEBHOOK_PORT="${TELEGRAM_WEBHOOK_PORT:-8082}"

nginx -c /etc/nginx/nginx.conf &

exec python3 main.py
