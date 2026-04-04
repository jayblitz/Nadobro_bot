#!/bin/sh
set -e
export PYTHONPATH=/app
export TELEGRAM_WEBHOOK_PORT="${TELEGRAM_WEBHOOK_PORT:-8082}"
# Mini App API (REST + voice WS); nginx fronts :8080 and proxies here.
uvicorn miniapp_api.main:app --host 127.0.0.1 --port 8081 &
nginx -c /etc/nginx/nginx.conf &
exec python main.py
