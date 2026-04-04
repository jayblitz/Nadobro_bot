# --- Frontend (Telegram Mini App) ---
FROM node:20-bookworm-slim AS frontend
WORKDIR /app
COPY miniapp_web/package.json miniapp_web/package-lock.json ./
RUN npm ci
COPY miniapp_web/ ./
RUN npm run build

# --- Bot + Mini App API + nginx ---
FROM python:3.11-slim-bookworm
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    nginx \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py ./
COPY src/ ./src/
COPY miniapp_api/ ./miniapp_api/
COPY deploy/nginx-miniapp.conf /etc/nginx/nginx.conf
COPY docker-entrypoint.sh ./
RUN chmod +x docker-entrypoint.sh

COPY --from=frontend /app/dist ./miniapp_web/dist

EXPOSE 8080
ENV TELEGRAM_WEBHOOK_PORT=8082
ENTRYPOINT ["./docker-entrypoint.sh"]
