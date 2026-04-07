# --- Bot runtime + nginx ---
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
COPY deploy/nginx-miniapp.conf /etc/nginx/nginx.conf
RUN nginx -t
COPY docker-entrypoint.sh ./
RUN chmod +x docker-entrypoint.sh

EXPOSE 8080
ENV TELEGRAM_WEBHOOK_PORT=8082
ENTRYPOINT ["./docker-entrypoint.sh"]
