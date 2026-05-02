# syntax=docker/dockerfile:1.7
ARG PYTHON_VERSION=3.11

FROM python:${PYTHON_VERSION}-slim-bookworm AS base
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl tini libpq5 nginx \
        fonts-dejavu fonts-noto \
    && rm -rf /var/lib/apt/lists/*

FROM base AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc libpq-dev build-essential \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --target=/build/site-packages

FROM base AS runtime
ARG APP_USER=nadobro
ARG APP_UID=10001
RUN useradd --create-home --uid ${APP_UID} ${APP_USER} \
    && mkdir -p /tmp/nginx/client_body_temp /tmp/nginx/proxy_temp \
    && chown -R ${APP_USER}:${APP_USER} /tmp/nginx
ENV PYTHONPATH=/app/site-packages:/app \
    PATH=/app/site-packages/bin:$PATH \
    TELEGRAM_WEBHOOK_PORT=8082
WORKDIR /app
COPY --from=builder /build/site-packages /app/site-packages
COPY --chown=${APP_USER}:${APP_USER} main.py ./
COPY --chown=${APP_USER}:${APP_USER} assets/ ./assets/
COPY --chown=${APP_USER}:${APP_USER} src/ ./src/
COPY deploy/nginx-miniapp.conf /etc/nginx/nginx.conf
RUN nginx -t
COPY --chown=${APP_USER}:${APP_USER} docker-entrypoint.sh ./
RUN chmod +x docker-entrypoint.sh
USER ${APP_USER}
EXPOSE 8080 8082
ENTRYPOINT ["/usr/bin/tini", "--", "./docker-entrypoint.sh"]
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -fsS http://localhost:8080/health || exit 1
