FROM python:3.12-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install compiler for lru-dict source build on Python 3.12.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files (uv.lock excluded via .dockerignore when stale)
COPY pyproject.toml ./
RUN uv sync --no-dev --no-install-project

# Copy app
COPY . .
RUN uv sync --no-dev

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["uv", "run", "python", "main.py"]
