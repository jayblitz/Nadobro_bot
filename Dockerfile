# Nadobro — Python 3.12, uv-friendly
FROM python:3.12-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency files (uv.lock excluded via .dockerignore when stale)
COPY pyproject.toml ./
RUN uv sync --no-dev --no-install-project

# Copy app
COPY . .
RUN uv sync --no-dev

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["uv", "run", "python", "main.py"]
