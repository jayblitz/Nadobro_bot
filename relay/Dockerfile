FROM python:3.11-slim

WORKDIR /app

# Build context is usually the repo root; RELAY_ROOT matches that folder (see fly.toml).
# From `relay/` as context: `fly deploy --build-arg RELAY_ROOT=.`
ARG RELAY_ROOT=relay

COPY ${RELAY_ROOT}/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

RUN mkdir -p /app/relay
COPY ${RELAY_ROOT}/ /app/relay/

ENV PORT=8080
EXPOSE 8080

CMD ["python", "-m", "uvicorn", "relay.main:app", "--host", "0.0.0.0", "--port", "8080", "--log-level", "info"]
