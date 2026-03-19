FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

RUN mkdir -p /app/relay
COPY . /app/relay/

ENV PORT=8080
EXPOSE 8080

CMD ["python", "-m", "uvicorn", "relay.main:app", "--host", "0.0.0.0", "--port", "8080", "--log-level", "info"]
