"""Client IP extraction for ASGI scopes (Fly: X-Forwarded-For)."""


def client_ip_from_scope(scope: dict) -> str:
    headers = {k.decode().lower(): v.decode() for k, v in scope.get("headers", [])}
    xff = headers.get("x-forwarded-for", "")
    if xff:
        return xff.split(",")[0].strip()
    client = scope.get("client")
    if client and len(client) >= 1:
        return str(client[0])
    return "unknown"
