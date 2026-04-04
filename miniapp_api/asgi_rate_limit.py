"""ASGI middleware: enforce daily per-IP budget on /api/* HTTP traffic."""

from __future__ import annotations

from starlette.responses import JSONResponse

from src.nadobro.services.async_utils import run_blocking

from miniapp_api.ip_utils import client_ip_from_scope
from miniapp_api.rate_limit import check_rate_limit


_MUTATING = frozenset({"POST", "PUT", "PATCH", "DELETE"})


class RateLimitMiddleware:
    """Enforce daily budget on mutating /api/* calls (POST/PUT/PATCH/DELETE).

    GET polling (prices, portfolio, etc.) is excluded so a 50/day cap remains usable
    for real trading actions. WebSocket /ws/voice still increments separately in voice.py.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path") or ""
        if not path.startswith("/api"):
            await self.app(scope, receive, send)
            return

        method = (scope.get("method") or "GET").upper()
        if method not in _MUTATING:
            await self.app(scope, receive, send)
            return

        ip = client_ip_from_scope(scope)
        try:
            allowed = await run_blocking(check_rate_limit, ip)
        except Exception:
            allowed = True
        if not allowed:
            resp = JSONResponse(
                status_code=429,
                content={"detail": "Daily request limit reached for this network. Try again tomorrow."},
            )
            await resp(scope, receive, send)
            return

        await self.app(scope, receive, send)
