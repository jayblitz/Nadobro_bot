import asyncio
import hmac
import logging
import os
import re
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from relay.db import close_db, init_db
from relay.event_store import find_session_for_incoming, poll_events, store_event
from relay.session_manager import (
    cleanup_idle_sessions,
    close_session,
    create_session,
    reply_option_to_session,
    reply_to_session,
)
from relay.telegram_client import set_message_callback, start_client, stop_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("relay")

_cleanup_task: Optional[asyncio.Task] = None


async def _periodic_cleanup() -> None:
    while True:
        try:
            await asyncio.sleep(60)
            await cleanup_idle_sessions()
        except asyncio.CancelledError:
            break
        except Exception:
            logger.warning("Cleanup tick failed", exc_info=True)


async def _on_lowiqpts_message(
    *,
    chat_id: int,
    sender_id: int,
    text: str,
    options: Optional[list[str]] = None,
    source_message_id: Optional[int] = None,
) -> None:
    session_id = await find_session_for_incoming(sender_id)
    if not session_id:
        logger.debug("No active session for sender_id=%s, dropping message", sender_id)
        return
    await store_event(session_id, text, options=options, source_message_id=source_message_id)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _cleanup_task
    await init_db()
    set_message_callback(_on_lowiqpts_message)
    await start_client()
    _cleanup_task = asyncio.create_task(_periodic_cleanup())
    logger.info("Relay service started")
    yield
    if _cleanup_task:
        _cleanup_task.cancel()
        try:
            await _cleanup_task
        except asyncio.CancelledError:
            pass
    await stop_client()
    await close_db()
    logger.info("Relay service stopped")


app = FastAPI(title="LOWIQPTS Relay", lifespan=lifespan)

_bearer = HTTPBearer(auto_error=False)


def _expected_token() -> str:
    return (os.environ.get("RELAY_AUTH_TOKEN") or "").strip()


async def verify_auth(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer),
) -> None:
    token = _expected_token()
    if not token:
        raise HTTPException(status_code=503, detail="RELAY_AUTH_TOKEN not configured")
    if credentials is None or not hmac.compare_digest(credentials.credentials, token):
        raise HTTPException(status_code=401, detail="Unauthorized")


_WALLET_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


def _normalize_wallet(wallet: str) -> str:
    value = str(wallet or "").strip()
    if not _WALLET_RE.fullmatch(value):
        raise HTTPException(status_code=422, detail="Invalid wallet address")
    return value


class StartRequest(BaseModel):
    telegram_user_id: int
    chat_id: int
    wallet: str
    request_id: str


class ReplyRequest(BaseModel):
    session_id: str
    text: str


class CloseRequest(BaseModel):
    session_id: str
    reason: Optional[str] = None


class ReplyOptionRequest(BaseModel):
    session_id: str
    option_text: str
    source_message_id: int


@app.get("/health")
async def health():
    return {"ok": True, "service": "lowiqpts-relay"}


@app.post("/sessions/start")
async def start_session_endpoint(
    body: StartRequest,
    _auth: None = Depends(verify_auth),
):
    try:
        result = await create_session(
            telegram_user_id=body.telegram_user_id,
            chat_id=body.chat_id,
            wallet=_normalize_wallet(body.wallet),
            request_id=body.request_id,
        )
        if not result.get("ok") and result.get("error") == "busy":
            raise HTTPException(status_code=409, detail=result.get("detail", "Another session is active"))
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error("start_session failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to start session")


@app.post("/sessions/reply")
async def reply_session_endpoint(
    body: ReplyRequest,
    _auth: None = Depends(verify_auth),
):
    result = await reply_to_session(body.session_id, body.text)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error", "session_error"))
    return result


@app.get("/events/poll")
async def poll_events_endpoint(
    session_id: str = Query(..., min_length=1),
    cursor: Optional[str] = Query(default=None),
    limit: int = Query(default=25, ge=1, le=100),
    _auth: None = Depends(verify_auth),
):
    return await poll_events(session_id=session_id, cursor=cursor, limit=limit)


@app.post("/sessions/close")
async def close_session_endpoint(
    body: CloseRequest,
    _auth: None = Depends(verify_auth),
):
    result = await close_session(body.session_id, body.reason)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error", "session_error"))
    return result


@app.post("/sessions/reply_option")
async def reply_option_endpoint(
    body: ReplyOptionRequest,
    _auth: None = Depends(verify_auth),
):
    result = await reply_option_to_session(
        body.session_id,
        body.option_text,
        body.source_message_id,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error", "session_error"))
    return result


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run("relay.main:app", host="0.0.0.0", port=port, log_level="info")
