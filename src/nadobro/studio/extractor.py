"""LLM-backed natural-language extractor for Strategy Studio."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services import bro_llm
from src.nadobro.studio.intent import TradingIntent, intent_from_json
from src.nadobro.studio.prompts import FEW_SHOTS, SYSTEM_PROMPT


def _messages(raw: str, prior: Optional[TradingIntent], history: list[dict]) -> list[dict]:
    now = datetime.now(timezone.utc).isoformat()
    messages = [
        {"role": "system", "content": f"{SYSTEM_PROMPT}\nCurrent UTC time: {now}"},
    ]
    for shot in FEW_SHOTS:
        payload = dict(shot["json"])
        payload["raw_input"] = shot["user"]
        messages.append({"role": "user", "content": shot["user"]})
        messages.append({"role": "assistant", "content": json.dumps(payload)})
    if prior is not None:
        messages.append({"role": "system", "content": f"Prior intent JSON: {prior.json()}"})
    for item in history[-8:]:
        role = item.get("role", "user")
        content = item.get("content", "")
        if content:
            messages.append({"role": role, "content": str(content)})
    messages.append({"role": "user", "content": raw})
    return messages


async def extract(raw: str, prior: Optional[TradingIntent], history: list[dict]) -> TradingIntent:
    payload, _provider = await run_blocking(bro_llm.chat_json, _messages(raw, prior, history), None)
    return intent_from_json(payload, raw_fallback=raw)
