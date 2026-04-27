"""n8n-backed workflow orchestration for Nadobro.

Nadobro owns execution and risk. n8n owns workflow visualization and node
orchestration. Workflows are generated as **valid n8n JSON** (nodes + connections)
either by an LLM (NanoGPT recommended) or by small starter templates.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import logging
import os
import time
from typing import Any
from uuid import uuid4

import requests

from src.nadobro.models.database import get_bot_state, set_bot_state
from src.nadobro.services.nanogpt_client import (
    extract_json_object,
    nanogpt_chat_completion,
    nanogpt_is_configured,
    openai_compatible_chat,
)
from src.nadobro.services.provider_config import n8n_base_url, n8n_configured, n8n_deploy_headers
from src.nadobro.services.source_registry import record_source

logger = logging.getLogger(__name__)

WORKFLOW_PREFIX = "workflow:"


def _n8n_base_url() -> str:
    """Resolve n8n origin: standard env first, then Fly/Cursor-style ``n8n_Server_URL``."""
    return n8n_base_url()


def _n8n_deploy_headers() -> dict[str, str]:
    """Auth for n8n REST API: ``X-N8N-API-KEY`` or ``Authorization`` (Bearer/Basic).

    Accepts the same secret names used for MCP / Fly: ``n8n_authorization``,
    ``n8n_MCP_Access_Token``, or ``N8N_API_KEY``.
    """
    return n8n_deploy_headers()


def _n8n_deploy_ready() -> bool:
    return n8n_configured()

N8N_WORKFLOW_SYSTEM_PROMPT = """You are an expert n8n workflow author. The user describes automation they want.
Return ONLY a single JSON object (no markdown, no commentary) with this exact shape:
{
  "name": "short descriptive workflow title",
  "nodes": [ ... ],
  "connections": { ... },
  "settings": { "timezone": "UTC" },
  "setup_guide": "2-6 sentences: what this workflow does, which nodes need credentials or URL edits, and how to test with 'Execute workflow'."
}

Rules for nodes:
- Each node MUST include: "id" (unique string), "name" (unique within the workflow), "type", "typeVersion" (number), "position" ([x, y] integers), "parameters" (object).
- Use only standard n8n node types, for example:
  - n8n-nodes-base.manualTrigger (typeVersion 1) — for easy testing
  - n8n-nodes-base.scheduleTrigger (typeVersion 1.2) — for cron-style runs
  - n8n-nodes-base.webhook (typeVersion 2) — if the user wants an external HTTP trigger
  - n8n-nodes-base.httpRequest (typeVersion 4.2) — for REST calls (set url, method, options)
  - n8n-nodes-base.if (typeVersion 2) — branching
  - n8n-nodes-base.set (typeVersion 3.4) — assign fields
  - n8n-nodes-base.code (typeVersion 2) — only if truly needed; keep JavaScript minimal
- Prefer manualTrigger at the start unless the user explicitly asked for a schedule or webhook.
- connections MUST wire nodes by **name** (exact string match). Shape:
  "connections": {
    "Source Node Name": {
      "main": [[{"node": "Target Node Name", "type": "main", "index": 0}]]
    }
  }
- Use realistic placeholder URLs the user can edit (e.g. their Nadobro Mini App API or Nado gateway). Never invent secrets.
- Keep workflows small (usually 3–8 nodes) and actionable for a beginner.
"""


@dataclass
class WorkflowNode:
    node_type: str
    name: str
    parameters: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.node_type,
            "name": self.name,
            "parameters": self.parameters,
        }


def _n8n_id() -> str:
    return str(uuid4())


def _starter_setup_guide(template_id: str) -> str:
    return (
        f"This is a starter '{template_id}' workflow you can open in n8n. "
        "Click 'Test workflow' on the first node, then inspect the HTTP response. "
        "Replace placeholder URLs with your Mini App API base or Nado endpoints, and add n8n "
        "Credentials where needed (none required for the demo HTTP step)."
    )


def _fallback_price_monitor() -> dict[str, Any]:
    t = _n8n_id()
    s = _n8n_id()
    h = _n8n_id()
    trig = "When clicking 'Test workflow'"
    setn = "Explain request"
    http = "Fetch BTC price (demo)"
    return {
        "name": "Nadobro starter — price check (demo)",
        "nodes": [
            {
                "parameters": {},
                "id": t,
                "name": trig,
                "type": "n8n-nodes-base.manualTrigger",
                "typeVersion": 1,
                "position": [0, 0],
            },
            {
                "parameters": {
                    "assignments": {
                        "assignments": [
                            {
                                "id": _n8n_id(),
                                "name": "symbol",
                                "value": "bitcoin",
                                "type": "string",
                            }
                        ]
                    },
                    "options": {},
                },
                "id": s,
                "name": setn,
                "type": "n8n-nodes-base.set",
                "typeVersion": 3.4,
                "position": [220, 0],
            },
            {
                "parameters": {
                    "url": "=https://api.coingecko.com/api/v3/simple/price?ids={{$json.symbol}}&vs_currencies=usd",
                    "options": {},
                },
                "id": h,
                "name": http,
                "type": "n8n-nodes-base.httpRequest",
                "typeVersion": 4.2,
                "position": [440, 0],
            },
        ],
        "connections": {
            trig: {"main": [[{"node": setn, "type": "main", "index": 0}]]},
            setn: {"main": [[{"node": http, "type": "main", "index": 0}]]},
        },
        "settings": {"timezone": "UTC"},
        "setup_guide": _starter_setup_guide("price_monitor_notify"),
    }


def _fallback_funding() -> dict[str, Any]:
    t = _n8n_id()
    s = _n8n_id()
    h = _n8n_id()
    trig = "When clicking 'Test workflow'"
    setn = "Nado gateway base"
    http = "GET meta (replace path)"
    return {
        "name": "Nadobro starter — Nado gateway ping",
        "nodes": [
            {
                "parameters": {},
                "id": t,
                "name": trig,
                "type": "n8n-nodes-base.manualTrigger",
                "typeVersion": 1,
                "position": [0, 0],
            },
            {
                "parameters": {
                    "assignments": {
                        "assignments": [
                            {
                                "id": _n8n_id(),
                                "name": "gateway_base",
                                "value": "https://gateway.prod.nado.xyz/v1",
                                "type": "string",
                            }
                        ]
                    },
                    "options": {},
                },
                "id": s,
                "name": setn,
                "type": "n8n-nodes-base.set",
                "typeVersion": 3.4,
                "position": [220, 0],
            },
            {
                "parameters": {
                    "url": "={{$json.gateway_base}}/meta",
                    "options": {},
                },
                "id": h,
                "name": http,
                "type": "n8n-nodes-base.httpRequest",
                "typeVersion": 4.2,
                "position": [440, 0],
            },
        ],
        "connections": {
            trig: {"main": [[{"node": setn, "type": "main", "index": 0}]]},
            setn: {"main": [[{"node": http, "type": "main", "index": 0}]]},
        },
        "settings": {"timezone": "UTC"},
        "setup_guide": _starter_setup_guide("funding_recommend_strategy"),
    }


def _fallback_risk_pause() -> dict[str, Any]:
    t = _n8n_id()
    st = _n8n_id()
    trig = "When clicking 'Test workflow'"
    note = "Log risk-off reminder"
    return {
        "name": "Nadobro starter — risk-off checklist",
        "nodes": [
            {
                "parameters": {},
                "id": t,
                "name": trig,
                "type": "n8n-nodes-base.manualTrigger",
                "typeVersion": 1,
                "position": [0, 0],
            },
            {
                "parameters": {
                    "assignments": {
                        "assignments": [
                            {
                                "id": _n8n_id(),
                                "name": "message",
                                "value": "Risk-off: review open Nadobro strategies and reduce size if needed.",
                                "type": "string",
                            }
                        ]
                    },
                    "options": {},
                },
                "id": st,
                "name": note,
                "type": "n8n-nodes-base.set",
                "typeVersion": 3.4,
                "position": [240, 0],
            },
        ],
        "connections": {trig: {"main": [[{"node": note, "type": "main", "index": 0}]]}},
        "settings": {"timezone": "UTC"},
        "setup_guide": _starter_setup_guide("risk_off_pause"),
    }


def _fallback_recover() -> dict[str, Any]:
    t = _n8n_id()
    st = _n8n_id()
    trig = "When clicking 'Test workflow'"
    note = "Recovery reminder"
    return {
        "name": "Nadobro starter — session recovery reminder",
        "nodes": [
            {
                "parameters": {},
                "id": t,
                "name": trig,
                "type": "n8n-nodes-base.manualTrigger",
                "typeVersion": 1,
                "position": [0, 0],
            },
            {
                "parameters": {
                    "assignments": {
                        "assignments": [
                            {
                                "id": _n8n_id(),
                                "name": "message",
                                "value": "A strategy session failed: open Nadobro and run recovery / reconcile from the dashboard.",
                                "type": "string",
                            }
                        ]
                    },
                    "options": {},
                },
                "id": st,
                "name": note,
                "type": "n8n-nodes-base.set",
                "typeVersion": 3.4,
                "position": [240, 0],
            },
        ],
        "connections": {trig: {"main": [[{"node": note, "type": "main", "index": 0}]]}},
        "settings": {"timezone": "UTC"},
        "setup_guide": _starter_setup_guide("session_failed_recover"),
    }


WORKFLOW_TEMPLATES_LEGACY: dict[str, list[WorkflowNode]] = {
    "price_monitor_notify": [
        WorkflowNode("trigger.price", "Price Monitor", {"product": "BTC", "condition": "above", "value": 0}),
        WorkflowNode("action.telegram_notify", "Telegram Notify", {}),
    ],
    "funding_recommend_strategy": [
        WorkflowNode("trigger.funding", "Funding Monitor", {"product": "BTC", "condition": "above", "value": 0.0005}),
        WorkflowNode("decision.confidence_gate", "Confidence Gate", {"min_confidence": 0.7}),
        WorkflowNode("action.strategy_recommend", "Strategy Recommend", {}),
    ],
    "risk_off_pause": [
        WorkflowNode("trigger.sentiment", "Risk-Off Sentiment", {"regime": "risk_off"}),
        WorkflowNode("action.strategy_pause", "Strategy Pause", {}),
        WorkflowNode("action.telegram_notify", "Telegram Notify", {}),
    ],
    "session_failed_recover": [
        WorkflowNode("trigger.session_state", "Session Failed", {"state": "failed"}),
        WorkflowNode("action.recovery_card", "Recovery Card", {}),
    ],
}


def list_workflow_templates() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for key, nodes in WORKFLOW_TEMPLATES_LEGACY.items():
        out.append({"id": key, "nodes": [node.to_dict() for node in nodes]})
    out.append(
        {
            "id": "llm_generated",
            "description": "Describe your automation in plain English; Nadobro builds a full n8n workflow (requires NANOGPT_API_KEY or N8N_WORKFLOWS_USE_LLM).",
            "nodes": [],
        }
    )
    return out


def _workflow_key(user_id: int, workflow_id: str) -> str:
    return f"{WORKFLOW_PREFIX}{int(user_id)}:{workflow_id}"


def _workflows_llm_enabled() -> bool:
    if nanogpt_is_configured():
        return True
    flag = (os.environ.get("N8N_WORKFLOWS_USE_LLM") or "").strip().lower() in ("1", "true", "yes", "on")
    if not flag:
        return False
    return bool(os.environ.get("OPENAI_API_KEY") or os.environ.get("XAI_API_KEY"))


def _workflow_llm_complete(messages: list[dict[str, Any]]) -> tuple[bool, str]:
    if nanogpt_is_configured():
        model = (
            os.environ.get("NANOGPT_WORKFLOW_MODEL")
            or os.environ.get("NANOGPT_MODEL")
            or "chatgpt-4o-latest"
        ).strip()
        ok, text, _raw = nanogpt_chat_completion(messages, model=model, temperature=0.15, timeout=120.0)
        return ok, text
    key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if key:
        model = (os.environ.get("OPENAI_WORKFLOW_MODEL") or "gpt-4o-mini").strip()
        ok, text, _ = openai_compatible_chat(
            base_url="https://api.openai.com/v1",
            api_key=key,
            model=model,
            messages=messages,
            temperature=0.15,
            timeout=120.0,
        )
        return ok, text
    key = (os.environ.get("XAI_API_KEY") or "").strip()
    if key:
        model = (os.environ.get("XAI_WORKFLOW_MODEL") or "grok-3-mini-fast").strip()
        ok, text, _ = openai_compatible_chat(
            base_url="https://api.x.ai/v1",
            api_key=key,
            model=model,
            messages=messages,
            temperature=0.15,
            timeout=120.0,
        )
        return ok, text
    return False, ""


def _coerce_llm_workflow(data: dict[str, Any]) -> dict[str, Any] | None:
    nodes = data.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        return None
    cleaned: list[dict[str, Any]] = []
    names: set[str] = set()
    for i, node in enumerate(nodes):
        if not isinstance(node, dict):
            return None
        name = str(node.get("name") or f"Node {i + 1}")
        if name in names:
            name = f"{name} ({i + 1})"
        names.add(name)
        ntype = str(node.get("type") or "")
        if "." not in ntype:
            return None
        nid = str(node.get("id") or _n8n_id())
        tv = node.get("typeVersion", 1)
        try:
            type_version: Any = float(tv) if "." in str(tv) else int(tv)
        except (TypeError, ValueError):
            type_version = 1
        pos = node.get("position")
        if not isinstance(pos, list) or len(pos) != 2:
            pos = [240 * i, 0]
        params = node.get("parameters")
        if not isinstance(params, dict):
            params = {}
        cleaned.append(
            {
                **{k: v for k, v in node.items() if k not in ("id", "name", "type", "typeVersion", "position", "parameters")},
                "id": nid,
                "name": name,
                "type": ntype,
                "typeVersion": type_version,
                "position": [int(pos[0]), int(pos[1])],
                "parameters": params,
            }
        )
    connections = data.get("connections")
    if not isinstance(connections, dict):
        connections = {}
    settings = data.get("settings")
    if not isinstance(settings, dict):
        settings = {"timezone": "UTC"}
    setup = data.get("setup_guide")
    if not isinstance(setup, str):
        setup = (
            "Review each node: add credentials for Telegram or private APIs, set URLs to your Nadobro backend, "
            "then use 'Test workflow' starting from the manual trigger."
        )
    wname = data.get("name")
    if not isinstance(wname, str) or not wname.strip():
        wname = "Nadobro generated workflow"
    return {
        "name": wname.strip(),
        "nodes": cleaned,
        "connections": connections,
        "settings": settings,
        "setup_guide": setup.strip(),
    }


def _generate_workflow_via_llm(user_prompt: str) -> tuple[dict[str, Any] | None, str]:
    if not _workflows_llm_enabled():
        return None, ""
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": N8N_WORKFLOW_SYSTEM_PROMPT},
        {"role": "user", "content": f"User request:\n{user_prompt.strip()}"},
    ]
    ok, text = _workflow_llm_complete(messages)
    if not ok or not text.strip():
        return None, "LLM request failed or returned empty output"
    parsed = extract_json_object(text)
    if not parsed:
        return None, "Could not parse JSON workflow from LLM output"
    coerced = _coerce_llm_workflow(parsed)
    if not coerced:
        return None, "LLM JSON missing valid n8n nodes"
    return coerced, ""


def _select_fallback_template_id(prompt: str) -> str:
    text = (prompt or "").lower()
    if "funding" in text:
        return "funding_recommend_strategy"
    if "risk" in text or "pause" in text:
        return "risk_off_pause"
    if "recover" in text or "failed" in text:
        return "session_failed_recover"
    return "price_monitor_notify"


def _fallback_n8n_body(template_id: str) -> dict[str, Any]:
    if template_id == "funding_recommend_strategy":
        return _fallback_funding()
    if template_id == "risk_off_pause":
        return _fallback_risk_pause()
    if template_id == "session_failed_recover":
        return _fallback_recover()
    return _fallback_price_monitor()


def build_workflow_from_prompt(prompt: str) -> dict[str, Any]:
    llm_body: dict[str, Any] | None = None
    llm_err = ""
    if _workflows_llm_enabled():
        llm_body, llm_err = _generate_workflow_via_llm(prompt)

    if llm_body:
        template_id = "llm_generated"
        body = llm_body
    else:
        template_id = _select_fallback_template_id(prompt)
        body = _fallback_n8n_body(template_id)

    nodes = body["nodes"]
    digest = hashlib.sha256(
        json.dumps({"prompt": prompt, "nodes": nodes, "template_id": template_id}, sort_keys=True).encode()
    ).hexdigest()[:16]

    return {
        "id": digest,
        "template_id": template_id,
        "prompt": prompt,
        "name": body.get("name", "Nadobro workflow"),
        "nodes": nodes,
        "connections": body.get("connections", {}),
        "settings": body.get("settings", {"timezone": "UTC"}),
        "setup_guide": body.get("setup_guide", ""),
        "llm_error": llm_err or None,
        "status": "draft",
        "created_at": time.time(),
        "n8n_workflow_id": "",
    }


def save_workflow(user_id: int, workflow: dict[str, Any]) -> dict[str, Any]:
    workflow_id = str(workflow.get("id") or "")
    if not workflow_id:
        raise ValueError("workflow id is required")
    set_bot_state(_workflow_key(user_id, workflow_id), workflow)
    return workflow


def get_workflow(user_id: int, workflow_id: str) -> dict[str, Any] | None:
    return get_bot_state(_workflow_key(user_id, workflow_id))


def deploy_to_n8n(workflow: dict[str, Any]) -> dict[str, Any]:
    base_url = _n8n_base_url()
    req_headers = _n8n_deploy_headers()
    if not _n8n_deploy_ready():
        record_source(
            "n8n",
            ttl_seconds=30,
            confidence=0.0,
            detail="n8n not configured",
            allowed_use="workflow",
            source_url="https://n8n.io/",
        )
        return {
            "ok": False,
            "error": (
                "n8n deploy requires n8n_Server_URL (or N8N_BASE_URL) plus auth: "
                "N8N_API_KEY, or n8n_authorization (Bearer token or API key), "
                "or n8n_MCP_Access_Token"
            ),
            "workflow": workflow,
        }

    payload = {
        "name": workflow.get("name") or f"Nadobro {workflow.get('template_id', 'workflow')} {workflow.get('id')}",
        "nodes": workflow.get("nodes", []),
        "connections": workflow.get("connections", {}),
        "settings": workflow.get("settings", {"timezone": "UTC"}),
        "active": False,
    }
    started = time.perf_counter()
    try:
        resp = requests.post(
            f"{base_url}/api/v1/workflows",
            headers=req_headers,
            json=payload,
            timeout=30,
        )
        latency_ms = (time.perf_counter() - started) * 1000
        resp.raise_for_status()
        data = resp.json()
        workflow["n8n_workflow_id"] = str(data.get("id") or "")
        workflow["status"] = "deployed"
        record_source(
            "n8n",
            ttl_seconds=300,
            confidence=0.9,
            latency_ms=latency_ms,
            detail="n8n workflow deployed",
            allowed_use="workflow",
            source_url=base_url,
        )
        return {"ok": True, "workflow": workflow, "n8n": data}
    except Exception as exc:
        logger.warning("n8n deploy failed: %s", exc)
        record_source(
            "n8n",
            ttl_seconds=30,
            confidence=0.0,
            detail="n8n deploy failed",
            allowed_use="workflow",
            source_url=base_url,
        )
        return {"ok": False, "error": str(exc), "workflow": workflow}


def build_and_save_workflow(user_id: int, prompt: str, deploy: bool = False) -> dict[str, Any]:
    workflow = save_workflow(user_id, build_workflow_from_prompt(prompt))
    if deploy:
        result = deploy_to_n8n(workflow)
        if result.get("workflow"):
            save_workflow(user_id, result["workflow"])
        return result
    return {"ok": True, "workflow": workflow}
