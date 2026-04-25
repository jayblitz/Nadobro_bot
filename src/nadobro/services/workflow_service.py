"""n8n-backed workflow orchestration for Nadobro.

Nadobro owns execution and risk. n8n owns workflow visualization and node
orchestration. This service translates Telegram/user intents into safe workflow
definitions and exposes action previews for API routes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import os
import time
from typing import Any

import requests

from src.nadobro.models.database import get_bot_state, set_bot_state
from src.nadobro.services.source_registry import record_source

WORKFLOW_PREFIX = "workflow:"


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


WORKFLOW_TEMPLATES: dict[str, list[WorkflowNode]] = {
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
    return [
        {"id": key, "nodes": [node.to_dict() for node in nodes]}
        for key, nodes in WORKFLOW_TEMPLATES.items()
    ]


def _workflow_key(user_id: int, workflow_id: str) -> str:
    return f"{WORKFLOW_PREFIX}{int(user_id)}:{workflow_id}"


def build_workflow_from_prompt(prompt: str) -> dict[str, Any]:
    text = (prompt or "").lower()
    if "funding" in text:
        template_id = "funding_recommend_strategy"
    elif "risk" in text or "pause" in text:
        template_id = "risk_off_pause"
    elif "recover" in text or "failed" in text:
        template_id = "session_failed_recover"
    else:
        template_id = "price_monitor_notify"
    nodes = [node.to_dict() for node in WORKFLOW_TEMPLATES[template_id]]
    digest = hashlib.sha256(json.dumps({"prompt": prompt, "nodes": nodes}, sort_keys=True).encode()).hexdigest()[:16]
    return {
        "id": digest,
        "template_id": template_id,
        "prompt": prompt,
        "nodes": nodes,
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
    base_url = os.environ.get("N8N_BASE_URL", "").rstrip("/")
    api_key = os.environ.get("N8N_API_KEY", "")
    if not base_url or not api_key:
        record_source(
            "n8n",
            ttl_seconds=30,
            confidence=0.0,
            detail="n8n not configured",
            allowed_use="workflow",
            source_url="https://n8n.io/",
        )
        return {"ok": False, "error": "N8N_BASE_URL and N8N_API_KEY are required", "workflow": workflow}

    payload = {
        "name": f"Nadobro {workflow.get('template_id', 'workflow')} {workflow.get('id')}",
        "active": False,
        "nodes": workflow.get("nodes", []),
        "connections": {},
        "settings": {"timezone": "UTC"},
    }
    started = time.perf_counter()
    try:
        resp = requests.post(
            f"{base_url}/api/v1/workflows",
            headers={"X-N8N-API-KEY": api_key, "Content-Type": "application/json"},
            json=payload,
            timeout=10,
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
