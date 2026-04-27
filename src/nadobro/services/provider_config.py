"""Provider environment resolution shared by runtime code and provider catalog."""

from __future__ import annotations

import os


def env_first(*names: str) -> str:
    for name in names:
        value = (os.environ.get(name) or "").strip()
        if value:
            return value
    return ""


def nanogpt_api_key() -> str:
    return env_first("NANOGPT_API_KEY", "NANO_GPT_API_KEY")


def nanogpt_base_url() -> str:
    return (os.environ.get("NANOGPT_BASE_URL") or "https://nano-gpt.com/api/v1").strip().rstrip("/")


def nanogpt_configured() -> bool:
    return bool(nanogpt_api_key())


def dmind_configured() -> bool:
    return bool(env_first("DMIND_API_KEY"))


def n8n_base_url() -> str:
    return env_first("N8N_BASE_URL", "n8n_Server_URL", "N8N_SERVER_URL").rstrip("/")


def n8n_auth_header() -> tuple[str, str] | None:
    api_key = env_first("N8N_API_KEY")
    if api_key:
        return "X-N8N-API-KEY", api_key
    auth = env_first("n8n_authorization")
    if auth:
        scheme = auth.split(None, 1)[0].lower() if auth else ""
        if scheme in ("bearer", "basic"):
            return "Authorization", auth
        return "X-N8N-API-KEY", auth
    mcp_token = env_first("n8n_MCP_Access_Token")
    if mcp_token:
        return "X-N8N-API-KEY", mcp_token
    return None


def n8n_deploy_headers() -> dict[str, str]:
    headers: dict[str, str] = {"Content-Type": "application/json"}
    auth = n8n_auth_header()
    if auth:
        headers[auth[0]] = auth[1]
    return headers


def n8n_configured() -> bool:
    return bool(n8n_base_url() and n8n_auth_header())


def provider_configured(provider: str, api_key_env: str = "") -> bool:
    if provider == "nanogpt":
        return nanogpt_configured()
    if provider == "dmind":
        return dmind_configured()
    if provider == "n8n":
        return n8n_configured()
    if not api_key_env:
        return True
    return bool(env_first(api_key_env))
