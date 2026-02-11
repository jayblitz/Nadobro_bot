import json
from datetime import datetime
from typing import Optional

from src.nadobro.models.database import BotState, get_session
from src.nadobro.services.user_service import (
    get_user,
    has_mode_private_key,
    get_user_nado_client,
)
from src.nadobro.services.debug_logger import debug_log

ONBOARDING_PREFIX = "onboarding:"

ONBOARDING_STEPS = ["welcome", "mode", "key", "funding", "risk", "template"]
SKIPPABLE_STEPS = {"risk", "template"}


def _state_key(telegram_id: int, network: str) -> str:
    return f"{ONBOARDING_PREFIX}{telegram_id}:{network}"


def _default_state() -> dict:
    return {
        "current_step": "welcome",
        "completed_steps": [],
        "skipped_steps": [],
        "selected_template": None,
        "onboarding_complete": False,
        "updated_at": datetime.utcnow().isoformat(),
    }


def _compute_complete(state: dict) -> bool:
    completed = set(state.get("completed_steps", []))
    skipped = set(state.get("skipped_steps", []))
    for step in ONBOARDING_STEPS:
        if step in SKIPPABLE_STEPS:
            if step not in completed and step not in skipped:
                return False
        elif step not in completed:
            return False
    return True


def get_onboarding_state(telegram_id: int) -> tuple[str, dict]:
    user = get_user(telegram_id)
    network = user.network_mode.value if user else "testnet"
    state = _default_state()
    with get_session() as session:
        row = session.query(BotState).filter_by(key=_state_key(telegram_id, network)).first()
        if row and row.value:
            try:
                loaded = json.loads(row.value)
                if isinstance(loaded, dict):
                    state.update(loaded)
            except Exception:
                pass
    state["onboarding_complete"] = _compute_complete(state)
    return network, state


def save_onboarding_state(telegram_id: int, network: str, state: dict):
    state = dict(state)
    state["onboarding_complete"] = _compute_complete(state)
    state["updated_at"] = datetime.utcnow().isoformat()
    payload = json.dumps(state)
    with get_session() as session:
        row = session.query(BotState).filter_by(key=_state_key(telegram_id, network)).first()
        if row:
            row.value = payload
            row.updated_at = datetime.utcnow()
        else:
            row = BotState(key=_state_key(telegram_id, network), value=payload)
            session.add(row)
        session.commit()


def update_onboarding_state(telegram_id: int, mutator):
    network, state = get_onboarding_state(telegram_id)
    mutator(state)
    save_onboarding_state(telegram_id, network, state)
    return network, state


def _first_missing_step(state: dict) -> Optional[str]:
    completed = set(state.get("completed_steps", []))
    skipped = set(state.get("skipped_steps", []))
    for step in ONBOARDING_STEPS:
        if step in SKIPPABLE_STEPS:
            if step not in completed and step not in skipped:
                return step
        elif step not in completed:
            return step
    return None


def get_resume_step(telegram_id: int) -> str:
    _, state = get_onboarding_state(telegram_id)
    if state.get("onboarding_complete"):
        return "complete"
    return _first_missing_step(state) or state.get("current_step", "welcome")


def set_current_step(telegram_id: int, step: str):
    if step not in ONBOARDING_STEPS:
        return
    update_onboarding_state(telegram_id, lambda s: s.update({"current_step": step}))


def mark_step_completed(telegram_id: int, step: str):
    if step not in ONBOARDING_STEPS:
        return

    def _mutate(state: dict):
        completed = list(dict.fromkeys(state.get("completed_steps", [])))
        if step not in completed:
            completed.append(step)
        skipped = [x for x in state.get("skipped_steps", []) if x != step]
        state["completed_steps"] = completed
        state["skipped_steps"] = skipped
        state["current_step"] = _first_missing_step(state) or "template"

    update_onboarding_state(telegram_id, _mutate)


def skip_step(telegram_id: int, step: str):
    if step not in SKIPPABLE_STEPS:
        return

    def _mutate(state: dict):
        skipped = list(dict.fromkeys(state.get("skipped_steps", [])))
        if step not in skipped:
            skipped.append(step)
        completed = [x for x in state.get("completed_steps", []) if x != step]
        state["skipped_steps"] = skipped
        state["completed_steps"] = completed
        state["current_step"] = _first_missing_step(state) or "template"

    update_onboarding_state(telegram_id, _mutate)


def set_selected_template(telegram_id: int, template_id: str):
    def _mutate(state: dict):
        state["selected_template"] = template_id
        completed = list(dict.fromkeys(state.get("completed_steps", [])))
        if "template" not in completed:
            completed.append("template")
        state["completed_steps"] = completed

    update_onboarding_state(telegram_id, _mutate)


def is_onboarding_complete(telegram_id: int) -> bool:
    _, state = get_onboarding_state(telegram_id)
    return bool(state.get("onboarding_complete"))


def get_onboarding_progress(telegram_id: int) -> dict:
    network, state = get_onboarding_state(telegram_id)
    completed = set(state.get("completed_steps", []))
    skipped = set(state.get("skipped_steps", []))
    total = len(ONBOARDING_STEPS)
    done = len(completed.union(skipped))
    return {
        "network": network,
        "state": state,
        "done": done,
        "total": total,
        "percent": int((done / total) * 100) if total else 0,
    }


def evaluate_readiness(telegram_id: int) -> dict:
    network, state = get_onboarding_state(telegram_id)
    user = get_user(telegram_id)
    has_key = has_mode_private_key(telegram_id, network)
    funded = False
    if has_key:
        try:
            client = get_user_nado_client(telegram_id)
            if client:
                bal = client.get_balance()
                funded = bool(bal.get("exists"))
        except Exception:
            funded = False

    missing_step = get_resume_step(telegram_id)
    readiness = {
        "network": network,
        "has_key": has_key,
        "funded": funded,
        "onboarding_complete": bool(state.get("onboarding_complete")),
        "missing_step": missing_step if missing_step != "complete" else None,
        "selected_template": state.get("selected_template"),
        "risk_profile_set": "risk" in set(state.get("completed_steps", []))
        or "risk" in set(state.get("skipped_steps", [])),
        "user_exists": bool(user),
    }
    # region agent log
    debug_log(
        "baseline",
        "H3",
        "onboarding_service.py:204",
        "readiness_evaluated",
        {
            "telegram_id": telegram_id,
            "network": readiness["network"],
            "has_key": readiness["has_key"],
            "funded": readiness["funded"],
            "onboarding_complete": readiness["onboarding_complete"],
            "missing_step": readiness["missing_step"],
            "selected_template": readiness["selected_template"],
        },
    )
    # endregion
    return readiness
