import json
import time
import logging
from pathlib import Path


DEBUG_LOG_PATH = Path("/tmp/nadobro_debug.log")
DEBUG_STDOUT_LOGGER = logging.getLogger("nadobro.debug")


def debug_log(run_id: str, hypothesis_id: str, location: str, message: str, data: dict):
    payload = {
        "id": f"log_{int(time.time() * 1000)}_{hypothesis_id}",
        "runId": run_id,
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": int(time.time() * 1000),
    }
    try:
        with DEBUG_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=True) + "\n")
    except Exception:
        pass
    try:
        DEBUG_STDOUT_LOGGER.info("DBG %s", json.dumps(payload, ensure_ascii=True))
    except Exception:
        pass
