from pathlib import Path

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_FALLBACK = _DATA_DIR / "start-bot.png"


def mascot_path_for_cost(cost_per_point: float) -> str:
    if cost_per_point < 8:
        candidate = _DATA_DIR / "nadobro2_happy.png"
    elif cost_per_point > 15:
        candidate = _DATA_DIR / "nadobro2_sad.png"
    else:
        candidate = _DATA_DIR / "nadobro2_neutral.png"

    if candidate.exists():
        return str(candidate)
    if _FALLBACK.exists():
        return str(_FALLBACK)
    return ""


def mascot_caption_for_cost(cost_per_point: float) -> str:
    if cost_per_point < 8:
        return "😄 Robot mood: HAPPY mode activated. Cost/Point looks elite."
    if cost_per_point > 15:
        return "😟 Robot mood: We can improve this legend! Let's tighten execution."
    return "🙂 Robot mood: Neutral. Steady farming, keep optimizing."
