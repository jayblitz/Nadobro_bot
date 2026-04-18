def plain_text_fallback(text: str) -> str:
    """Best-effort fallback for MarkdownV2 parsing failures."""
    plain = (text or "").replace("\\", "")
    plain = plain.replace("**", "")
    plain = plain.replace("*", "")
    plain = plain.replace("`", "")
    plain = plain.replace("_", "")
    return plain
