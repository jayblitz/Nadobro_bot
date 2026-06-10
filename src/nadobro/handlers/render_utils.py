import re as _re

_HTML_TAG_RE = _re.compile(r"</?(?:b|i|u|s|code|pre|a|tg-spoiler|blockquote)(?:\s[^>]*)?>")


def plain_text_fallback(text: str) -> str:
    """Best-effort fallback for MarkdownV2/HTML parsing failures."""
    plain = _HTML_TAG_RE.sub("", text or "")
    plain = plain.replace("\\", "")
    plain = plain.replace("**", "")
    plain = plain.replace("*", "")
    plain = plain.replace("`", "")
    plain = plain.replace("_", "")
    return plain
