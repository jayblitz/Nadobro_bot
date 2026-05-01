import logging
import re
from typing import Any


_BOT_TOKEN_RE = re.compile(r"/bot\d+:[A-Za-z0-9_-]+(?=/|\s|$)")
_HEX_SECRET_RE = re.compile(r"\b0x[a-fA-F0-9]{40,128}\b")
_BEARER_RE = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{12,}")
_ACCOUNT_ID_RE = re.compile(r'(?i)("?account_id"?\s*[:=]\s*)\d{8,}')
_BARE_LONG_ID_RE = re.compile(r"(?<![\w.-])\d{8,}(?![\w.-])")
_LONG_HEX_RE = re.compile(r"\b[a-fA-F0-9]{14,128}\b")
_IPV4_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
_IPV6_RE = re.compile(r"\b(?:[a-fA-F0-9]{1,4}:){2,}[a-fA-F0-9:]{1,}\b")
_SUPABASE_HOST_RE = re.compile(r"\b[a-z0-9-]+\.pooler\.supabase\.com\b", re.IGNORECASE)
_FLY_INTERNAL_RE = re.compile(r"\bfdaa:[a-fA-F0-9:]+\b")
_URL_CREDENTIALS_RE = re.compile(r"([a-z][a-z0-9+.-]*://)([^/\s:@]+):([^@\s/]+)@", re.IGNORECASE)
_PRIVATE_KEY_FIELD_RE = re.compile(
    r"(?i)\b(private[_-]?key|secret|api[_-]?key|authorization|bearer|token)\b"
    r"(\s*[:=]\s*)"
    r"([A-Za-z0-9_\-./:+]{12,})"
)


def redact_sensitive_text(value: Any) -> Any:
    """
    Redact secrets and account identifiers from text while preserving non-string
    values so %-style logging keeps numeric formatting semantics.
    """
    if not isinstance(value, str):
        text = str(value)
        redacted = redact_sensitive_text(text)
        return redacted if redacted != text else value

    text = _URL_CREDENTIALS_RE.sub(r"\1<REDACTED>:<REDACTED>@", value)
    text = _BOT_TOKEN_RE.sub("/bot<REDACTED>", text)
    text = _BEARER_RE.sub("Bearer <REDACTED>", text)
    text = _HEX_SECRET_RE.sub("0x<REDACTED>", text)
    text = _ACCOUNT_ID_RE.sub(lambda m: f"{m.group(1)}<REDACTED>", text)
    text = _PRIVATE_KEY_FIELD_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}<REDACTED>", text)
    text = _SUPABASE_HOST_RE.sub("<REDACTED_DB_HOST>", text)
    text = _FLY_INTERNAL_RE.sub("<REDACTED_IPV6>", text)
    text = _IPV4_RE.sub("<REDACTED_IP>", text)
    text = _IPV6_RE.sub("<REDACTED_IPV6>", text)
    text = _LONG_HEX_RE.sub("<REDACTED_HEX>", text)
    text = _BARE_LONG_ID_RE.sub("<REDACTED_ID>", text)
    return text


class SensitiveDataRedactFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = redact_sensitive_text(record.msg)
        if record.args:
            if isinstance(record.args, dict):
                record.args = {k: redact_sensitive_text(v) for k, v in record.args.items()}
            elif isinstance(record.args, tuple):
                record.args = tuple(redact_sensitive_text(arg) for arg in record.args)
            else:
                record.args = redact_sensitive_text(record.args)
        return True


class RedactingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return redact_sensitive_text(super().format(record))
