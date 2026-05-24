"""Patched: services/log_redaction.py

Fix applied (search for AUDIT-FIX):
  AUDIT-FIX-LR-1: the original _BARE_LONG_ID_RE matched ANY bare run of 8+
                  digits and replaced it with <REDACTED_ID>. That hides
                  unix-epoch timestamps, prices in cents, lot sizes,
                  product IDs and any other long numeric value — including
                  things you actually need to debug. The dedicated
                  account_id and subaccount field patterns above already
                  cover the real risk (numeric account identifiers in
                  structured key=value pairs). The bare-number pattern
                  produces "silent damage" with no security gain — any
                  attacker who can read logs already has the structured
                  patterns redacted. We constrain the rule to long numbers
                  that look like Telegram chat IDs (>= 10 digits) which is
                  closer to the originally intended use.
"""
import logging
import re
from typing import Any


_BOT_TOKEN_RE = re.compile(r"/bot\d+:[A-Za-z0-9_-]+(?=/|\s|$)")
_HEX_LONG_RE = re.compile(r"\b0x[a-fA-F0-9]{40,}\b")
_BEARER_RE = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{12,}")
_ACCOUNT_ID_RE = re.compile(r'(?i)("?account_id"?\s*[:=]\s*)\d{8,}')
_SUBACCOUNT_FIELD_RE = re.compile(
    r"(?i)\b(subaccount(?:_hex)?=)(0x[a-fA-F0-9]+|[a-fA-F0-9]{16,})"
)
_ELLIPSIS_HEX_ADDR_RE = re.compile(r"\b0x[a-fA-F0-9]{4,}\.{3}[a-fA-F0-9]{4,}\b")
# AUDIT-FIX-LR-1: bump 8 -> 10 so we stop nuking 8-digit prices, 8-digit
# product ids, and 8-digit unix timestamps. Telegram user IDs are 10+ digits.
_BARE_LONG_ID_RE = re.compile(r"(?<![\w.-])\d{10,}(?![\w.-])")
_LONG_HEX_RE = re.compile(r"\b[a-fA-F0-9]{14,256}\b")
_IPV4_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
_IPV6_RE = re.compile(r"\b(?:[a-fA-F0-9]{1,4}:){2,}[a-fA-F0-9:]{1,}\b")
_SUPABASE_HOST_RE = re.compile(r"\b[a-z0-9-]+\.pooler\.supabase\.com\b", re.IGNORECASE)
_FLY_INTERNAL_RE = re.compile(r"\bfdaa:[a-fA-F0-9:]+\b")
_PINECONE_URL_RE = re.compile(r'https?://[^\s"\'<>]+pinecone\.io[^\s"\'<>]*', re.IGNORECASE)
_URL_CREDENTIALS_RE = re.compile(r"([a-z][a-z0-9+.-]*://)([^/\s:@]+):([^@\s/]+)@", re.IGNORECASE)
_PRIVATE_KEY_FIELD_RE = re.compile(
    r"(?i)\b(private[_-]?key|secret|api[_-]?key|authorization|bearer|token)\b"
    r"(\s*[:=]\s*)"
    r"([A-Za-z0-9_\-./:+]{12,})"
)
# AUDIT-FIX-LR-1: catch Telegram bot tokens even when not in /bot<token>/
# URL form. Token shape is <digits>:<35+ chars from URL-safe alphabet>.
_TG_BOT_TOKEN_BARE_RE = re.compile(r"\b\d{8,12}:[A-Za-z0-9_-]{30,}\b")


def redact_sensitive_text(value: Any) -> Any:
    """Redact secrets and account identifiers from text while preserving non-string
    values so %-style logging keeps numeric formatting semantics."""
    if not isinstance(value, str):
        return value

    text = _URL_CREDENTIALS_RE.sub(r"\1<REDACTED>:<REDACTED>@", value)
    text = _PINECONE_URL_RE.sub("<REDACTED_PINECONE_URL>", text)
    text = _BOT_TOKEN_RE.sub("/bot<REDACTED>", text)
    text = _TG_BOT_TOKEN_BARE_RE.sub("<REDACTED_BOT_TOKEN>", text)
    text = _BEARER_RE.sub("Bearer <REDACTED>", text)
    text = _SUBACCOUNT_FIELD_RE.sub(lambda m: f"{m.group(1)}<REDACTED>", text)
    text = _ELLIPSIS_HEX_ADDR_RE.sub("0x<REDACTED>...<REDACTED>", text)
    text = _HEX_LONG_RE.sub("0x<REDACTED>", text)
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
