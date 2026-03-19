#!/usr/bin/env python3
"""
One-time Telethon login: creates the SQLite session file used by the relay.

  cd relay
  export TELEGRAM_API_ID=12345
  export TELEGRAM_API_HASH=your_hex_hash
  # Optional: same path you will use in production (default: ./relay_session)
  # export SESSION_PATH="$PWD/relay_session"

  python3 scripts/first_login.py

Telethon will prompt for phone, SMS code, and 2FA if enabled.
Produces e.g. relay_session.session (or {SESSION_PATH}.session depending on Telethon).
"""

from __future__ import annotations

import os
import sys

# Allow `python3 scripts/first_login.py` from relay/ without installing package
_RELAY_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _RELAY_ROOT not in sys.path:
    sys.path.insert(0, _RELAY_ROOT)

from telethon.sync import TelegramClient  # noqa: E402


def main() -> None:
    raw_id = os.environ.get("TELEGRAM_API_ID", "").strip()
    api_hash = os.environ.get("TELEGRAM_API_HASH", "").strip()
    if not raw_id or not api_hash:
        print(
            "Set TELEGRAM_API_ID and TELEGRAM_API_HASH (from https://my.telegram.org), then re-run.",
            file=sys.stderr,
        )
        sys.exit(1)
    try:
        api_id = int(raw_id)
    except ValueError:
        print("TELEGRAM_API_ID must be an integer.", file=sys.stderr)
        sys.exit(1)

    session = os.environ.get("SESSION_PATH", "").strip()
    if not session:
        session = os.path.join(_RELAY_ROOT, "relay_session")

    client = TelegramClient(session, api_id, api_hash)
    client.start()
    me = client.get_me()
    print("Logged in as:", getattr(me, "username", None) or me.first_name)
    client.disconnect()


if __name__ == "__main__":
    main()
