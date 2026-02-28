#!/usr/bin/env python3
"""
Secure secrets setup — Replit-style.

Run from project root:
  uv run python scripts/setup_secrets.py
  # or: python scripts/setup_secrets.py

Prompts for each API key/secret in the terminal. Values are masked (not echoed).
Writes only to .env; nothing is logged or stored elsewhere.
"""

import getpass
import sys
from pathlib import Path

# Project root = parent of scripts/
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"

REQUIRED = [
    ("TELEGRAM_TOKEN", "Telegram Bot token from @BotFather"),
    ("SUPABASE_URL", "Supabase project URL (https://xxx.supabase.co)"),
    ("SUPABASE_KEY", "Supabase service_role key (secret)"),
    ("ENCRYPTION_KEY", "Encryption key (generate: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\")"),
]

OPTIONAL = [
    ("XAI_API_KEY", "xAI API key (optional, for Ask Nado)"),
    ("OPENAI_API_KEY", "OpenAI API key (optional)"),
    ("ADMIN_USER_IDS", "Comma-separated Telegram user IDs (optional)"),
]


def main():
    print("Nadobro — Secure secrets setup")
    print("Values you type will not be shown. Paste each value and press Enter.")
    print()

    if ENV_PATH.exists():
        overwrite = input(".env already exists. Overwrite? [y/N]: ").strip().lower()
        if overwrite not in ("y", "yes"):
            print("Exiting. No changes made.")
            sys.exit(0)
        print()

    lines = [
        "# Nadobro — Telegram bot for Perps on Nado",
        "# Written by scripts/setup_secrets.py — do not commit .env",
        "",
    ]

    for name, hint in REQUIRED:
        print(f"  {name}")
        print(f"  ({hint})")
        value = getpass.getpass(f"  Value: ")
        if not value.strip():
            print(f"  ERROR: {name} is required. Exiting.")
            sys.exit(1)
        lines.append(f"{name}={value}")
        lines.append("")
        print("  OK")
        print()

    for name, hint in OPTIONAL:
        print(f"  {name} (optional)")
        print(f"  ({hint})")
        value = getpass.getpass(f"  Value [press Enter to skip]: ")
        if value.strip():
            lines.append(f"{name}={value}")
            lines.append("")
            print("  OK")
        else:
            print("  Skipped")
        print()

    ENV_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    n = sum(1 for line in lines if line and "=" in line and not line.startswith("#"))
    print(f"Done. Wrote {ENV_PATH} with {n} variable(s).")
    print("Do not commit .env. Run the bot with: uv run python main.py")


if __name__ == "__main__":
    main()
