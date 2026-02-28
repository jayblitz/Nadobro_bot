#!/usr/bin/env python3
"""
Secure secrets setup — Replit-style.

Run from this project's root (same folder as main.py):
  python run_setup_secrets.py

Prompts for each API key/secret. Values are masked when possible.
Writes only to .env in this folder; nothing is logged.
"""

import sys
from pathlib import Path

# Project root = folder containing this file (and main.py)
PROJECT_ROOT = Path(__file__).resolve().parent
ENV_PATH = PROJECT_ROOT / ".env"

REQUIRED = [
    ("TELEGRAM_TOKEN", "Telegram Bot token from @BotFather"),
    ("SUPABASE_URL", "Supabase project URL (https://xxx.supabase.co)"),
    ("SUPABASE_KEY", "Supabase service_role key (secret)"),
    ("ENCRYPTION_KEY", "Encryption key (run: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\")"),
]

OPTIONAL = [
    ("XAI_API_KEY", "xAI API key (optional, for Ask Nado)"),
    ("OPENAI_API_KEY", "OpenAI API key (optional)"),
    ("ADMIN_USER_IDS", "Comma-separated Telegram user IDs (optional)"),
]


def safe_getpass(prompt, skip_ok=False):
    """Use getpass if possible, else input (may echo)."""
    try:
        import getpass
        return getpass.getpass(prompt)
    except Exception:
        if skip_ok:
            print("  (input may be visible) ", end="")
        return input(prompt)


def main():
    print("Nadobro — Secure secrets setup")
    print("Paste each value and press Enter. Values are hidden when possible.")
    print(f"Will write to: {ENV_PATH}")
    print()

    if ENV_PATH.exists():
        overwrite = input(".env already exists. Overwrite? [y/N]: ").strip().lower()
        if overwrite not in ("y", "yes"):
            print("Exiting. No changes made.")
            sys.exit(0)
        print()

    lines = [
        "# Nadobro — do not commit .env",
        "",
    ]

    for name, hint in REQUIRED:
        print(f"  {name}")
        print(f"  ({hint})")
        value = safe_getpass("  Value: ").strip()
        if not value:
            print(f"  ERROR: {name} is required. Exiting.")
            sys.exit(1)
        lines.append(f"{name}={value}")
        lines.append("")
        print("  OK")
        print()

    for name, hint in OPTIONAL:
        print(f"  {name} (optional)")
        print(f"  ({hint})")
        value = safe_getpass("  Value [Enter to skip]: ", skip_ok=True).strip()
        if value:
            lines.append(f"{name}={value}")
            lines.append("")
            print("  OK")
        else:
            print("  Skipped")
        print()

    ENV_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    n = sum(1 for line in lines if line and "=" in line and not line.startswith("#"))
    print(f"Done. Wrote {ENV_PATH} with {n} variable(s).")
    print("Do not commit .env. Run the bot: python main.py")


if __name__ == "__main__":
    main()
