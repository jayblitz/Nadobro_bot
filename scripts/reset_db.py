#!/usr/bin/env python3
"""
Reset Nadobro database: truncates users, trades, alerts, bot_state, admin_logs.

Run from project root with .env set (or SUPABASE_DATABASE_URL):
  python scripts/reset_db.py
  # or with venv: .venv/bin/python scripts/reset_db.py

Type 'yes' when prompted to confirm. All application data will be deleted.
"""
import sys
from pathlib import Path

# Ensure project root is on path when run as scripts/reset_db.py
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from dotenv import load_dotenv

load_dotenv()

from src.nadobro.db import execute

TRUNCATE_SQL = """
TRUNCATE users, trades, alerts, bot_state, admin_logs RESTART IDENTITY CASCADE;
"""


def main():
    confirm = input("Reset database? This will DELETE all users, trades, alerts, and state. Type 'yes' to confirm: ")
    if confirm.strip().lower() != "yes":
        print("Aborted.")
        sys.exit(0)

    try:
        execute(TRUNCATE_SQL)
        print("Database reset complete. All tables truncated.")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
