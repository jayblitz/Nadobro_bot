"""Supabase async client for Nadobro. Uses service key for RLS bypass."""
import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

_supabase = None


def get_supabase():
    """Return the Supabase client (sync). Call from async via run_in_executor if needed."""
    global _supabase
    if _supabase is None:
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        if not url or not key:
            raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set.")
        from supabase import create_client
        _supabase = create_client(url, key)
        logger.info("Supabase client initialized")
    return _supabase


def init_supabase() -> bool:
    """Initialize Supabase client. Returns True if configured."""
    try:
        get_supabase()
        return True
    except Exception as e:
        logger.warning("Supabase init failed: %s", e)
        return False
