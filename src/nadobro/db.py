import os
import logging
import psycopg2
import psycopg2.pool
import psycopg2.extras

logger = logging.getLogger(__name__)

_pool = None

def _prepare_db_url(url: str) -> str:
    import re
    from urllib.parse import quote
    m = re.match(r'^(postgresql|postgres)(\+\w+)?://', url)
    if not m:
        return url
    scheme_end = m.end()
    rest = url[scheme_end:]
    at_idx = rest.rfind('@')
    if at_idx < 0:
        return url
    userinfo = rest[:at_idx]
    hostpart = rest[at_idx + 1:]
    colon_idx = userinfo.find(':')
    if colon_idx < 0:
        return url
    username = userinfo[:colon_idx]
    password = userinfo[colon_idx + 1:]
    encoded_pw = quote(password, safe='')
    return f"{url[:scheme_end]}{username}:{encoded_pw}@{hostpart}"


def get_pool():
    global _pool
    if _pool is None:
        url = os.environ.get("SUPABASE_DATABASE_URL") or os.environ.get("DATABASE_URL")
        if not url:
            raise RuntimeError("Neither SUPABASE_DATABASE_URL nor DATABASE_URL environment variable is set.")
        db_label = "Supabase" if os.environ.get("SUPABASE_DATABASE_URL") else "default"
        url = _prepare_db_url(url)
        _pool = psycopg2.pool.ThreadedConnectionPool(1, 5, url)
        logger.info("PostgreSQL connection pool initialized (%s)", db_label)
    return _pool


def get_db():
    return get_pool().getconn()


def put_db(conn):
    try:
        get_pool().putconn(conn)
    except Exception:
        pass


def query_one(sql, params=None):
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None
    except Exception:
        conn.rollback()
        raise
    finally:
        put_db(conn)


def query_all(sql, params=None):
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            conn.commit()
            return [dict(r) for r in rows]
    except Exception:
        conn.rollback()
        raise
    finally:
        put_db(conn)


def execute(sql, params=None):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_db(conn)


def execute_returning(sql, params=None):
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None
    except Exception:
        conn.rollback()
        raise
    finally:
        put_db(conn)


def query_count(sql, params=None):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            conn.commit()
            return row[0] if row else 0
    except Exception:
        conn.rollback()
        raise
    finally:
        put_db(conn)


def init_db():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    telegram_id BIGINT UNIQUE NOT NULL,
                    telegram_username TEXT,
                    main_address TEXT,
                    linked_signer_address TEXT,
                    encrypted_linked_signer_pk TEXT,
                    salt TEXT,
                    language TEXT DEFAULT 'en',
                    strategy_settings JSONB DEFAULT '{}',
                    network_mode TEXT DEFAULT 'mainnet',
                    created_at TIMESTAMPTZ DEFAULT now(),
                    last_active TIMESTAMPTZ DEFAULT now(),
                    last_trade_at TIMESTAMPTZ,
                    total_trades INT DEFAULT 0,
                    total_volume_usd DOUBLE PRECISION DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS bot_state (
                    id SERIAL PRIMARY KEY,
                    key TEXT UNIQUE NOT NULL,
                    value TEXT,
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
                CREATE TABLE IF NOT EXISTS trades (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    product_id INT NOT NULL,
                    product_name TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    side TEXT NOT NULL,
                    size DOUBLE PRECISION NOT NULL,
                    price DOUBLE PRECISION,
                    leverage DOUBLE PRECISION DEFAULT 1.0,
                    status TEXT DEFAULT 'pending',
                    order_digest TEXT,
                    pnl DOUBLE PRECISION,
                    fees DOUBLE PRECISION DEFAULT 0,
                    network TEXT NOT NULL,
                    error_message TEXT,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    filled_at TIMESTAMPTZ
                );
                CREATE INDEX IF NOT EXISTS idx_trades_user_product ON trades (user_id, product_id);
                CREATE INDEX IF NOT EXISTS idx_trades_created ON trades (created_at);
                CREATE TABLE IF NOT EXISTS alerts (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    product_id INT NOT NULL,
                    product_name TEXT NOT NULL,
                    condition TEXT NOT NULL,
                    target_value DOUBLE PRECISION NOT NULL,
                    is_active BOOLEAN DEFAULT true,
                    triggered_at TIMESTAMPTZ,
                    network TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS idx_alerts_active ON alerts (user_id, is_active);
                CREATE TABLE IF NOT EXISTS admin_logs (
                    id SERIAL PRIMARY KEY,
                    admin_id BIGINT NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT,
                    created_at TIMESTAMPTZ DEFAULT now()
                );
            """)
            conn.commit()
        logger.info("Database tables verified/created")
    except Exception:
        conn.rollback()
        raise
    finally:
        put_db(conn)
