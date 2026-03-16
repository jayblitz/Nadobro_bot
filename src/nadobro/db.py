import os
import logging
import psycopg2
import psycopg2.pool
import psycopg2.extras

logger = logging.getLogger(__name__)

_pool = None
_DB_POOL_MIN = int(os.environ.get("DB_POOL_MIN", "1"))
_DB_POOL_MAX = int(os.environ.get("DB_POOL_MAX", "12"))

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


def _resolve_host_ipv4(url: str) -> str:
    import re
    import socket
    m = re.search(r'@([^/:]+)', url)
    if not m:
        return url
    hostname = m.group(1)
    try:
        ipv4 = socket.getaddrinfo(hostname, None, socket.AF_INET)[0][4][0]
        logger.info("Resolved %s -> %s (IPv4)", hostname, ipv4)
        return url.replace(f"@{hostname}", f"@{ipv4}") + (f"&options=-csearch_path%3Dpublic" if "?" in url else f"?options=-csearch_path%3Dpublic")
    except Exception as e:
        logger.warning("IPv4 resolution failed for %s: %s — using hostname as-is", hostname, e)
        return url


def get_pool():
    global _pool
    if _pool is None:
        url = os.environ.get("SUPABASE_DATABASE_URL") or os.environ.get("DATABASE_URL")
        if not url:
            raise RuntimeError("Neither SUPABASE_DATABASE_URL nor DATABASE_URL environment variable is set.")
        db_label = "Supabase" if os.environ.get("SUPABASE_DATABASE_URL") else "default"
        url = _prepare_db_url(url)
        if db_label == "Supabase":
            url = _resolve_host_ipv4(url)
        _pool = psycopg2.pool.ThreadedConnectionPool(_DB_POOL_MIN, _DB_POOL_MAX, url)
        logger.info("PostgreSQL connection pool initialized (%s) min=%s max=%s", db_label, _DB_POOL_MIN, _DB_POOL_MAX)
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
                    filled_at TIMESTAMPTZ,
                    close_price DOUBLE PRECISION,
                    closed_at TIMESTAMPTZ
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
                CREATE INDEX IF NOT EXISTS idx_users_last_active ON users (last_active);
                CREATE TABLE IF NOT EXISTS admin_logs (
                    id SERIAL PRIMARY KEY,
                    admin_id BIGINT NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT,
                    created_at TIMESTAMPTZ DEFAULT now()
                );
            """)
            conn.commit()

        with conn.cursor() as cur:
            for col, col_type in [("close_price", "DOUBLE PRECISION"), ("closed_at", "TIMESTAMPTZ")]:
                try:
                    cur.execute(f"ALTER TABLE trades ADD COLUMN {col} {col_type}")
                    conn.commit()
                    logger.info(f"Added column trades.{col}")
                except Exception:
                    conn.rollback()

        _NETWORK_TRADES_DDL = """
            CREATE TABLE IF NOT EXISTS trades_{net} (
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
                error_message TEXT,
                created_at TIMESTAMPTZ DEFAULT now(),
                filled_at TIMESTAMPTZ,
                close_price DOUBLE PRECISION,
                closed_at TIMESTAMPTZ
            );
            CREATE INDEX IF NOT EXISTS idx_trades_{net}_user_product ON trades_{net} (user_id, product_id);
            CREATE INDEX IF NOT EXISTS idx_trades_{net}_created ON trades_{net} (created_at);
        """
        _NETWORK_ALERTS_DDL = """
            CREATE TABLE IF NOT EXISTS alerts_{net} (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                product_id INT NOT NULL,
                product_name TEXT NOT NULL,
                condition TEXT NOT NULL,
                target_value DOUBLE PRECISION NOT NULL,
                is_active BOOLEAN DEFAULT true,
                triggered_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS idx_alerts_{net}_active ON alerts_{net} (user_id, is_active);
        """
        with conn.cursor() as cur:
            for net in ("testnet", "mainnet"):
                cur.execute(_NETWORK_TRADES_DDL.format(net=net))
                cur.execute(_NETWORK_ALERTS_DDL.format(net=net))
            conn.commit()
            logger.info("Network-specific tables (trades_testnet/mainnet, alerts_testnet/mainnet) verified/created")

        with conn.cursor() as cur:
            for net in ("testnet", "mainnet"):
                cur.execute(f"SELECT COUNT(*) FROM trades_{net}")
                trades_count = cur.fetchone()[0]
                cur.execute(f"SELECT COUNT(*) FROM alerts_{net}")
                alerts_count = cur.fetchone()[0]
                migrated_trades = 0
                migrated_alerts = 0
                if trades_count == 0:
                    cur.execute(f"""
                        INSERT INTO trades_{net}
                            (user_id, product_id, product_name, order_type, side, size, price,
                             leverage, status, order_digest, pnl, fees, error_message,
                             created_at, filled_at, close_price, closed_at)
                        SELECT user_id, product_id, product_name, order_type, side, size, price,
                               leverage, status, order_digest, pnl, fees, error_message,
                               created_at, filled_at, close_price, closed_at
                        FROM trades WHERE network = %s
                    """, (net,))
                    migrated_trades = cur.rowcount
                if alerts_count == 0:
                    cur.execute(f"""
                        INSERT INTO alerts_{net}
                            (user_id, product_id, product_name, condition, target_value,
                             is_active, triggered_at, created_at)
                        SELECT user_id, product_id, product_name, condition, target_value,
                               is_active, triggered_at, created_at
                        FROM alerts WHERE network = %s
                    """, (net,))
                    migrated_alerts = cur.rowcount
                if migrated_trades or migrated_alerts:
                    logger.info("Migrated %d trades, %d alerts to %s tables", migrated_trades, migrated_alerts, net)
            conn.commit()

        _COPY_TRADING_DDL = """
            CREATE TABLE IF NOT EXISTS copy_traders (
                id SERIAL PRIMARY KEY,
                wallet_address TEXT UNIQUE NOT NULL,
                label TEXT NOT NULL DEFAULT '',
                is_curated BOOLEAN DEFAULT false,
                active BOOLEAN DEFAULT true,
                created_at TIMESTAMPTZ DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS idx_copy_traders_active ON copy_traders (active);

            CREATE TABLE IF NOT EXISTS copy_mirrors (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                trader_id INT NOT NULL REFERENCES copy_traders(id),
                budget_usd DOUBLE PRECISION NOT NULL DEFAULT 100.0,
                risk_factor DOUBLE PRECISION NOT NULL DEFAULT 1.0,
                max_leverage DOUBLE PRECISION NOT NULL DEFAULT 10.0,
                active BOOLEAN DEFAULT true,
                last_synced_fill_tid BIGINT DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT now(),
                stopped_at TIMESTAMPTZ,
                UNIQUE(user_id, trader_id)
            );
            CREATE INDEX IF NOT EXISTS idx_copy_mirrors_user ON copy_mirrors (user_id, active);
            CREATE INDEX IF NOT EXISTS idx_copy_mirrors_trader ON copy_mirrors (trader_id, active);

            CREATE TABLE IF NOT EXISTS copy_trades (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                mirror_id INT NOT NULL REFERENCES copy_mirrors(id),
                hl_fill_tid BIGINT,
                hl_coin TEXT NOT NULL,
                nado_product_id INT NOT NULL,
                side TEXT NOT NULL,
                hl_size DOUBLE PRECISION NOT NULL,
                hl_price DOUBLE PRECISION NOT NULL,
                nado_size DOUBLE PRECISION,
                nado_price DOUBLE PRECISION,
                nado_trade_id INT,
                status TEXT NOT NULL DEFAULT 'pending',
                error_message TEXT,
                created_at TIMESTAMPTZ DEFAULT now(),
                filled_at TIMESTAMPTZ
            );
            CREATE INDEX IF NOT EXISTS idx_copy_trades_mirror ON copy_trades (mirror_id);
            CREATE INDEX IF NOT EXISTS idx_copy_trades_user ON copy_trades (user_id, created_at);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_copy_trades_dedup ON copy_trades (user_id, mirror_id, hl_fill_tid) WHERE hl_fill_tid IS NOT NULL;
        """
        with conn.cursor() as cur:
            cur.execute(_COPY_TRADING_DDL)
            conn.commit()
            logger.info("Copy trading tables verified/created")

        logger.info("Database tables verified/created")
    except Exception:
        conn.rollback()
        raise
    finally:
        put_db(conn)
