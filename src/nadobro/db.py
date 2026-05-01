import os
import logging
import psycopg2
import psycopg2.pool
import psycopg2.extras

logger = logging.getLogger(__name__)

_pool = None
_pool_pid = None
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
        logger.info("Resolved database hostname to IPv4")
        return url.replace(f"@{hostname}", f"@{ipv4}") + (f"&options=-csearch_path%3Dpublic" if "?" in url else f"?options=-csearch_path%3Dpublic")
    except Exception as e:
        logger.warning("IPv4 resolution failed for database hostname: %s — using hostname as-is", e)
        return url


def get_pool():
    global _pool, _pool_pid
    current_pid = os.getpid()
    if _pool is not None and _pool_pid is not None and _pool_pid != current_pid:
        try:
            _pool.closeall()
        except Exception:
            pass
        _pool = None
        _pool_pid = None

    if _pool is None:
        url = os.environ.get("SUPABASE_DATABASE_URL") or os.environ.get("DATABASE_URL")
        if not url:
            raise RuntimeError("Neither SUPABASE_DATABASE_URL nor DATABASE_URL environment variable is set.")
        db_label = "Supabase" if os.environ.get("SUPABASE_DATABASE_URL") else "default"
        url = _prepare_db_url(url)
        if db_label == "Supabase":
            url = _resolve_host_ipv4(url)
        _pool = psycopg2.pool.ThreadedConnectionPool(_DB_POOL_MIN, _DB_POOL_MAX, url)
        _pool_pid = current_pid
        logger.info("PostgreSQL connection pool initialized (%s) min=%s max=%s", db_label, _DB_POOL_MIN, _DB_POOL_MAX)
    return _pool


def get_db():
    return get_pool().getconn()


def put_db(conn):
    try:
        get_pool().putconn(conn)
    except Exception as e:
        logger.warning("Failed to return connection to pool: %s", e)
        try:
            conn.close()
        except Exception:
            pass


def query_one(sql, params=None):
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            result = dict(row) if row else None
        conn.commit()
        return result
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
            result = [dict(r) for r in rows]
        conn.commit()
        return result
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
            result = row[0] if row else 0
        conn.commit()
        return result
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
                    total_volume_usd DOUBLE PRECISION DEFAULT 0,
                    mainnet_volume_usd DOUBLE PRECISION DEFAULT 0,
                    testnet_volume_usd DOUBLE PRECISION DEFAULT 0
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
                CREATE TABLE IF NOT EXISTS invite_codes (
                    id BIGSERIAL PRIMARY KEY,
                    code_hash TEXT UNIQUE NOT NULL,
                    public_code TEXT,
                    code_type TEXT NOT NULL DEFAULT 'private_access',
                    code_prefix TEXT NOT NULL,
                    created_by BIGINT NOT NULL,
                    referrer_user_id BIGINT,
                    network TEXT,
                    created_for_telegram_id BIGINT,
                    note TEXT,
                    max_redemptions INT NOT NULL DEFAULT 1,
                    redemption_count INT NOT NULL DEFAULT 0,
                    active BOOLEAN NOT NULL DEFAULT true,
                    redeemed_by BIGINT,
                    redeemed_username TEXT,
                    redeemed_at TIMESTAMPTZ,
                    expires_at TIMESTAMPTZ,
                    revoked_at TIMESTAMPTZ,
                    revoked_by BIGINT,
                    earned_volume_threshold_usd DOUBLE PRECISION,
                    sequence_number INT,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now(),
                    CHECK (code_type IN ('private_access', 'referral')),
                    CHECK (max_redemptions > 0),
                    CHECK (redemption_count >= 0)
                );
                ALTER TABLE invite_codes ADD COLUMN IF NOT EXISTS active BOOLEAN NOT NULL DEFAULT true;
                ALTER TABLE invite_codes ADD COLUMN IF NOT EXISTS public_code TEXT;
                ALTER TABLE invite_codes ADD COLUMN IF NOT EXISTS code_type TEXT NOT NULL DEFAULT 'private_access';
                ALTER TABLE invite_codes ADD COLUMN IF NOT EXISTS referrer_user_id BIGINT;
                ALTER TABLE invite_codes ADD COLUMN IF NOT EXISTS network TEXT;
                ALTER TABLE invite_codes ADD COLUMN IF NOT EXISTS earned_volume_threshold_usd DOUBLE PRECISION;
                ALTER TABLE invite_codes ADD COLUMN IF NOT EXISTS sequence_number INT;
                CREATE INDEX IF NOT EXISTS idx_invite_codes_redeemed_by ON invite_codes (redeemed_by);
                CREATE INDEX IF NOT EXISTS idx_invite_codes_created_at ON invite_codes (created_at DESC);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_invite_codes_public_code ON invite_codes (public_code) WHERE public_code IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_invite_codes_referrer ON invite_codes (referrer_user_id, code_type, active);
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint WHERE conname = 'invite_codes_code_type_check'
                    ) THEN
                        ALTER TABLE invite_codes
                        ADD CONSTRAINT invite_codes_code_type_check
                        CHECK (code_type IN ('private_access', 'referral'));
                    END IF;
                END $$;
            """)
            conn.commit()

        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE users ADD COLUMN IF NOT EXISTS private_access_granted BOOLEAN DEFAULT false;
                ALTER TABLE users ADD COLUMN IF NOT EXISTS private_access_code_id BIGINT;
                ALTER TABLE users ADD COLUMN IF NOT EXISTS private_access_granted_at TIMESTAMPTZ;
                ALTER TABLE users ADD COLUMN IF NOT EXISTS private_access_granted_by BIGINT;
                ALTER TABLE users ADD COLUMN IF NOT EXISTS mainnet_volume_usd DOUBLE PRECISION DEFAULT 0;
                ALTER TABLE users ADD COLUMN IF NOT EXISTS testnet_volume_usd DOUBLE PRECISION DEFAULT 0;
                ALTER TABLE invite_codes ADD COLUMN IF NOT EXISTS active BOOLEAN NOT NULL DEFAULT true;
                ALTER TABLE invite_codes ADD COLUMN IF NOT EXISTS public_code TEXT;
                ALTER TABLE invite_codes ADD COLUMN IF NOT EXISTS code_type TEXT NOT NULL DEFAULT 'private_access';
                ALTER TABLE invite_codes ADD COLUMN IF NOT EXISTS referrer_user_id BIGINT;
                ALTER TABLE invite_codes ADD COLUMN IF NOT EXISTS network TEXT;
                ALTER TABLE invite_codes ADD COLUMN IF NOT EXISTS earned_volume_threshold_usd DOUBLE PRECISION;
                ALTER TABLE invite_codes ADD COLUMN IF NOT EXISTS sequence_number INT;
                CREATE INDEX IF NOT EXISTS idx_users_private_access ON users (private_access_granted);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_invite_codes_public_code ON invite_codes (public_code) WHERE public_code IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_invite_codes_referrer ON invite_codes (referrer_user_id, code_type, active);
                CREATE INDEX IF NOT EXISTS idx_invite_codes_referrer_network ON invite_codes (referrer_user_id, network, code_type, active);
            """)
            conn.commit()

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS referrals (
                    id BIGSERIAL PRIMARY KEY,
                    referrer_user_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
                    referred_user_id BIGINT NOT NULL UNIQUE REFERENCES users(telegram_id) ON DELETE CASCADE,
                    invite_code_id BIGINT REFERENCES invite_codes(id),
                    referred_username TEXT,
                    referred_volume_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
                    referred_trade_count INT NOT NULL DEFAULT 0,
                    first_trade_at TIMESTAMPTZ,
                    last_trade_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now(),
                    CHECK (referrer_user_id <> referred_user_id),
                    CHECK (referred_volume_usd >= 0),
                    CHECK (referred_trade_count >= 0)
                );
                CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals (referrer_user_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_referrals_referred ON referrals (referred_user_id);

                CREATE TABLE IF NOT EXISTS referral_volume_events (
                    id BIGSERIAL PRIMARY KEY,
                    referral_id BIGINT NOT NULL REFERENCES referrals(id) ON DELETE CASCADE,
                    referrer_user_id BIGINT NOT NULL,
                    referred_user_id BIGINT NOT NULL,
                    network TEXT NOT NULL DEFAULT 'mainnet',
                    volume_usd DOUBLE PRECISION NOT NULL,
                    trade_count_delta INT NOT NULL DEFAULT 1,
                    source TEXT NOT NULL DEFAULT 'trade_stats',
                    created_at TIMESTAMPTZ DEFAULT now(),
                    CHECK (volume_usd >= 0)
                );
                ALTER TABLE referral_volume_events ADD COLUMN IF NOT EXISTS network TEXT NOT NULL DEFAULT 'mainnet';
                ALTER TABLE referral_volume_events ADD COLUMN IF NOT EXISTS trade_count_delta INT NOT NULL DEFAULT 1;
                CREATE INDEX IF NOT EXISTS idx_referral_volume_events_referrer ON referral_volume_events (referrer_user_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_referral_volume_events_referred ON referral_volume_events (referred_user_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_referral_volume_events_referrer_network ON referral_volume_events (referrer_user_id, network, created_at DESC);
            """)
            conn.commit()

        with conn.cursor() as cur:
            for col, col_type in [("close_price", "DOUBLE PRECISION"), ("closed_at", "TIMESTAMPTZ")]:
                try:
                    cur.execute(f"ALTER TABLE trades ADD COLUMN {col} {col_type}")  # Safe: col and col_type are hardcoded constants above
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
                network TEXT NOT NULL DEFAULT 'mainnet',
                margin_per_trade DOUBLE PRECISION NOT NULL DEFAULT 50.0,
                max_leverage DOUBLE PRECISION NOT NULL DEFAULT 10.0,
                cumulative_stop_loss_pct DOUBLE PRECISION NOT NULL DEFAULT 50.0,
                cumulative_take_profit_pct DOUBLE PRECISION NOT NULL DEFAULT 100.0,
                total_allocated_usd DOUBLE PRECISION NOT NULL DEFAULT 500.0,
                cumulative_pnl DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                active BOOLEAN DEFAULT true,
                paused BOOLEAN DEFAULT false,
                auto_stopped_reason TEXT,
                created_at TIMESTAMPTZ DEFAULT now(),
                stopped_at TIMESTAMPTZ,
                UNIQUE(user_id, trader_id, network)
            );
            CREATE INDEX IF NOT EXISTS idx_copy_mirrors_user ON copy_mirrors (user_id, active);
            CREATE INDEX IF NOT EXISTS idx_copy_mirrors_trader ON copy_mirrors (trader_id, active);

            CREATE TABLE IF NOT EXISTS copy_positions (
                id SERIAL PRIMARY KEY,
                mirror_id INT NOT NULL REFERENCES copy_mirrors(id),
                user_id BIGINT NOT NULL,
                product_id INT NOT NULL,
                product_name TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price DOUBLE PRECISION,
                size DOUBLE PRECISION,
                leverage DOUBLE PRECISION DEFAULT 1.0,
                tp_price DOUBLE PRECISION,
                sl_price DOUBLE PRECISION,
                leader_entry_price DOUBLE PRECISION,
                leader_size DOUBLE PRECISION,
                status TEXT NOT NULL DEFAULT 'open',
                pnl DOUBLE PRECISION DEFAULT 0.0,
                opened_at TIMESTAMPTZ DEFAULT now(),
                closed_at TIMESTAMPTZ,
                close_reason TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_copy_positions_mirror ON copy_positions (mirror_id, status);
            CREATE INDEX IF NOT EXISTS idx_copy_positions_user ON copy_positions (user_id, status);

            CREATE TABLE IF NOT EXISTS copy_snapshots (
                id SERIAL PRIMARY KEY,
                trader_id INT NOT NULL REFERENCES copy_traders(id),
                network TEXT NOT NULL DEFAULT 'mainnet',
                positions_json JSONB NOT NULL DEFAULT '[]',
                captured_at TIMESTAMPTZ DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS idx_copy_snapshots_trader ON copy_snapshots (trader_id, network);

            CREATE TABLE IF NOT EXISTS copy_trades (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                mirror_id INT NOT NULL REFERENCES copy_mirrors(id),
                hl_fill_tid BIGINT,
                hl_coin TEXT NOT NULL DEFAULT '',
                nado_product_id INT NOT NULL DEFAULT 0,
                side TEXT NOT NULL,
                hl_size DOUBLE PRECISION NOT NULL DEFAULT 0,
                hl_price DOUBLE PRECISION NOT NULL DEFAULT 0,
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
        """
        with conn.cursor() as cur:
            cur.execute(_COPY_TRADING_DDL)
            conn.commit()
            logger.info("Copy trading tables verified/created")

        # --- New columns on trades_testnet / trades_mainnet ---
        _NEW_TRADE_COLS = [
            ("fill_price", "DOUBLE PRECISION"),
            ("fill_size", "DOUBLE PRECISION"),
            ("fill_fee", "DOUBLE PRECISION DEFAULT 0"),
            ("builder_fee", "DOUBLE PRECISION DEFAULT 0"),
            ("slippage_bps", "DOUBLE PRECISION"),
            ("source", "TEXT DEFAULT 'manual'"),
            ("strategy_session_id", "INT"),
            ("open_trade_id", "INT"),
            ("realized_pnl", "DOUBLE PRECISION"),
            ("is_taker", "BOOLEAN"),
            ("funding_paid", "DOUBLE PRECISION DEFAULT 0"),
        ]
        with conn.cursor() as cur:
            for net in ("testnet", "mainnet"):
                table = f"trades_{net}"
                for col, col_type in _NEW_TRADE_COLS:
                    try:
                        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
                        conn.commit()
                        logger.info("Added column %s.%s", table, col)
                    except Exception:
                        conn.rollback()
            # Also add to the legacy trades table for backwards compat
            for col, col_type in _NEW_TRADE_COLS:
                try:
                    cur.execute(f"ALTER TABLE trades ADD COLUMN {col} {col_type}")
                    conn.commit()
                except Exception:
                    conn.rollback()
            try:
                cur.execute("""
                    UPDATE users u
                    SET mainnet_volume_usd = COALESCE(v.total, 0)
                    FROM (
                        SELECT user_id, COALESCE(SUM(ABS(size) * COALESCE(NULLIF(price, 0), fill_price, 0)), 0) AS total
                        FROM trades_mainnet
                        WHERE status IN ('filled', 'closed')
                        GROUP BY user_id
                    ) v
                    WHERE u.telegram_id = v.user_id;
                """)
                cur.execute("""
                    UPDATE users u
                    SET testnet_volume_usd = COALESCE(v.total, 0)
                    FROM (
                        SELECT user_id, COALESCE(SUM(ABS(size) * COALESCE(NULLIF(price, 0), fill_price, 0)), 0) AS total
                        FROM trades_testnet
                        WHERE status IN ('filled', 'closed')
                        GROUP BY user_id
                    ) v
                    WHERE u.telegram_id = v.user_id;
                """)
                conn.commit()
            except Exception:
                conn.rollback()
                logger.warning("Failed to backfill per-network user volume counters", exc_info=True)

        # --- strategy_sessions table ---
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS strategy_sessions (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    strategy TEXT NOT NULL,
                    product_id INT,
                    product_name TEXT,
                    network TEXT NOT NULL,
                    started_at TIMESTAMPTZ DEFAULT now(),
                    stopped_at TIMESTAMPTZ,
                    status TEXT DEFAULT 'running',
                    total_cycles INT DEFAULT 0,
                    total_orders_placed INT DEFAULT 0,
                    total_orders_filled INT DEFAULT 0,
                    total_orders_cancelled INT DEFAULT 0,
                    realized_pnl DOUBLE PRECISION DEFAULT 0,
                    total_fees_paid DOUBLE PRECISION DEFAULT 0,
                    total_volume_usd DOUBLE PRECISION DEFAULT 0,
                    total_funding_paid DOUBLE PRECISION DEFAULT 0,
                    config_snapshot JSONB,
                    stop_reason TEXT,
                    error_message TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_strategy_sessions_user
                    ON strategy_sessions (user_id, network, strategy);
                CREATE INDEX IF NOT EXISTS idx_strategy_sessions_status
                    ON strategy_sessions (status);
            """)
            conn.commit()
            logger.info("strategy_sessions table verified/created")

        # --- fill_sync_queue table ---
        # --- Product database design tables: strategy configs, positions, orders, points, and analytics ---
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS strategies (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
                    strategy_type TEXT NOT NULL CHECK (
                        strategy_type IN ('grid', 'r_grid', 'd_grid', 'delta_neutral', 'volume_bot', 'bro_mode')
                    ),
                    name TEXT,
                    network TEXT NOT NULL DEFAULT 'mainnet' CHECK (network IN ('testnet', 'mainnet')),
                    pair TEXT NOT NULL,
                    capital_usd NUMERIC(20, 8),
                    leverage NUMERIC(10, 4) NOT NULL DEFAULT 1,
                    risk_level TEXT NOT NULL DEFAULT 'medium',
                    parameters JSONB NOT NULL DEFAULT '{}',
                    status TEXT NOT NULL DEFAULT 'stopped' CHECK (status IN ('running', 'paused', 'stopped', 'failed')),
                    started_at TIMESTAMPTZ,
                    stopped_at TIMESTAMPTZ,
                    last_run_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS idx_strategies_user_status ON strategies (user_id, status);
                CREATE INDEX IF NOT EXISTS idx_strategies_user_type_network_status
                    ON strategies (user_id, strategy_type, network, status);
                CREATE INDEX IF NOT EXISTS idx_strategies_type_pair ON strategies (strategy_type, pair);

                CREATE TABLE IF NOT EXISTS strategy_performance_snapshots (
                    id BIGSERIAL PRIMARY KEY,
                    strategy_id BIGINT NOT NULL REFERENCES strategies(id) ON DELETE CASCADE,
                    user_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
                    period TEXT NOT NULL CHECK (period IN ('daily', 'weekly', 'monthly')),
                    period_start DATE NOT NULL,
                    pnl_usd NUMERIC(20, 8) NOT NULL DEFAULT 0,
                    fees_usd NUMERIC(20, 8) NOT NULL DEFAULT 0,
                    volume_usd NUMERIC(20, 8) NOT NULL DEFAULT 0,
                    trade_count INT NOT NULL DEFAULT 0,
                    win_rate NUMERIC(8, 4),
                    metadata JSONB NOT NULL DEFAULT '{}',
                    created_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE (strategy_id, period, period_start)
                );
                CREATE INDEX IF NOT EXISTS idx_strategy_perf_user_period
                    ON strategy_performance_snapshots (user_id, period, period_start DESC);

                CREATE TABLE IF NOT EXISTS positions (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
                    strategy_id BIGINT REFERENCES strategies(id) ON DELETE SET NULL,
                    network TEXT NOT NULL CHECK (network IN ('testnet', 'mainnet')),
                    pair TEXT NOT NULL,
                    side TEXT NOT NULL CHECK (side IN ('long', 'short')),
                    size NUMERIC(30, 12) NOT NULL,
                    entry_price NUMERIC(30, 12),
                    mark_price NUMERIC(30, 12),
                    leverage NUMERIC(10, 4) NOT NULL DEFAULT 1,
                    unrealized_pnl_usd NUMERIC(20, 8) NOT NULL DEFAULT 0,
                    realized_pnl_usd NUMERIC(20, 8) NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'closed')),
                    opened_at TIMESTAMPTZ DEFAULT now(),
                    closed_at TIMESTAMPTZ,
                    synced_at TIMESTAMPTZ,
                    metadata JSONB NOT NULL DEFAULT '{}'
                );
                ALTER TABLE positions ADD COLUMN IF NOT EXISTS isolated BOOLEAN NOT NULL DEFAULT false;
                ALTER TABLE positions ADD COLUMN IF NOT EXISTS product_id INTEGER;
                ALTER TABLE positions ADD COLUMN IF NOT EXISTS amount_x18 NUMERIC(78,0);
                ALTER TABLE positions ADD COLUMN IF NOT EXISTS v_quote_balance_x18 NUMERIC(78,0);
                ALTER TABLE positions ADD COLUMN IF NOT EXISTS last_cum_funding_x18 NUMERIC(78,0);
                ALTER TABLE positions ADD COLUMN IF NOT EXISTS est_pnl NUMERIC(38,18);
                ALTER TABLE positions ADD COLUMN IF NOT EXISTS est_liq_price NUMERIC(38,18);
                ALTER TABLE positions ADD COLUMN IF NOT EXISTS avg_entry_price NUMERIC(38,18);
                ALTER TABLE positions ADD COLUMN IF NOT EXISTS notional_value NUMERIC(38,18);
                ALTER TABLE positions ADD COLUMN IF NOT EXISTS margin_used NUMERIC(38,18);
                ALTER TABLE positions ADD COLUMN IF NOT EXISTS strategy_session_id BIGINT;
                ALTER TABLE positions ADD COLUMN IF NOT EXISTS close_price NUMERIC(38,18);
                ALTER TABLE positions ADD COLUMN IF NOT EXISTS close_realized_pnl NUMERIC(38,18);
                ALTER TABLE positions ADD COLUMN IF NOT EXISTS synced_at TIMESTAMPTZ;
                ALTER TABLE positions ADD COLUMN IF NOT EXISTS time_limit TIMESTAMPTZ NULL;
                ALTER TABLE positions ADD COLUMN IF NOT EXISTS time_limit_source TEXT NULL;
                ALTER TABLE positions ADD COLUMN IF NOT EXISTS time_limit_fired_at TIMESTAMPTZ NULL;
                CREATE UNIQUE INDEX IF NOT EXISTS positions_unique_open
                    ON positions (user_id, network, COALESCE(product_id, 0), isolated)
                    WHERE closed_at IS NULL;
                CREATE INDEX IF NOT EXISTS positions_user_network_open
                    ON positions (user_id, network)
                    WHERE closed_at IS NULL;
                CREATE INDEX IF NOT EXISTS idx_positions_user_status ON positions (user_id, status);
                CREATE INDEX IF NOT EXISTS idx_positions_pair_status ON positions (pair, status);
                CREATE INDEX IF NOT EXISTS idx_positions_time_limit_due
                    ON positions (network, time_limit)
                    WHERE time_limit IS NOT NULL AND time_limit_fired_at IS NULL AND status = 'open';

                CREATE TABLE IF NOT EXISTS open_orders (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
                    strategy_id BIGINT REFERENCES strategies(id) ON DELETE SET NULL,
                    network TEXT NOT NULL CHECK (network IN ('testnet', 'mainnet')),
                    pair TEXT NOT NULL,
                    side TEXT NOT NULL,
                    order_type TEXT NOT NULL DEFAULT 'limit',
                    size NUMERIC(30, 12) NOT NULL,
                    price NUMERIC(30, 12),
                    leverage NUMERIC(10, 4) DEFAULT 1,
                    order_digest TEXT UNIQUE,
                    status TEXT NOT NULL DEFAULT 'open',
                    placed_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now(),
                    synced_at TIMESTAMPTZ,
                    metadata JSONB NOT NULL DEFAULT '{}'
                );
                ALTER TABLE open_orders ADD COLUMN IF NOT EXISTS isolated BOOLEAN NOT NULL DEFAULT false;
                ALTER TABLE open_orders ADD COLUMN IF NOT EXISTS product_id INTEGER;
                ALTER TABLE open_orders ADD COLUMN IF NOT EXISTS price_x18 NUMERIC(78,0);
                ALTER TABLE open_orders ADD COLUMN IF NOT EXISTS amount_x18 NUMERIC(78,0);
                ALTER TABLE open_orders ADD COLUMN IF NOT EXISTS expiration BIGINT;
                ALTER TABLE open_orders ADD COLUMN IF NOT EXISTS nonce BIGINT;
                ALTER TABLE open_orders ADD COLUMN IF NOT EXISTS strategy_session_id BIGINT;
                ALTER TABLE open_orders ADD COLUMN IF NOT EXISTS synced_at TIMESTAMPTZ;
                ALTER TABLE open_orders ADD COLUMN IF NOT EXISTS time_limit TIMESTAMPTZ NULL;
                ALTER TABLE open_orders ADD COLUMN IF NOT EXISTS time_limit_source TEXT NULL;
                ALTER TABLE open_orders ADD COLUMN IF NOT EXISTS time_limit_fired_at TIMESTAMPTZ NULL;
                CREATE UNIQUE INDEX IF NOT EXISTS open_orders_unique_digest
                    ON open_orders (user_id, network, order_digest);
                CREATE INDEX IF NOT EXISTS idx_open_orders_user_status ON open_orders (user_id, status);
                CREATE INDEX IF NOT EXISTS idx_open_orders_pair_status ON open_orders (pair, status);
                CREATE INDEX IF NOT EXISTS idx_open_orders_time_limit_due
                    ON open_orders (network, time_limit)
                    WHERE time_limit IS NOT NULL AND time_limit_fired_at IS NULL AND status IN ('open', 'pending', 'armed');

                CREATE TABLE IF NOT EXISTS studio_sessions (
                    id BIGSERIAL PRIMARY KEY,
                    telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
                    network TEXT NOT NULL CHECK (network IN ('testnet', 'mainnet')),
                    state TEXT NOT NULL CHECK (state IN ('EXTRACTING', 'CLARIFYING', 'CONFIRMING', 'EXECUTING', 'DONE', 'CANCELLED')),
                    intent_json JSONB NOT NULL DEFAULT '{}',
                    history_json JSONB NOT NULL DEFAULT '[]',
                    strategy_session_id BIGINT REFERENCES strategy_sessions(id) ON DELETE SET NULL,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS idx_studio_sessions_user_active
                    ON studio_sessions (telegram_id, network, updated_at DESC)
                    WHERE state IN ('EXTRACTING', 'CLARIFYING', 'CONFIRMING', 'EXECUTING');
                CREATE INDEX IF NOT EXISTS idx_studio_sessions_strategy_session
                    ON studio_sessions (strategy_session_id)
                    WHERE strategy_session_id IS NOT NULL;

                CREATE TABLE IF NOT EXISTS conditional_orders (
                    id BIGSERIAL PRIMARY KEY,
                    telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
                    network TEXT NOT NULL CHECK (network IN ('testnet', 'mainnet')),
                    studio_session_id BIGINT REFERENCES studio_sessions(id) ON DELETE SET NULL,
                    strategy_session_id BIGINT REFERENCES strategy_sessions(id) ON DELETE SET NULL,
                    symbol TEXT NOT NULL,
                    action TEXT NOT NULL,
                    order_type TEXT NOT NULL DEFAULT 'conditional',
                    intent_json JSONB NOT NULL DEFAULT '{}',
                    conditions_json JSONB NOT NULL DEFAULT '[]',
                    status TEXT NOT NULL DEFAULT 'armed',
                    time_limit TIMESTAMPTZ NULL,
                    time_limit_source TEXT NULL,
                    time_limit_fired_at TIMESTAMPTZ NULL,
                    fired_at TIMESTAMPTZ NULL,
                    last_evaluated_at TIMESTAMPTZ NULL,
                    last_evaluation TEXT NULL,
                    error_message TEXT NULL,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS idx_conditional_orders_armed
                    ON conditional_orders (network, status, updated_at)
                    WHERE status = 'armed';
                CREATE INDEX IF NOT EXISTS idx_conditional_orders_time_limit_due
                    ON conditional_orders (network, time_limit)
                    WHERE time_limit IS NOT NULL AND time_limit_fired_at IS NULL AND status = 'armed';
                CREATE INDEX IF NOT EXISTS idx_conditional_orders_user_status
                    ON conditional_orders (telegram_id, network, status);

                CREATE TABLE IF NOT EXISTS points_snapshots (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
                    period TEXT NOT NULL CHECK (period IN ('daily', 'weekly', 'monthly', 'all')),
                    period_start DATE NOT NULL,
                    nado_points NUMERIC(20, 8) NOT NULL DEFAULT 0,
                    volume_usd NUMERIC(20, 8) NOT NULL DEFAULT 0,
                    cost_per_point_usd NUMERIC(20, 8),
                    maker_ratio NUMERIC(8, 4),
                    taker_ratio NUMERIC(8, 4),
                    metadata JSONB NOT NULL DEFAULT '{}',
                    created_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE (user_id, period, period_start)
                );
                CREATE INDEX IF NOT EXISTS idx_points_snapshots_user_period
                    ON points_snapshots (user_id, period, period_start DESC);

                CREATE TABLE IF NOT EXISTS funding_payments_mainnet (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    product_id INTEGER NOT NULL,
                    amount_x18 NUMERIC(78,0) NOT NULL,
                    balance_amount_x18 NUMERIC(78,0),
                    rate_x18 NUMERIC(78,0),
                    oracle_price_x18 NUMERIC(78,0),
                    paid_at TIMESTAMPTZ NOT NULL,
                    synced_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE (user_id, product_id, paid_at, amount_x18)
                );
                CREATE TABLE IF NOT EXISTS funding_payments_testnet (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    product_id INTEGER NOT NULL,
                    amount_x18 NUMERIC(78,0) NOT NULL,
                    balance_amount_x18 NUMERIC(78,0),
                    rate_x18 NUMERIC(78,0),
                    oracle_price_x18 NUMERIC(78,0),
                    paid_at TIMESTAMPTZ NOT NULL,
                    synced_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE (user_id, product_id, paid_at, amount_x18)
                );
                CREATE UNIQUE INDEX IF NOT EXISTS funding_payments_mainnet_event_unique
                    ON funding_payments_mainnet (user_id, product_id, paid_at, amount_x18);
                CREATE UNIQUE INDEX IF NOT EXISTS funding_payments_testnet_event_unique
                    ON funding_payments_testnet (user_id, product_id, paid_at, amount_x18);
                CREATE TABLE IF NOT EXISTS sync_cursors (
                    user_id BIGINT,
                    network TEXT,
                    matches_idx NUMERIC(78,0),
                    funding_idx NUMERIC(78,0),
                    ws_last_event_at TIMESTAMPTZ,
                    PRIMARY KEY (user_id, network)
                );
                CREATE TABLE IF NOT EXISTS sync_log (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT,
                    network TEXT,
                    ran_at TIMESTAMPTZ DEFAULT now(),
                    positions_seen INTEGER DEFAULT 0,
                    positions_closed INTEGER DEFAULT 0,
                    orders_seen INTEGER DEFAULT 0,
                    orders_cleared INTEGER DEFAULT 0,
                    fills_inserted INTEGER DEFAULT 0,
                    funding_inserted INTEGER DEFAULT 0,
                    duration_ms INTEGER,
                    error TEXT
                );
            """)
            conn.commit()
            logger.info("Strategy, position, order, and points analytics tables verified/created")

        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE users ADD COLUMN IF NOT EXISTS onboarding_status TEXT DEFAULT 'not_started';
                ALTER TABLE users ADD COLUMN IF NOT EXISTS has_strategy_bot BOOLEAN DEFAULT false;

                ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS scope TEXT DEFAULT 'global';
                ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS user_id BIGINT;
                ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}';
                CREATE INDEX IF NOT EXISTS idx_bot_state_scope_key ON bot_state (scope, key);
                CREATE INDEX IF NOT EXISTS idx_bot_state_user_scope ON bot_state (user_id, scope);

                ALTER TABLE copy_traders ADD COLUMN IF NOT EXISTS total_pnl_usd NUMERIC(20, 8) DEFAULT 0;
                ALTER TABLE copy_traders ADD COLUMN IF NOT EXISTS total_volume_usd NUMERIC(20, 8) DEFAULT 0;
                ALTER TABLE copy_traders ADD COLUMN IF NOT EXISTS nado_points NUMERIC(20, 8) DEFAULT 0;
                ALTER TABLE copy_traders ADD COLUMN IF NOT EXISTS win_rate NUMERIC(8, 4);
                ALTER TABLE copy_traders ADD COLUMN IF NOT EXISTS last_updated_at TIMESTAMPTZ;
                CREATE INDEX IF NOT EXISTS idx_copy_traders_leaderboard
                    ON copy_traders (active, total_pnl_usd DESC, total_volume_usd DESC);

                ALTER TABLE copy_mirrors ADD COLUMN IF NOT EXISTS budget_cap_usd NUMERIC(20, 8);
                ALTER TABLE copy_mirrors ADD COLUMN IF NOT EXISTS risk_multiplier NUMERIC(10, 4) DEFAULT 1;
                ALTER TABLE copy_mirrors ADD COLUMN IF NOT EXISTS last_synced_at TIMESTAMPTZ;
                ALTER TABLE copy_mirrors ADD COLUMN IF NOT EXISTS budget_usd DOUBLE PRECISION;
                ALTER TABLE copy_mirrors ADD COLUMN IF NOT EXISTS risk_factor DOUBLE PRECISION DEFAULT 1.0;
                ALTER TABLE copy_mirrors ADD COLUMN IF NOT EXISTS last_synced_fill_tid BIGINT;

                ALTER TABLE copy_trades ADD COLUMN IF NOT EXISTS original_trade_digest TEXT;
                ALTER TABLE copy_trades ADD COLUMN IF NOT EXISTS copied_order_digest TEXT;
                ALTER TABLE copy_trades ADD COLUMN IF NOT EXISTS pair TEXT;
                ALTER TABLE copy_trades ADD COLUMN IF NOT EXISTS pnl_usd NUMERIC(20, 8);
                ALTER TABLE copy_trades ADD COLUMN IF NOT EXISTS fees_usd NUMERIC(20, 8) DEFAULT 0;
                CREATE INDEX IF NOT EXISTS idx_copy_trades_original_digest ON copy_trades (original_trade_digest);
            """)
            conn.commit()
            logger.info("Product database design columns verified/created")

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fill_sync_queue (
                    id SERIAL PRIMARY KEY,
                    trade_id INT NOT NULL,
                    network TEXT NOT NULL,
                    user_id BIGINT NOT NULL,
                    subaccount_hex TEXT NOT NULL,
                    order_digest TEXT NOT NULL,
                    product_id INT NOT NULL,
                    placed_at_ts DOUBLE PRECISION,
                    status TEXT DEFAULT 'pending',
                    attempts INT DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    resolved_at TIMESTAMPTZ
                );
                CREATE INDEX IF NOT EXISTS idx_fill_sync_pending
                    ON fill_sync_queue (status) WHERE status = 'pending';
            """)
            conn.commit()
            logger.info("fill_sync_queue table verified/created")

        # --- Additional indexes and constraints ---
        with conn.cursor() as cur:
            # Unique constraint on fill_sync_queue to prevent duplicate entries
            try:
                cur.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_fill_sync_trade_network
                        ON fill_sync_queue (trade_id, network)
                """)
                conn.commit()
            except Exception:
                conn.rollback()
            # Index on strategy_session_id for fast strategy-trade lookups
            for net in ("testnet", "mainnet"):
                try:
                    cur.execute(f"""
                        CREATE INDEX IF NOT EXISTS idx_trades_{net}_session
                            ON trades_{net} (strategy_session_id)
                            WHERE strategy_session_id IS NOT NULL
                    """)
                    conn.commit()
                except Exception:
                    conn.rollback()

        logger.info("Database tables verified/created")
    except Exception:
        conn.rollback()
        raise
    finally:
        put_db(conn)
