--
-- PostgreSQL database dump
--

\restrict 67xfveNhPVDWVrnffTeRINmosKVYcoX3RCZvrK2tCKX4V251c9tvXZxuT5vKle4

-- Dumped from database version 16.10
-- Dumped by pg_dump version 16.10

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: alertcondition; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE public.alertcondition AS ENUM (
    'ABOVE',
    'BELOW',
    'FUNDING_ABOVE',
    'FUNDING_BELOW',
    'PNL_ABOVE',
    'PNL_BELOW'
);


ALTER TYPE public.alertcondition OWNER TO postgres;

--
-- Name: networkmode; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE public.networkmode AS ENUM (
    'TESTNET',
    'MAINNET'
);


ALTER TYPE public.networkmode OWNER TO postgres;

--
-- Name: orderside; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE public.orderside AS ENUM (
    'LONG',
    'SHORT'
);


ALTER TYPE public.orderside OWNER TO postgres;

--
-- Name: ordertypeenum; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE public.ordertypeenum AS ENUM (
    'MARKET',
    'LIMIT',
    'TAKE_PROFIT',
    'STOP_LOSS'
);


ALTER TYPE public.ordertypeenum OWNER TO postgres;

--
-- Name: tradestatus; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE public.tradestatus AS ENUM (
    'PENDING',
    'FILLED',
    'PARTIALLY_FILLED',
    'CANCELLED',
    'FAILED'
);


ALTER TYPE public.tradestatus OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: admin_logs; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.admin_logs (
    id integer NOT NULL,
    admin_id bigint NOT NULL,
    action character varying(100) NOT NULL,
    details text,
    created_at timestamp without time zone NOT NULL
);


ALTER TABLE public.admin_logs OWNER TO postgres;

--
-- Name: admin_logs_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.admin_logs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.admin_logs_id_seq OWNER TO postgres;

--
-- Name: admin_logs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.admin_logs_id_seq OWNED BY public.admin_logs.id;


--
-- Name: alerts; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.alerts (
    id integer NOT NULL,
    user_id bigint NOT NULL,
    product_id integer NOT NULL,
    product_name character varying(50) NOT NULL,
    condition public.alertcondition NOT NULL,
    target_value double precision NOT NULL,
    is_active boolean NOT NULL,
    triggered_at timestamp without time zone,
    network public.networkmode NOT NULL,
    created_at timestamp without time zone NOT NULL
);


ALTER TABLE public.alerts OWNER TO postgres;

--
-- Name: alerts_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.alerts_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.alerts_id_seq OWNER TO postgres;

--
-- Name: alerts_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.alerts_id_seq OWNED BY public.alerts.id;


--
-- Name: bot_state; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bot_state (
    id integer NOT NULL,
    key character varying(100) NOT NULL,
    value text,
    updated_at timestamp without time zone
);


ALTER TABLE public.bot_state OWNER TO postgres;

--
-- Name: bot_state_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.bot_state_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.bot_state_id_seq OWNER TO postgres;

--
-- Name: bot_state_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.bot_state_id_seq OWNED BY public.bot_state.id;


--
-- Name: trades; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.trades (
    id integer NOT NULL,
    user_id bigint NOT NULL,
    product_id integer NOT NULL,
    product_name character varying(50) NOT NULL,
    order_type public.ordertypeenum NOT NULL,
    side public.orderside NOT NULL,
    size double precision NOT NULL,
    price double precision,
    leverage double precision,
    status public.tradestatus,
    order_digest character varying(128),
    pnl double precision,
    fees double precision,
    network public.networkmode NOT NULL,
    error_message text,
    created_at timestamp without time zone NOT NULL,
    filled_at timestamp without time zone
);


ALTER TABLE public.trades OWNER TO postgres;

--
-- Name: trades_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.trades_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.trades_id_seq OWNER TO postgres;

--
-- Name: trades_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.trades_id_seq OWNED BY public.trades.id;


--
-- Name: users; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.users (
    id integer NOT NULL,
    telegram_id bigint NOT NULL,
    telegram_username character varying(255),
    encrypted_private_key_testnet text,
    encrypted_private_key_mainnet text,
    wallet_address_testnet character varying(42),
    wallet_address_mainnet character varying(42),
    network_mode public.networkmode NOT NULL,
    is_active boolean NOT NULL,
    is_banned boolean NOT NULL,
    created_at timestamp without time zone NOT NULL,
    last_active timestamp without time zone,
    last_trade_at timestamp without time zone,
    total_trades integer,
    total_volume_usd double precision,
    mnemonic_hash_testnet character varying(128),
    mnemonic_hash_mainnet character varying(128)
);


ALTER TABLE public.users OWNER TO postgres;

--
-- Name: users_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.users_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.users_id_seq OWNER TO postgres;

--
-- Name: users_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.users_id_seq OWNED BY public.users.id;


--
-- Name: admin_logs id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.admin_logs ALTER COLUMN id SET DEFAULT nextval('public.admin_logs_id_seq'::regclass);


--
-- Name: alerts id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.alerts ALTER COLUMN id SET DEFAULT nextval('public.alerts_id_seq'::regclass);


--
-- Name: bot_state id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bot_state ALTER COLUMN id SET DEFAULT nextval('public.bot_state_id_seq'::regclass);


--
-- Name: trades id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.trades ALTER COLUMN id SET DEFAULT nextval('public.trades_id_seq'::regclass);


--
-- Name: users id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.users ALTER COLUMN id SET DEFAULT nextval('public.users_id_seq'::regclass);


--
-- Data for Name: admin_logs; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.admin_logs (id, admin_id, action, details, created_at) FROM stdin;
\.


--
-- Data for Name: alerts; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.alerts (id, user_id, product_id, product_name, condition, target_value, is_active, triggered_at, network, created_at) FROM stdin;
1	1124285818	2	BTC-PERP	ABOVE	68000	f	2026-02-12 01:27:50.03773	TESTNET	2026-02-11 21:58:06.448824
\.


--
-- Data for Name: bot_state; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.bot_state (id, key, value, updated_at) FROM stdin;
3	strategy_bot:1124285818:testnet	{"running": false, "strategy": "dn", "product": "BTC", "notional_usd": 100.0, "spread_bp": 0.5, "tp_pct": 10.0, "sl_pct": 1.0, "leverage": 5.0, "slippage_pct": 1.0, "interval_seconds": 10, "reference_price": 67915.0, "started_at": "2026-02-11T21:54:41.502503", "last_run_ts": 1770846926.367656, "last_error": null, "runs": 3}	2026-02-11 21:55:40.096511
2	user_settings:1124285818:testnet	{"default_leverage": 5, "slippage": 1.0, "risk_profile": "balanced", "strategies": {"mm": {"notional_usd": 75.0, "spread_bp": 4.0, "interval_seconds": 45, "tp_pct": 0.6, "sl_pct": 0.5}, "grid": {"notional_usd": 100.0, "spread_bp": 10.0, "interval_seconds": 60, "tp_pct": 1.2, "sl_pct": 0.8}, "dn": {"notional_usd": 100.0, "spread_bp": 0.5, "interval_seconds": 10, "tp_pct": 10.0, "sl_pct": 1.0}}}	2026-02-12 04:49:26.362361
1	onboarding:1124285818:testnet	{"current_step": "template", "completed_steps": ["welcome", "mode", "key", "funding", "risk", "template"], "skipped_steps": [], "selected_template": null, "onboarding_complete": true, "updated_at": "2026-02-11T21:44:41.516850"}	2026-02-11 21:44:41.517868
\.


--
-- Data for Name: trades; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.trades (id, user_id, product_id, product_name, order_type, side, size, price, leverage, status, order_digest, pnl, fees, network, error_message, created_at, filled_at) FROM stdin;
1	1124285818	2	BTC-PERP	MARKET	LONG	0.0001	\N	1	FAILED	\N	\N	0	TESTNET	{"reason": "ip_query_only", "blocked": true}	2026-02-09 21:36:21.969215	\N
2	1124285818	2	BTC-PERP	MARKET	LONG	0.0001	\N	1	FAILED	\N	\N	0	TESTNET	Order was blocked by the exchange. Your wallet may need funds deposited on-chain first.	2026-02-10 15:32:03.518715	\N
3	1124285818	2	BTC-PERP	MARKET	LONG	0.001	\N	10	FAILED	\N	\N	0	TESTNET	Order was blocked by the exchange. Your wallet may need funds deposited on-chain first.	2026-02-11 20:11:12.389711	\N
4	1124285818	2	BTC-PERP	MARKET	SHORT	0.01	\N	10	FAILED	\N	\N	0	TESTNET	Order was blocked by the exchange. Your wallet may need funds deposited on-chain first.	2026-02-11 21:45:11.457749	\N
5	1124285818	2	BTC-PERP	LIMIT	LONG	0.0014724287712581904	67901.417	5	FAILED	\N	\N	0	TESTNET	Order was blocked by the exchange. Your wallet may need funds deposited on-chain first.	2026-02-11 21:54:42.667901	\N
6	1124285818	2	BTC-PERP	LIMIT	SHORT	0.0014724287712581904	67928.583	5	FAILED	\N	\N	0	TESTNET	Order was blocked by the exchange. Your wallet may need funds deposited on-chain first.	2026-02-11 21:54:43.121812	\N
7	1124285818	2	BTC-PERP	LIMIT	LONG	0.0014724287712581904	67901.417	5	FAILED	\N	\N	0	TESTNET	Order was blocked by the exchange. Your wallet may need funds deposited on-chain first.	2026-02-11 21:55:04.205936	\N
8	1124285818	2	BTC-PERP	LIMIT	SHORT	0.0014724287712581904	67928.583	5	FAILED	\N	\N	0	TESTNET	Order was blocked by the exchange. Your wallet may need funds deposited on-chain first.	2026-02-11 21:55:04.673416	\N
9	1124285818	2	BTC-PERP	LIMIT	LONG	0.0014724287712581904	67901.417	5	FAILED	\N	\N	0	TESTNET	Order was blocked by the exchange. Your wallet may need funds deposited on-chain first.	2026-02-11 21:55:25.87545	\N
10	1124285818	2	BTC-PERP	LIMIT	SHORT	0.0014724287712581904	67928.583	5	FAILED	\N	\N	0	TESTNET	Order was blocked by the exchange. Your wallet may need funds deposited on-chain first.	2026-02-11 21:55:26.318953	\N
11	1124285818	2	BTC-PERP	MARKET	SHORT	0.01	\N	40	FAILED	\N	\N	0	TESTNET	Your wallet needs funds deposited on Nado DEX before trading. Please deposit USDT0 at https://testnet.nado.xyz/portfolio/faucet	2026-02-12 04:50:31.536156	\N
\.


--
-- Data for Name: users; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.users (id, telegram_id, telegram_username, encrypted_private_key_testnet, encrypted_private_key_mainnet, wallet_address_testnet, wallet_address_mainnet, network_mode, is_active, is_banned, created_at, last_active, last_trade_at, total_trades, total_volume_usd, mnemonic_hash_testnet, mnemonic_hash_mainnet) FROM stdin;
1	1124285818	Jay_Walker1	gAAAAABpikz5864BBFIm7laQvsAWFr10gQzTF48lsWJLX5-niiROk1YOHk6dPJHlh3CGIQUHVBE95GLK5LTQQb29cyFd_menIz_xwilvICb9omA_gsmydEn_QfabKBFESXpxk4BS7psh-MRDBj1aqw4bAi7uDrcKZrfskAsrMCAcpCV3ZVhDOdA=	gAAAAABpi02C9KhAzSiwpvUiUT9WK1-iHjuAiU_F3pqDovWDVuwU9Y8KuVpmrKNbqEL3V9sg6jEwQZ3p68eIA2TILwz1w7xMtgZrMUTGph_0Am6BUvC1N80Msr7mE6aGllptSUnTYgp2cJoZ_TBwN4YppJYIww_l-yiASa8RH1ivC1_smpzP4UA=	0x6115cAF237026B45B037191B20056d1e4AfAfFa3	0xCEe1D223fB4b9Fe305Fb3447525882293DDC1fE3	TESTNET	t	f	2026-02-09 21:09:13.502312	2026-02-12 04:48:12.03991	\N	0	0	f96d7655ccfddd021131548fdee94c84440713494f3c6b494a9c5ade483c7b41	0b4b284a084e009659fc492af53e02cfe97411eaf952f0f576610c68867b98a5
\.


--
-- Name: admin_logs_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.admin_logs_id_seq', 1, false);


--
-- Name: alerts_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.alerts_id_seq', 1, true);


--
-- Name: bot_state_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.bot_state_id_seq', 3, true);


--
-- Name: trades_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.trades_id_seq', 11, true);


--
-- Name: users_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.users_id_seq', 1, true);


--
-- Name: admin_logs admin_logs_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.admin_logs
    ADD CONSTRAINT admin_logs_pkey PRIMARY KEY (id);


--
-- Name: alerts alerts_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.alerts
    ADD CONSTRAINT alerts_pkey PRIMARY KEY (id);


--
-- Name: bot_state bot_state_key_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bot_state
    ADD CONSTRAINT bot_state_key_key UNIQUE (key);


--
-- Name: bot_state bot_state_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bot_state
    ADD CONSTRAINT bot_state_pkey PRIMARY KEY (id);


--
-- Name: trades trades_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.trades
    ADD CONSTRAINT trades_pkey PRIMARY KEY (id);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: idx_alerts_active; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_alerts_active ON public.alerts USING btree (user_id, is_active);


--
-- Name: idx_trades_created; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_trades_created ON public.trades USING btree (created_at);


--
-- Name: idx_trades_user_product; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_trades_user_product ON public.trades USING btree (user_id, product_id);


--
-- Name: idx_users_telegram_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_users_telegram_id ON public.users USING btree (telegram_id);


--
-- Name: ix_alerts_user_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_alerts_user_id ON public.alerts USING btree (user_id);


--
-- Name: ix_trades_user_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_trades_user_id ON public.trades USING btree (user_id);


--
-- Name: ix_users_telegram_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX ix_users_telegram_id ON public.users USING btree (telegram_id);


--
-- PostgreSQL database dump complete
--

\unrestrict 67xfveNhPVDWVrnffTeRINmosKVYcoX3RCZvrK2tCKX4V251c9tvXZxuT5vKle4

