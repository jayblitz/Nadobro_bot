import os
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, BigInteger, String, Float, Boolean, DateTime, Text, Index, Enum as SAEnum
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from contextlib import contextmanager
import enum

DATABASE_URL = os.environ.get("DATABASE_URL")
engine = create_engine(DATABASE_URL, pool_recycle=300, pool_pre_ping=True, pool_size=10, max_overflow=20)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


class NetworkMode(enum.Enum):
    TESTNET = "testnet"
    MAINNET = "mainnet"


class OrderSide(enum.Enum):
    LONG = "long"
    SHORT = "short"


class OrderTypeEnum(enum.Enum):
    MARKET = "market"
    LIMIT = "limit"
    TAKE_PROFIT = "take_profit"
    STOP_LOSS = "stop_loss"


class TradeStatus(enum.Enum):
    PENDING = "pending"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    FAILED = "failed"


class AlertCondition(enum.Enum):
    ABOVE = "above"
    BELOW = "below"
    FUNDING_ABOVE = "funding_above"
    FUNDING_BELOW = "funding_below"
    PNL_ABOVE = "pnl_above"
    PNL_BELOW = "pnl_below"


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False, index=True)
    telegram_username = Column(String(255), nullable=True)
    encrypted_private_key_testnet = Column(Text, nullable=True)
    encrypted_private_key_mainnet = Column(Text, nullable=True)
    wallet_address_testnet = Column(String(42), nullable=True)
    wallet_address_mainnet = Column(String(42), nullable=True)
    mnemonic_hash_testnet = Column(String(128), nullable=True)
    mnemonic_hash_mainnet = Column(String(128), nullable=True)
    network_mode = Column(SAEnum(NetworkMode), default=NetworkMode.TESTNET, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    is_banned = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_active = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_trade_at = Column(DateTime, nullable=True)
    total_trades = Column(Integer, default=0)
    total_volume_usd = Column(Float, default=0.0)

    __table_args__ = (
        Index("idx_users_telegram_id", "telegram_id"),
    )


class Trade(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False, index=True)
    product_id = Column(Integer, nullable=False)
    product_name = Column(String(50), nullable=False)
    order_type = Column(SAEnum(OrderTypeEnum), nullable=False)
    side = Column(SAEnum(OrderSide), nullable=False)
    size = Column(Float, nullable=False)
    price = Column(Float, nullable=True)
    leverage = Column(Float, default=1.0)
    status = Column(SAEnum(TradeStatus), default=TradeStatus.PENDING)
    order_digest = Column(String(128), nullable=True)
    pnl = Column(Float, nullable=True)
    fees = Column(Float, default=0.0)
    network = Column(SAEnum(NetworkMode), nullable=False)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    filled_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index("idx_trades_user_product", "user_id", "product_id"),
        Index("idx_trades_created", "created_at"),
    )


class Alert(Base):
    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False, index=True)
    product_id = Column(Integer, nullable=False)
    product_name = Column(String(50), nullable=False)
    condition = Column(SAEnum(AlertCondition), nullable=False)
    target_value = Column(Float, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    triggered_at = Column(DateTime, nullable=True)
    network = Column(SAEnum(NetworkMode), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("idx_alerts_active", "user_id", "is_active"),
    )


class AdminLog(Base):
    __tablename__ = "admin_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    admin_id = Column(BigInteger, nullable=False)
    action = Column(String(100), nullable=False)
    details = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class BotState(Base):
    __tablename__ = "bot_state"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(100), unique=True, nullable=False)
    value = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


def init_db():
    Base.metadata.create_all(bind=engine)


@contextmanager
def get_session() -> Session:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
