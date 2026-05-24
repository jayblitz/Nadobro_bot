from src.nadobro.handlers.vault_handler import _vault_home_card


def test_vault_home_card_renders_nado_parity_metrics():
    snapshot = {
        "ready": True,
        "usdt0_balance": 44.35,
        "lp_balance": 148.669149,
        "lp_value_usdt0": 156.42,
        "position_usdt0": 156.42,
        "all_time_earned_usdt0": -0.62,
        "unrealized_pnl_usdt0": 0.0,
        "deposit_room_usdt0": 0.0,
        "max_mintable_usdt0": 0.0,
        "lockup_seconds_remaining": 0,
        "pool": {"tvl_usdt0": 10_001_684.0, "apr_pct": 18.16, "apr_source": "snapshots"},
        "deposit_watch_enabled": False,
    }
    text, kb = _vault_home_card(snapshot)
    assert "Nado Liquidity Provider" in text
    assert "TVL:" in text
    assert "APR:" in text
    assert "Your Position" in text
    assert "All-time Earned" in text
    # Legacy ParseMode.MARKDOWN does not need hyphen escaping; the literal
    # backslash form would render as "\-" in Telegram.
    assert "All\\-time" not in text
    assert "Unrealized PnL" in text
    assert "Vault capacity is currently" in text
    assert "-$0.62" in text
    assert "+$0.00" in text
    assert "18.16%" in text
    callbacks = [btn.callback_data for row in kb.inline_keyboard for btn in row]
    assert "vault:watch:on" in callbacks
    # When mintable ≈ 0 the deposit button collapses to vault:home so users
    # cannot start a deposit they will be rejected from.
    assert "vault:deposit" not in callbacks


def test_vault_home_card_when_capacity_open_shows_deposit_button():
    snapshot = {
        "ready": True,
        "usdt0_balance": 1000.0,
        "lp_balance": 0.0,
        "lp_value_usdt0": 0.0,
        "position_usdt0": 0.0,
        "all_time_earned_usdt0": 0.0,
        "unrealized_pnl_usdt0": 0.0,
        "deposit_room_usdt0": 5000.0,
        "max_mintable_usdt0": 5000.0,
        "lockup_seconds_remaining": 0,
        "pool": {"tvl_usdt0": 10_000_000.0, "apr_pct": 12.5, "apr_source": "snapshots"},
        "deposit_watch_enabled": True,
    }
    text, kb = _vault_home_card(snapshot)
    callbacks = [btn.callback_data for row in kb.inline_keyboard for btn in row]
    assert "vault:deposit" in callbacks
    assert "vault:watch:off" in callbacks
    assert "Vault capacity is currently" not in text


def test_vault_home_card_error_path():
    snapshot = {"error": "Wallet not initialized. Use /start first."}
    text, kb = _vault_home_card(snapshot)
    assert "Wallet not initialized" in text
    callbacks = [btn.callback_data for row in kb.inline_keyboard for btn in row]
    assert "vault:refresh" in callbacks
    assert "nav:main" in callbacks
