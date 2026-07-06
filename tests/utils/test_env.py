import pytest

from src.nadobro.utils.env import env_bool, env_tristate


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("NADO_TEST_FLAG", raising=False)
    yield


def test_env_bool_unset_uses_default(monkeypatch):
    assert env_bool("NADO_TEST_FLAG", True) is True
    assert env_bool("NADO_TEST_FLAG", False) is False


def test_env_bool_blank_uses_default(monkeypatch):
    monkeypatch.setenv("NADO_TEST_FLAG", "   ")
    assert env_bool("NADO_TEST_FLAG", True) is True
    assert env_bool("NADO_TEST_FLAG", False) is False


@pytest.mark.parametrize("raw", ["1", "true", "TRUE", "Yes", "on", " on "])
def test_env_bool_truthy(monkeypatch, raw):
    monkeypatch.setenv("NADO_TEST_FLAG", raw)
    assert env_bool("NADO_TEST_FLAG", False) is True


@pytest.mark.parametrize("raw", ["0", "false", "no", "off", "banana"])
def test_env_bool_non_truthy(monkeypatch, raw):
    monkeypatch.setenv("NADO_TEST_FLAG", raw)
    assert env_bool("NADO_TEST_FLAG", True) is False


def test_env_tristate_unset_is_none():
    assert env_tristate("NADO_TEST_FLAG") is None


@pytest.mark.parametrize("raw", ["1", "true", "yes", "on"])
def test_env_tristate_truthy(monkeypatch, raw):
    monkeypatch.setenv("NADO_TEST_FLAG", raw)
    assert env_tristate("NADO_TEST_FLAG") is True


@pytest.mark.parametrize("raw", ["0", "false", "no", "off"])
def test_env_tristate_falsy(monkeypatch, raw):
    monkeypatch.setenv("NADO_TEST_FLAG", raw)
    assert env_tristate("NADO_TEST_FLAG") is False


@pytest.mark.parametrize("raw", ["", "   ", "banana", "maybe"])
def test_env_tristate_blank_or_unknown_is_none(monkeypatch, raw):
    monkeypatch.setenv("NADO_TEST_FLAG", raw)
    assert env_tristate("NADO_TEST_FLAG") is None
