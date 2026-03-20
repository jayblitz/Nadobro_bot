import sys
import types


def install_test_stubs() -> None:
    # Telegram
    telegram_mod = sys.modules.get("telegram") or types.ModuleType("telegram")
    telegram_constants = sys.modules.get("telegram.constants") or types.ModuleType("telegram.constants")
    telegram_ext = sys.modules.get("telegram.ext") or types.ModuleType("telegram.ext")
    telegram_error = sys.modules.get("telegram.error") or types.ModuleType("telegram.error")

    class _ParseMode:
        MARKDOWN_V2 = "MARKDOWN_V2"
        MARKDOWN = "MARKDOWN"

    class _ChatAction:
        TYPING = "typing"

    class _CallbackContext:
        user_data = {}

    class _Update:
        pass

    class _InlineKeyboardButton:
        def __init__(self, text, callback_data=None):
            self.text = text
            self.callback_data = callback_data

    class _InlineKeyboardMarkup:
        def __init__(self, inline_keyboard):
            self.inline_keyboard = inline_keyboard

    class _ReplyKeyboardMarkup:
        def __init__(self, keyboard=None, **kwargs):
            self.keyboard = keyboard or []
            self.kwargs = kwargs

    class _KeyboardButton:
        def __init__(self, text):
            self.text = text

    class _ReplyKeyboardRemove:
        pass

    class _BotCommand:
        def __init__(self, command, description):
            self.command = command
            self.description = description

    class _BadRequest(Exception):
        pass

    telegram_constants.ParseMode = _ParseMode
    telegram_constants.ChatAction = _ChatAction
    telegram_ext.CallbackContext = _CallbackContext
    telegram_mod.Update = _Update
    telegram_mod.InlineKeyboardButton = _InlineKeyboardButton
    telegram_mod.InlineKeyboardMarkup = _InlineKeyboardMarkup
    telegram_mod.ReplyKeyboardMarkup = _ReplyKeyboardMarkup
    telegram_mod.KeyboardButton = _KeyboardButton
    telegram_mod.ReplyKeyboardRemove = _ReplyKeyboardRemove
    telegram_mod.BotCommand = _BotCommand
    telegram_error.BadRequest = _BadRequest

    sys.modules["telegram"] = telegram_mod
    sys.modules["telegram.constants"] = telegram_constants
    sys.modules["telegram.ext"] = telegram_ext
    sys.modules["telegram.error"] = telegram_error

    # psycopg2
    if "psycopg2" not in sys.modules:
        psycopg2_mod = types.ModuleType("psycopg2")
        psycopg2_pool = types.ModuleType("psycopg2.pool")
        psycopg2_extras = types.ModuleType("psycopg2.extras")
        psycopg2_sql = types.ModuleType("psycopg2.sql")

        class _ThreadedConnectionPool:
            def __init__(self, *args, **kwargs):
                pass

        class _RealDictCursor:
            pass

        class _SqlFragment:
            def __init__(self, value=""):
                self.value = value

            def format(self, *args, **kwargs):
                return self

            def join(self, _iterable):
                return self

            def __mul__(self, _):
                return self

        def _sql_factory(value=""):
            return _SqlFragment(value)

        def _identifier_factory(_value=""):
            return _SqlFragment("")

        def _placeholder_factory():
            return _SqlFragment("%s")

        psycopg2_pool.ThreadedConnectionPool = _ThreadedConnectionPool
        psycopg2_extras.RealDictCursor = _RealDictCursor
        psycopg2_sql.SQL = _sql_factory
        psycopg2_sql.Identifier = _identifier_factory
        psycopg2_sql.Placeholder = _placeholder_factory

        psycopg2_mod.pool = psycopg2_pool
        psycopg2_mod.extras = psycopg2_extras
        psycopg2_mod.sql = psycopg2_sql

        sys.modules["psycopg2"] = psycopg2_mod
        sys.modules["psycopg2.pool"] = psycopg2_pool
        sys.modules["psycopg2.extras"] = psycopg2_extras
        sys.modules["psycopg2.sql"] = psycopg2_sql

    # requests
    if "requests" not in sys.modules:
        requests_mod = types.ModuleType("requests")
        requests_adapters = types.ModuleType("requests.adapters")

        class _DummyResponse:
            def json(self):
                return {}

        class _DummySession:
            def mount(self, *args, **kwargs):
                return None

            def get(self, *args, **kwargs):
                return _DummyResponse()

        class _HTTPAdapter:
            def __init__(self, *args, **kwargs):
                pass

        requests_mod.Session = _DummySession
        requests_mod.RequestException = Exception
        requests_adapters.HTTPAdapter = _HTTPAdapter
        sys.modules["requests"] = requests_mod
        sys.modules["requests.adapters"] = requests_adapters

    # eth_account
    if "eth_account" not in sys.modules:
        eth_account_mod = types.ModuleType("eth_account")

        class _Account:
            @staticmethod
            def create():
                key = types.SimpleNamespace(hex=lambda: "0x0")
                return types.SimpleNamespace(key=key, address="0x" + "0" * 40)

        eth_account_mod.Account = _Account
        sys.modules["eth_account"] = eth_account_mod
