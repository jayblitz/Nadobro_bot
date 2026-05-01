import asyncio

from src.nadobro.studio.extractor import extract


def test_extractor_uses_bro_llm_json(monkeypatch):
    def fake_chat_json(messages, schema):
        return (
            {
                "action": "buy",
                "symbol": "ETH",
                "order_type": "market",
                "quantity": {"type": "contracts", "value": 0.1},
                "leverage": 10,
                "raw_input": "long ETH 0.1 at 10x",
            },
            "mock",
        )

    monkeypatch.setattr("src.nadobro.services.bro_llm.chat_json", fake_chat_json)
    intent = asyncio.run(extract("long ETH 0.1 at 10x", None, []))
    assert intent.symbol == "ETH"
    assert intent.quantity.value == 0.1
    assert intent.leverage == 10
