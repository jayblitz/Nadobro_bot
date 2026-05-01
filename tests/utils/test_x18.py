from decimal import Decimal

import pytest

from src.nadobro.utils.x18 import from_x18, to_x18


def test_from_x18_decodes_positive_and_negative_values():
    assert from_x18("1234500000000000000") == Decimal("1.2345")
    assert from_x18("-2500000000000000000") == Decimal("-2.5")


def test_to_x18_round_trips_decimal_values():
    value = Decimal("987654321.123456789123456789")
    assert from_x18(to_x18(value)) == value


def test_from_x18_handles_large_integer_strings():
    assert from_x18("123456789123456789123456789000000000000000000") == Decimal(
        "123456789123456789123456789"
    )


def test_bool_is_rejected():
    with pytest.raises(TypeError):
        from_x18(True)
