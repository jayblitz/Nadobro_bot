from decimal import Decimal

X18 = Decimal("1000000000000000000")


def from_x18(value: str | int) -> Decimal:
    """Decode a Nado X18 fixed-point integer into Decimal units."""
    if isinstance(value, bool):
        raise TypeError("X18 value must be a string or integer")
    return Decimal(int(value)) / X18


def to_x18(value: Decimal | str | int) -> int:
    """Encode a Decimal-compatible value into a Nado X18 integer."""
    return int(Decimal(str(value)) * X18)
