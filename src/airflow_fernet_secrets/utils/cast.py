from __future__ import annotations

from typing import Any

__all__ = ["ensure_boolean"]

_TRUE = frozenset(("true", "t", "yes", "y", "1", b"true", b"t", b"yes", b"y", b"1"))


def ensure_boolean(value: Any) -> bool:
    """ensures that the input value is interpreted as a boolean.

    Args:
        value: The `value` parameter in the `ensure_boolean` function
        is the input value that needs to be checked
        and converted to a boolean value if possible.

    Returns:
        converted boolean value
    """
    if isinstance(value, bool):
        return value

    if isinstance(value, (str, bytes)):
        return value.lower() in _TRUE

    if isinstance(value, float):
        return value != 0

    return False
