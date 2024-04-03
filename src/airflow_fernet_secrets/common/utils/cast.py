from __future__ import annotations

from typing import Any

__all__ = ["ensure_boolean"]

_TRUE = frozenset(("true", "t", "yes", "y", "1", b"true", b"t", b"yes", b"y", b"1"))


def ensure_boolean(value: Any) -> bool:
    if isinstance(value, bool):
        return value

    if isinstance(value, (str, bytes)):
        return value.lower() in _TRUE

    if isinstance(value, float):
        return value != 0

    return False
