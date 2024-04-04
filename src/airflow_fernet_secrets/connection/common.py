from __future__ import annotations

from typing import Any

from typing_extensions import TypedDict

__all__ = ["ConnectionDict"]


class ConnectionDict(TypedDict, total=False):
    driver: str
    host: str
    login: str
    password: str
    schema: str
    port: int
    extra: dict[str, Any]
