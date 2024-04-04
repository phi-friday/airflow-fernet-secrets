from __future__ import annotations

import re
from typing import Any

from typing_extensions import NamedTuple, Required, TypedDict

from airflow_fernet_secrets.core.config.const import (
    CONNECTION_DRIVER_FORMAT as _CONNECTION_DRIVER_FORMAT,
)
from airflow_fernet_secrets.core.config.const import (
    RE_CONNECTION_DRIVER_FORMAT as _RE_CONNECTION_DRIVER_FORMAT,
)

__all__ = ["ConnectionDict", "Driver", "create_driver", "parse_driver"]

_RE_DRIVER = re.compile(_RE_CONNECTION_DRIVER_FORMAT)


class ConnectionDict(TypedDict, total=False):
    driver: Required[str]
    host: str
    login: str
    password: str
    schema: str
    port: int
    extra: Required[dict[str, Any]]


class Driver(NamedTuple):
    backend: str
    dialect: str
    conn_type: str

    def __str__(self) -> str:
        return create_driver(
            backend=self.backend, dialect=self.dialect, conn_type=self.conn_type
        )


def create_driver(
    *,
    backend: str | None = None,
    dialect: str | None = None,
    conn_type: str | None = None,
) -> str:
    return _CONNECTION_DRIVER_FORMAT.format(
        backend=backend or "", dialect=dialect or "", conn_type=conn_type or ""
    )


def parse_driver(driver: str) -> Driver:
    match = _RE_DRIVER.match(driver)
    if match is None:
        raise NotImplementedError

    values = match.groupdict()
    if not any(x for x in values.values()):
        raise NotImplementedError
    return Driver(**values)
