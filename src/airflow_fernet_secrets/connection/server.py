from __future__ import annotations

import json
from typing import Any, cast

from airflow.models.connection import Connection
from sqlalchemy.engine.url import URL, make_url

from airflow_fernet_secrets.connection.common import (
    ConnectionDict,
    create_driver,
    parse_driver,
)

__all__ = [
    "convert_connection_to_dict",
    "create_airflow_connection",
    "is_sql_connection",
]


def convert_connection_to_dict(connection: Connection) -> ConnectionDict:
    as_dict = connection.to_dict()
    if is_sql_connection(connection):
        uri = connection.get_uri()
        url = cast("URL", make_url(uri))
        backend = url.get_backend_name()
        driver = create_driver(backend=backend, conn_type=connection.conn_type)
    else:
        driver = create_driver(conn_type=connection.conn_type)

    result: ConnectionDict = {"driver": driver, "extra": as_dict["extra"]}

    for key in ("host", "login", "password", "schema", "port"):
        value = as_dict.get(key, None)
        if not value:
            continue
        result[key] = value

    return result


def create_airflow_connection(connection: ConnectionDict) -> Connection:
    driver = parse_driver(connection["driver"])
    conn_type = driver.conn_type or driver.backend
    as_dict: dict[str, Any] = dict(connection)
    as_dict.pop("driver")
    as_dict["conn_type"] = conn_type
    as_json = json.dumps(as_dict)
    return Connection.from_json(as_json)


def is_sql_connection(connection: Connection) -> bool:
    from airflow.providers_manager import ProvidersManager

    hook_class = ProvidersManager().hooks.get(connection.conn_type or "", None)
    return callable(getattr(hook_class, "get_sqlalchemy_engine", None))
