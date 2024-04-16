from __future__ import annotations

from typing import TYPE_CHECKING, Any
from urllib.parse import unquote

from airflow.models.connection import Connection
from sqlalchemy.engine.url import make_url

from airflow_fernet_secrets.const import SQLITE_CONN_TYPES as _SQLITE_CONN_TYPES

if TYPE_CHECKING:
    from airflow_fernet_secrets.connection.dump.main import ConnectionArgs

__all__ = ["connection_to_args"]


def connection_to_args(connection: Connection) -> ConnectionArgs:
    """get sqlalchemy url, connect_args, engine_kwargs from airflow sqlite connection"""
    if not isinstance(connection.conn_type, str):
        error_msg = (
            f"invalid conn_type attribute type: {type(connection.conn_type).__name__}"
        )
        raise TypeError(error_msg)
    if connection.conn_type.lower() not in _SQLITE_CONN_TYPES:
        error_msg = f"invalid conn_type attribute: {connection.conn_type}"
        raise TypeError(error_msg)

    uri = _sqlite_uri(connection)
    url = make_url(uri)

    backend, *drivers = url.drivername.split("+", 1)
    backend = backend.lower()
    driver = drivers[0] if drivers else ""

    if not driver:
        url = url.set(drivername="sqlite+pysqlite")
    url = url.difference_update_query([Connection.EXTRA_KEY])

    extras = dict(connection.extra_dejson)
    engine_kwargs: dict[str, Any] = extras.pop("engine_kwargs", {})
    connect_args: dict[str, Any] = engine_kwargs.pop("connect_args", {})

    return {"url": url, "connect_args": connect_args, "engine_kwargs": engine_kwargs}


def _sqlite_uri(connection: Connection) -> str:
    """obtained from airflow sqlite provider hook"""
    return (
        unquote(connection.get_uri())
        .replace("/?", "?")
        .replace("sqlite://", "sqlite:///")
    )
