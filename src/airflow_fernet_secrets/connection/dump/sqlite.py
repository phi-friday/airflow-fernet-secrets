from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote

from sqlalchemy.engine.url import make_url

from airflow.models.connection import Connection

from airflow_fernet_secrets import exceptions as fe
from airflow_fernet_secrets.const import SQLITE_CONN_TYPES as _SQLITE_CONN_TYPES

if TYPE_CHECKING:
    from airflow_fernet_secrets.connection import ConnectionArgs

__all__ = ["connection_to_args"]


def connection_to_args(connection: Connection) -> ConnectionArgs:
    """get sqlalchemy url, connect_args, engine_kwargs from airflow sqlite connection"""
    if not isinstance(connection.conn_type, str):
        error_msg = (
            f"invalid conn_type attribute type: {type(connection.conn_type).__name__}"
        )
        raise fe.FernetSecretsTypeError(error_msg)
    if connection.conn_type.lower() not in _SQLITE_CONN_TYPES:
        error_msg = f"invalid conn_type attribute: {connection.conn_type}"
        raise fe.FernetSecretsTypeError(error_msg)

    uri = _sqlite_uri(connection)
    url = make_url(uri)

    backend, *drivers = url.drivername.split("+", 1)
    backend = backend.lower()
    driver = drivers[0] if drivers else ""

    if not driver:
        url = url.set(drivername="sqlite+pysqlite")
    url = url.difference_update_query([Connection.EXTRA_KEY])

    lower_extra = {
        str(key).lower(): value for key, value in connection.extra_dejson.items()
    }
    engine_kwargs: dict[str, Any] = lower_extra.pop("engine_kwargs", {})
    if isinstance(engine_kwargs, str):
        engine_kwargs = json.loads(engine_kwargs)
    engine_kwargs = dict(engine_kwargs)

    connect_args: dict[str, Any] = engine_kwargs.pop("connect_args", {})
    if isinstance(connect_args, str):
        connect_args = json.loads(connect_args)
    connect_args = dict(connect_args)

    return {"url": url, "connect_args": connect_args, "engine_kwargs": engine_kwargs}


def _sqlite_uri(connection: Connection) -> str:
    """obtained from airflow sqlite provider hook.

    see more: `airflow.providers.sqlite.hooks.sqlite.SqliteHook.get_uri()`
    """
    return (
        unquote(connection.get_uri())
        .replace("/?", "?")
        .replace("sqlite://", "sqlite:///")
    )
