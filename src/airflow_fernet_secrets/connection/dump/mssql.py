from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from urllib import parse

from sqlalchemy.engine.url import make_url

from airflow.models.connection import Connection

from airflow_fernet_secrets import exceptions as fe
from airflow_fernet_secrets.const import MSSQL_CONN_TYPE as _MSSQL_CONN_TYPE

if TYPE_CHECKING:
    from airflow_fernet_secrets.connection import ConnectionArgs

__all__ = ["connection_to_args"]

_DEFAULT_DRIVERNAME = "mssql+pymssql"


def connection_to_args(connection: Connection) -> ConnectionArgs:
    """get sqlalchemy url, connect_args, engine_kwargs from airflow mssql connection"""
    if not isinstance(connection.conn_type, str):
        error_msg = (
            f"invalid conn_type attribute type: {type(connection.conn_type).__name__}"
        )
        raise fe.FernetSecretsTypeError(error_msg)
    if connection.conn_type.lower() not in _MSSQL_CONN_TYPE:
        error_msg = f"invalid conn_type attribute: {connection.conn_type}"
        raise fe.FernetSecretsTypeError(error_msg)

    lower_extra = {
        str(key).lower(): value for key, value in connection.extra_dejson.items()
    }

    uri = _mssql_uri(connection, lower_extra)
    url = make_url(uri)

    backend, *drivers = url.drivername.split("+", 1)
    backend = backend.lower()
    driver = drivers[0] if drivers else ""

    if not driver:
        url = url.set(drivername=_DEFAULT_DRIVERNAME)
    url = url.difference_update_query([Connection.EXTRA_KEY])

    engine_kwargs: dict[str, Any] = lower_extra.pop("engine_kwargs", {})
    if isinstance(engine_kwargs, str):
        engine_kwargs = json.loads(engine_kwargs)
    engine_kwargs = dict(engine_kwargs)

    connect_args: dict[str, Any] = engine_kwargs.pop("connect_args", {})
    if isinstance(connect_args, str):
        connect_args = json.loads(connect_args)
    connect_args = dict(connect_args)
    _connect_args = _mssql_connect_args(connection)
    connect_args.update(_connect_args)

    return {"url": url, "connect_args": connect_args, "engine_kwargs": engine_kwargs}


def _mssql_uri(connection: Connection, lower_extra: dict[str, Any]) -> str:
    """obtained from airflow mssql provider hook.

    see more: `airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.get_uri()`
    """

    uri = connection.get_uri()

    parsed = list(parse.urlsplit(uri))
    parsed[0] = _mssql_scheme(lower_extra)

    query = parse.parse_qs(parsed[3], keep_blank_values=True)
    for key in tuple(query):
        if key.lower() == "sqlalchemy_scheme":
            query.pop(key, None)

    parsed[3] = parse.urlencode(query, doseq=True)
    return parse.urlunsplit(parsed)


def _mssql_scheme(lower_extra: dict[str, Any]) -> str:
    """obtained from airflow mssql provider hook.

    see more: `airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.sqlalchemy_scheme`
    """  # noqa: E501
    schema = lower_extra.get("sqlalchemy_schema")
    if not schema or not isinstance(schema, str) or ":" in schema or "/" in schema:
        return _DEFAULT_DRIVERNAME
    return schema


def _mssql_connect_args(connection: Connection) -> dict[str, Any]:
    """obtained from airflow mssql provider hook.

    see more: `airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.get_conn()`
    """

    return {
        "server": connection.host,
        "user": connection.login,
        "password": connection.password,
        "database": connection.schema,
        "port": connection.port,
    }
