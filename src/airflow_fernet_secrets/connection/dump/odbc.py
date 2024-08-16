from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from urllib.parse import quote_plus

from sqlalchemy.engine.url import make_url

from airflow.models.connection import Connection

from airflow_fernet_secrets import exceptions as fe
from airflow_fernet_secrets.const import ODBC_CONN_TYPES as _ODBC_CONN_TYPES

if TYPE_CHECKING:
    from airflow_fernet_secrets.connection import ConnectionArgs

__all__ = ["connection_to_args"]

_ODBC_EXTRA_EXCLUDE = frozenset({
    "driver",
    "dsn",
    "connect_kwargs",
    "sqlalchemy_scheme",
    "placeholder",
})
_DEFAULT_DRIVERNAME = "mssql+pyodbc"


def connection_to_args(connection: Connection) -> ConnectionArgs:
    """get sqlalchemy url, connect_args, engine_kwargs from airflow odbc connection"""
    if not isinstance(connection.conn_type, str):
        error_msg = (
            f"invalid conn_type attribute type: {type(connection.conn_type).__name__}"
        )
        raise fe.FernetSecretsTypeError(error_msg)
    if connection.conn_type.lower() not in _ODBC_CONN_TYPES:
        error_msg = f"invalid conn_type attribute: {connection.conn_type}"
        raise fe.FernetSecretsTypeError(error_msg)

    lower_extra = {
        str(key).lower(): value for key, value in connection.extra_dejson.items()
    }

    uri = _odbc_uri(connection, lower_extra)
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
    _connect_args = _odbc_connect_args(lower_extra)
    connect_args.update(_connect_args)

    return {"url": url, "connect_args": connect_args, "engine_kwargs": engine_kwargs}


def _odbc_uri(connection: Connection, lower_extra: dict[str, Any]) -> str:
    """obtained from airflow odbc provider hook.

    see more: `airflow.providers.odbc.hooks.odbc.OdbcHook.get_uri()`
    """

    driver = _odbc_driver(lower_extra)
    dsn = _odbc_dsn(lower_extra)
    host = str(connection.host) if connection.host else ""
    database = str(connection.schema) if connection.schema else ""
    login = str(connection.login) if connection.login else ""
    password = str(connection.password) if connection.password else ""
    port = str(connection.port) if connection.port else ""

    conn_str = ""
    if driver:
        conn_str += f"DRIVER={{{driver}}};"
    if dsn:
        conn_str += f"DSN={dsn};"
    if host:
        conn_str += f"SERVER={host};"
    if database:
        conn_str += f"DATABASE={database};"
    if login:
        conn_str += f"UID={login};"
    if password:
        conn_str += f"PWD={password};"
    if port:
        conn_str += f"PORT={port};"

    for key, value in lower_extra.items():
        if key in _ODBC_EXTRA_EXCLUDE:
            continue
        conn_str += f"{key}={value};"

    conn_str = quote_plus(conn_str)
    schema = lower_extra.get("sqlalchemy_scheme", _DEFAULT_DRIVERNAME)
    return f"{schema}:///?odbc_connect={conn_str}"


def _odbc_driver(lower_extra: dict[str, Any]) -> str:
    """obtained from airflow odbc provider hook.

    see more: `airflow.providers.odbc.hooks.odbc.OdbcHook.driver`
    """
    driver: str | None = lower_extra.get("driver")
    if driver:
        driver = driver.strip()

    if not driver:
        try:
            import pyodbc
        except ImportError:
            driver = ""
        else:
            driver = pyodbc.drivers()[0]

    return driver.strip().lstrip("{").rstrip("}").strip()


def _odbc_dsn(lower_extra: dict[str, Any]) -> str:
    """obtained from airflow odbc provider hook.

    see more: `airflow.providers.odbc.hooks.odbc.OdbcHook.dsn`
    """
    dsn: str | None = lower_extra.get("dsn")
    if dsn:
        return dsn.strip()
    return ""


def _odbc_connect_args(lower_extra: dict[str, Any]) -> dict[str, Any]:
    """obtained from airflow odbc provider hook.

    see more: `airflow.providers.odbc.hooks.odbc.OdbcHook.connect_kwargs`
    """
    connect_args: dict[str, Any] = lower_extra.get("connect_kwargs", {})

    if "attrs_before" in connect_args:
        attrs_before = connect_args.pop("attrs_before")
        if isinstance(attrs_before, dict):
            connect_args["attrs_before"] = {
                int(key): int(value) for key, value in attrs_before.items()
            }

    return connect_args
