from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy.dialects import mssql as mssql_dialect
from sqlalchemy.dialects import postgresql as postgresql_dialect
from sqlalchemy.dialects import sqlite as sqlite_dialect
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession
from sqlalchemy.orm import Session

from airflow_fernet_secrets import const
from airflow_fernet_secrets import exceptions as fe
from airflow_fernet_secrets.database.connect import SessionMaker

if TYPE_CHECKING:
    from airflow_fernet_secrets.connection import ConnectionArgs, ConnectionDict

__all__ = [
    "convert_url_to_dict",
    "convert_connectable_to_dict",
    "create_url",
    "create_connection_args",
]


def convert_url_to_dict(url: str | URL) -> ConnectionDict:  # noqa: C901, PLR0912
    """sqlalchemy url to connection dict"""
    url = make_url(url)
    backend = url.get_backend_name()

    if backend == sqlite_dialect.dialect.name:
        conn_type = const.SQLITE_CONN_TYPE
    elif backend == postgresql_dialect.dialect.name:
        conn_type = const.POSTGRESQL_CONN_TYPE
    elif backend == mssql_dialect.dialect.name:
        conn_type = const.ODBC_CONN_TYPE
    else:
        raise fe.FernetSecretsNotImplementedError(backend)

    args: ConnectionArgs = {"url": url, "connect_args": {}, "engine_kwargs": {}}
    result: ConnectionDict = {
        "conn_type": conn_type,
        "extra": dict(url.query),
        "args": args,
    }

    if url.host:
        result["host"] = url.host
    if url.username:
        result["login"] = url.username
    if url.password is not None:
        if isinstance(url.password, str):
            result["password"] = url.password
        else:
            result["password"] = str(url.password)
    if url.database:
        if conn_type == const.SQLITE_CONN_TYPE:
            result["host"] = url.database
        else:
            result["schema"] = url.database
    if url.port:
        result["port"] = url.port

    return result


def convert_connectable_to_dict(
    connectable: Engine
    | Connection
    | SessionMaker[Session]
    | Session
    | AsyncEngine
    | AsyncConnection
    | SessionMaker[AsyncSession]
    | AsyncSession
    | URL
    | str,
) -> ConnectionDict:
    """sqlalchemy connectable to connection dict"""
    if isinstance(connectable, (Engine, Connection, AsyncEngine, AsyncConnection)):
        return convert_url_to_dict(connectable.engine.url)
    if isinstance(connectable, SessionMaker):
        connectable = connectable()
    if isinstance(connectable, (Session, AsyncSession)):
        bind = connectable.get_bind()
        return convert_url_to_dict(bind.engine.url)
    return convert_url_to_dict(connectable)


def create_url(connection: ConnectionDict) -> URL:
    """connection dict to url"""
    args = connection.get("args")
    if not args:
        raise fe.FernetSecretsValueError("connection dict has no connection args")

    url = args["url"]
    return make_url(url)


def create_connection_args(connection: ConnectionDict) -> ConnectionArgs:
    """connection dict to connection args"""
    url = create_url(connection)

    args = connection["args"] or {}
    connect_args = args.get("connect_args") or {}
    engine_kwargs = args.get("engine_kwargs") or {}

    return {"url": url, "connect_args": connect_args, "engine_kwargs": engine_kwargs}
