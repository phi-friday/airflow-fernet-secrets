from __future__ import annotations

from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession
from sqlalchemy.orm import Session

from airflow_fernet_secrets.connection import (
    ConnectionDict,
    create_driver,
    parse_driver,
)
from airflow_fernet_secrets.core.database import SessionMaker


def convert_url_to_dict(url: str | URL) -> ConnectionDict:
    url = make_url(url)
    backend = url.get_backend_name()
    dialect = url.get_driver_name()
    driver = create_driver(backend=backend, dialect=dialect)
    result: ConnectionDict = {"driver": driver, "extra": dict(url.query)}

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
        if backend == sqlite_dialect.name:
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
    if isinstance(connectable, (Engine, Connection, AsyncEngine, AsyncConnection)):
        return convert_url_to_dict(connectable.engine.url)
    if isinstance(connectable, SessionMaker):
        connectable = connectable()
    if isinstance(connectable, (Session, AsyncSession)):
        bind = connectable.get_bind()
        return convert_url_to_dict(bind.engine.url)
    return convert_url_to_dict(connectable)


def create_url(connection: ConnectionDict) -> URL:
    driver = parse_driver(connection["driver"])
    drivername = (
        f"{driver.backend}+{driver.dialect}" if driver.dialect else driver.backend
    )

    url = URL.create(
        drivername=drivername,
        username=connection.get("login", None),
        password=connection.get("password", None),
        host=connection.get("host", None),
        port=connection.get("port", None),
        database=connection.get("schema", None),
        query=connection.get("extra", None) or {},
    )

    if driver.backend == sqlite_dialect.name:
        url = url.set(host="", database=url.host)

    return url
