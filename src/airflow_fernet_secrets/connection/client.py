from __future__ import annotations

from typing import cast

from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from airflow_fernet_secrets.connection import (
    ConnectionDict,
    create_driver,
    parse_driver,
)


def convert_url_to_dict(url: str | URL) -> ConnectionDict:
    url = cast("URL", make_url(url))
    backend = url.get_backend_name()
    dialect = url.get_driver_name()
    driver = create_driver(backend=backend, dialect=dialect)
    result: ConnectionDict = {"driver": driver, "extra": dict(url.query)}

    if url.host:
        result["host"] = url.host
    if url.username:
        result["login"] = url.username
    if url.password:
        result["password"] = url.password
    if url.database:
        result["schema"] = url.database
    if url.port:
        result["port"] = url.port

    return result


def convert_connectable_to_dict(
    connectable: Engine
    | Connection
    | sessionmaker
    | scoped_session
    | Session
    | URL
    | str,
) -> ConnectionDict:
    if isinstance(connectable, (Engine, Connection)):
        return convert_url_to_dict(connectable.engine.url)
    if isinstance(connectable, (sessionmaker, scoped_session)):
        connectable = connectable()
    if isinstance(connectable, Session):
        connection: Connection = connectable.connection()
        return convert_url_to_dict(connection.engine.url)
    return convert_url_to_dict(connectable)


def create_url(connection: ConnectionDict) -> URL:
    driver = parse_driver(connection["driver"])
    drivername = (
        f"{driver.backend}+{driver.dialect}" if driver.dialect else driver.backend
    )

    return URL.create(
        drivername=drivername,
        username=connection.get("login", None),
        password=connection.get("password", None),
        host=connection.get("host", None),
        port=connection.get("port", None),
        database=connection.get("schema", None),
        query=connection.get("extra", None) or {},
    )
