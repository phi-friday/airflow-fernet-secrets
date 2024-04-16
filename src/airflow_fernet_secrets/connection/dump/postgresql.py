from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.models.connection import Connection
from sqlalchemy.engine.url import make_url

from airflow_fernet_secrets.const import POSTGRESQL_CONN_TYPES as _POSTGRESQL_CONN_TYPES

if TYPE_CHECKING:
    from airflow_fernet_secrets.connection.dump.main import ConnectionArgs

__all__ = ["connection_to_args"]


def connection_to_args(connection: Connection) -> ConnectionArgs:
    """get sqlalchemy url, connect_args, engine_kwargs from airflow postgresql connection"""  # noqa: E501
    if not isinstance(connection.conn_type, str):
        error_msg = (
            f"invalid conn_type attribute type: {type(connection.conn_type).__name__}"
        )
        raise TypeError(error_msg)
    if connection.conn_type.lower() not in _POSTGRESQL_CONN_TYPES:
        error_msg = f"invalid conn_type attribute: {connection.conn_type}"
        raise TypeError(error_msg)

    uri = _postgresql_uri(connection)
    url = make_url(uri)

    backend, *drivers = url.drivername.split("+", 1)
    backend = backend.lower()
    driver = drivers[0] if drivers else ""

    if backend in _POSTGRESQL_CONN_TYPES:
        backend = "postgresql"
    else:
        error_msg = f"invalid backend: {backend}"
        raise TypeError(error_msg)

    if driver:
        url = url.set(drivername=f"{backend}+{driver}")
    else:
        url = url.set(drivername="postgresql+psycopg2")

    url = url.difference_update_query([Connection.EXTRA_KEY])
    extras = dict(connection.extra_dejson)

    engine_kwargs: dict[str, Any] = extras.pop("engine_kwargs", {})
    connect_args: dict[str, Any] = engine_kwargs.pop("connect_args", {})

    if url.database:
        for key in ("dbname", "database"):
            connect_args.pop(key, None)

    return {"url": url, "connect_args": connect_args, "engine_kwargs": engine_kwargs}


def _postgresql_uri(connection: Connection) -> str:
    """obtained from airflow postgresql provider hook"""
    return connection.get_uri().replace("postgres://", "postgresql://")
