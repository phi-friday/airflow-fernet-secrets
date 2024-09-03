from __future__ import annotations

import json
from copy import deepcopy
from typing import TYPE_CHECKING, Any, Literal

from sqlalchemy.engine.url import URL

from airflow.models.connection import Connection

from airflow_fernet_secrets import exceptions as fe
from airflow_fernet_secrets.const import POSTGRESQL_CONN_TYPES as _POSTGRESQL_CONN_TYPES

if TYPE_CHECKING:
    from psycopg2._psycopg import cursor

    from airflow_fernet_secrets.connection import ConnectionArgs

__all__ = ["connection_to_args"]

_POSTGRESQL_EXTRA_EXCLUDE = frozenset({
    "iam",
    "redshift",
    "cursor",
    "cluster-identifier",
    "aws_conn_id",
})


def connection_to_args(connection: Connection) -> ConnectionArgs:
    """get sqlalchemy url, connect_args, engine_kwargs from airflow postgresql connection"""  # noqa: E501
    if not isinstance(connection.conn_type, str):
        error_msg = (
            f"invalid conn_type attribute type: {type(connection.conn_type).__name__}"
        )
        raise fe.FernetSecretsTypeError(error_msg)
    if connection.conn_type.lower() not in _POSTGRESQL_CONN_TYPES:
        error_msg = f"invalid conn_type attribute: {connection.conn_type}"
        raise fe.FernetSecretsTypeError(error_msg)

    url = URL.create(
        drivername="postgresql",
        username=connection.login,  # pyright: ignore[reportArgumentType]
        password=connection.password,
        host=connection.host,  # pyright: ignore[reportArgumentType]
        port=connection.port,  # pyright: ignore[reportArgumentType]
        database=connection.schema,  # pyright: ignore[reportArgumentType]
    )

    backend, *drivers = url.drivername.split("+", 1)
    backend = backend.lower()
    driver = drivers[0] if drivers else ""

    if backend in _POSTGRESQL_CONN_TYPES:
        backend = "postgresql"
    else:
        error_msg = f"invalid backend: {backend}"
        raise fe.FernetSecretsTypeError(error_msg)

    if driver:
        url = url.set(drivername=f"{backend}+{driver}")
    else:
        url = url.set(drivername="postgresql+psycopg2")

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
    _connect_args = _postgresql_connect_args(connection, lower_extra)
    connect_args.update(_connect_args)

    if url.database:
        for key in ("dbname", "database"):
            connect_args.pop(key, None)

    return {"url": url, "connect_args": connect_args, "engine_kwargs": engine_kwargs}


def _postgresql_connect_args(
    connection: Connection, extras: dict[str, Any] | None = None
) -> dict[str, Any]:
    """obtained from airflow postgresql provider hook.

    see more: `airflow.providers.postgres.hooks.postgres.PostgresHook.get_conn()`
    """
    connection = deepcopy(connection)
    if extras is None:
        extras = connection.extra_dejson

    """ needs hook & aws
    if extras.get("iam", False):
        connection.login, connection.password, connection.port = hook.get_iam_token(
            connection
        )
    """

    connect_args: dict[str, Any] = {
        "host": connection.host,
        "user": connection.login,
        "password": connection.password,
        "dbname": connection.schema,
        "port": connection.port,
    }

    raw_cursor = extras.get("cursor", "")
    if raw_cursor:
        connect_args["cursor_factory"] = _postgresql_cursor(raw_cursor)

    for arg_name, arg_val in extras.items():
        if arg_name in _POSTGRESQL_EXTRA_EXCLUDE:
            continue
        connect_args[arg_name] = arg_val

    return connect_args


def _postgresql_cursor(
    cursor_type: Literal["dictcursor", "realdictcursor", "namedtuplecursor"],
) -> type[cursor]:
    """obtained from airflow postgresql provider hook.

    see more: `airflow.providers.postgres.hooks.postgres.PostgresHook._get_cursor()`
    """
    if cursor_type == "dictcursor":
        from psycopg2.extras import DictCursor as Cursor
    elif cursor_type == "realdictcursor":
        from psycopg2.extras import RealDictCursor as Cursor
    elif cursor_type == "namedtuplecursor":
        from psycopg2.extras import NamedTupleCursor as Cursor
    else:
        error_msg = f"invalid cursor type: {cursor_type}"
        raise fe.FernetSecretsTypeError(error_msg)

    return Cursor
