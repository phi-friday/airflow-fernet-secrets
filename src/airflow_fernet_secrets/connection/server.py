from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, cast

from airflow_fernet_secrets import exceptions as fe

if TYPE_CHECKING:
    from airflow.models.connection import Connection

    from airflow_fernet_secrets.connection import ConnectionDict

__all__ = [
    "convert_connection_to_dict",
    "create_airflow_connection",
    "is_sql_connection",
]


def convert_connection_to_dict(connection: Connection) -> ConnectionDict:
    """airflow connection to connection dict"""
    from airflow_fernet_secrets.connection.dump import connection_to_args

    as_dict = _connection_to_dict(connection)

    conn_type = _get_conn_type(connection)
    args = connection_to_args(connection) if is_sql_connection(connection) else None
    result: ConnectionDict = {
        "conn_type": conn_type,
        "extra": as_dict["extra"],
        "args": args,
    }

    for key in ("host", "login", "password", "schema", "port"):
        value = as_dict.get(key, None)
        if not value:
            continue
        result[key] = value

    return result


def create_airflow_connection(
    connection: ConnectionDict, conn_id: str | None = None
) -> Connection:
    """connection dict to airflow connection"""
    from airflow.models.connection import Connection

    conn_type = connection.get("conn_type")
    if conn_type is None:
        error_msg = f"connection has no conn_type: id={conn_id}"
        raise fe.FernetSecretsValueError(error_msg)

    as_dict: dict[str, Any] = dict(connection)
    as_dict.pop("args", None)
    extra = as_dict.get("extra")
    if extra and not isinstance(extra, (str, bytes)):
        as_dict["extra"] = json.dumps(extra)

    as_json = json.dumps(as_dict)
    return Connection.from_json(as_json, conn_id=conn_id)


def is_sql_connection(connection: Connection) -> bool:
    """check is sql connection in airflow"""
    from airflow.providers_manager import ProvidersManager
    from airflow.utils.module_loading import import_string

    conn_type = _get_conn_type(connection)
    hook_info = ProvidersManager().hooks.get(conn_type, None)
    if hook_info is None:
        return False
    hook_class = import_string(hook_info.hook_class_name)
    return callable(getattr(hook_class, "get_sqlalchemy_engine", None))


def _get_conn_type(connection: Connection) -> str:
    return cast(str, connection.conn_type)


def _connection_to_dict(connection: Connection) -> dict[str, Any]:
    """obtained from airflow.models.Connection.to_dict"""
    if callable(getattr(connection, "to_dict", None)):
        return connection.to_dict()

    as_dict = {
        "conn_id": connection.conn_id,
        "conn_type": connection.conn_type,
        "description": connection.description,
        "host": connection.host,
        "login": connection.login,
        "password": connection.password,
        "schema": connection.schema,
        "port": connection.port,
    }
    as_dict = {key: value for key, value in as_dict.items() if value is not None}
    as_dict["extra"] = connection.extra_dejson
    json.dumps(as_dict)
    return as_dict
