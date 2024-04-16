from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, cast

from airflow_fernet_secrets.connection.dump.main import connection_to_args

if TYPE_CHECKING:
    from airflow.models.connection import Connection

    from airflow_fernet_secrets.connection import ConnectionDict

__all__ = [
    "convert_connection_to_dict",
    "create_airflow_connection",
    "is_sql_connection",
]


def convert_connection_to_dict(connection: Connection) -> ConnectionDict:
    as_dict = connection.to_dict()

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
    from airflow.models.connection import Connection

    conn_type = connection.get("conn_type")
    if conn_type is None:
        raise NotImplementedError

    as_dict: dict[str, Any] = dict(connection)
    as_dict.pop("args", None)
    extra = as_dict.get("extra")
    if extra and not isinstance(extra, (str, bytes)):
        as_dict["extra"] = json.dumps(extra)

    as_json = json.dumps(as_dict)
    return Connection.from_json(as_json, conn_id=conn_id)


def is_sql_connection(connection: Connection) -> bool:
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
