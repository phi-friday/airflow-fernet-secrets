from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from typing_extensions import TypeAlias, override

from airflow_fernet_secrets import const
from airflow_fernet_secrets.config.server import load_backend_file as _load_backend_file
from airflow_fernet_secrets.config.server import load_secret_key as _load_secret_key
from airflow_fernet_secrets.connection import (
    convert_args_from_jsonable,
    convert_args_to_jsonable,
)
from airflow_fernet_secrets.connection.server import (
    convert_connection_to_dict,
    create_airflow_connection,
    is_sql_connection,
)
from airflow_fernet_secrets.log.server import LoggingMixin
from airflow_fernet_secrets.secrets.common import (
    CommonFernetLocalSecretsBackend as _CommonFernetLocalSecretsBackend,
)

if TYPE_CHECKING:
    from airflow.models.connection import Connection

    from airflow_fernet_secrets.connection import ConnectionDict
else:
    Connection = Any

__all__ = ["ServerFernetLocalSecretsBackend"]

ConnectionType: TypeAlias = "Connection"


class ServerFernetLocalSecretsBackend(
    _CommonFernetLocalSecretsBackend[Connection], LoggingMixin
):
    """using in airflow"""

    load_backend_file = staticmethod(_load_backend_file)
    load_secret_key = staticmethod(_load_secret_key)

    @override
    def _get_conn_type(self, connection: Connection) -> str:
        if is_sql_connection(connection):
            return const.SQL_CONN_TYPE
        return cast("str", connection.conn_type)

    @override
    def _deserialize_connection(
        self, conn_id: str, connection: ConnectionDict
    ) -> Connection:
        connection_args = connection["args"]
        if connection_args:
            connection = connection.copy()
            connection["args"] = convert_args_from_jsonable(connection_args)

        return create_airflow_connection(connection=connection, conn_id=conn_id)

    @override
    def _serialize_connection(
        self, conn_id: str, connection: Connection
    ) -> ConnectionDict:
        as_dict = convert_connection_to_dict(connection)
        args = as_dict["args"]
        if args is not None:
            as_dict["args"] = convert_args_to_jsonable(args)
        return as_dict
