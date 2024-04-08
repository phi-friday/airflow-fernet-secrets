from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from airflow.models.connection import Connection
from typing_extensions import override

from airflow_fernet_secrets.connection.server import (
    convert_connection_to_dict,
    create_airflow_connection,
    is_sql_connection,
)
from airflow_fernet_secrets.core.config import const
from airflow_fernet_secrets.secrets.common import (
    CommonFernetLocalSecretsBackend as _CommonFernetLocalSecretsBackend,
)

if TYPE_CHECKING:
    from airflow.models.connection import Connection

    from airflow_fernet_secrets.connection import ConnectionDict
else:
    Connection = Any

__all__ = ["ServerFernetLocalSecretsBackend"]


class ServerFernetLocalSecretsBackend(_CommonFernetLocalSecretsBackend[Connection]):
    @override
    def _get_conn_type(self, connection: Connection) -> str:
        if is_sql_connection(connection):
            return const.SQL_CONN_TYPE
        return cast("str", connection.conn_type)

    @override
    def _deserialize_connection(
        self, conn_id: str, connection: ConnectionDict
    ) -> Connection:
        return create_airflow_connection(connection=connection, conn_id=conn_id)

    @override
    def _serialize_connection(
        self, conn_id: str, connection: Connection
    ) -> ConnectionDict:
        return convert_connection_to_dict(connection)
