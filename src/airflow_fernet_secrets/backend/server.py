from __future__ import annotations

from typing import TYPE_CHECKING, Any

from typing_extensions import override

from airflow_fernet_secrets.backend.common import (
    CommonFernetLocalSecretsBackend as _CommonFernetLocalSecretsBackend,
)
from airflow_fernet_secrets.connection.server import (
    convert_connection_to_dict,
    create_airflow_connection,
    is_sql_connection,
)

if TYPE_CHECKING:
    from airflow.models.connection import Connection

    from airflow_fernet_secrets.connection import ConnectionDict
else:
    Connection = Any

__all__ = ["FernetLocalSecretsBackend"]


class FernetLocalSecretsBackend(_CommonFernetLocalSecretsBackend[Connection]):
    @override
    def set_connection(
        self, conn_id: str, conn_type: str | None, connection: Connection
    ) -> None:
        conn_type_or_null: str | None = (
            "sql"
            if is_sql_connection(connection, conn_type=conn_type)
            else conn_type
            if connection.conn_type is None
            else connection.conn_type
        )
        return super().set_connection(
            conn_id=conn_id, conn_type=conn_type_or_null, connection=connection
        )

    @override
    def _deserialize_connection(
        self, conn_id: str, connection: ConnectionDict
    ) -> Connection:
        result = create_airflow_connection(connection=connection)
        result.conn_id = conn_id
        return result

    @override
    def _serialize_connection(
        self, conn_id: str, connection: Connection
    ) -> ConnectionDict:
        return convert_connection_to_dict(connection)
