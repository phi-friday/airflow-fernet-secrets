from __future__ import annotations

from airflow.models.connection import Connection
from typing_extensions import override

from airflow_fernet_secrets.backend.common import (
    CommonFernetLocalSecretsBackend as _CommonFernetLocalSecretsBackend,
)

__all__ = ["FernetLocalSecretsBackend"]


class FernetLocalSecretsBackend(_CommonFernetLocalSecretsBackend[Connection]):
    @override
    def _deserialize_connection(self, conn_id: str, value: bytes) -> Connection:
        return Connection.from_json(value, conn_id=conn_id)

    @override
    def serialize_connection(self, conn_id: str, connection: Connection) -> bytes:
        as_str = connection.as_json()
        return as_str.encode("utf-8")
