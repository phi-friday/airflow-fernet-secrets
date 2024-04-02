from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.models.connection import Connection
from airflow.secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin
from typing_extensions import override

if TYPE_CHECKING:
    from _typeshed import StrOrBytesPath
    from airflow.models.connection import Connection


class FernetLocalSecretsBackend(BaseSecretsBackend, LoggingMixin):
    @override
    def get_conn_value(
        self,
        *,
        variables_file_path: StrOrBytesPath | None = None,
        connections_file_path: StrOrBytesPath | None = None,
    ) -> str | None:
        super().__init__()
        self.variables_file = variables_file_path
        self.connections_file = connections_file_path

    @override
    def deserialize_connection(self, conn_id: str, value: str) -> Connection:
        return super().deserialize_connection(conn_id, value)

    @override
    def get_variable(self, key: str) -> str | None:
        return super().get_variable(key)
