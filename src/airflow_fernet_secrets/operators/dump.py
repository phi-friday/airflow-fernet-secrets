from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from airflow.models.connection import Connection
from typing_extensions import override

from airflow_fernet_secrets.operators.base import HasConnIds

if TYPE_CHECKING:
    from airflow.utils.context import Context
    from cryptography.fernet import Fernet

    from airflow_fernet_secrets.core.typeshed import PathType
    from airflow_fernet_secrets.secrets.server import FernetLocalSecretsBackend


__all__ = ["DumpConnectionsOperator"]


class DumpConnectionsOperator(HasConnIds):
    template_fields: Sequence[str] = (
        "fernet_secrets_conn_ids",
        "fernet_secrets_conn_ids_separate",
        "fernet_secrets_conn_ids_separator",
        "fernet_secrets_key",
        "fernet_secrets_backend_file_path",
    )

    def __init__(
        self,
        *,
        fernet_secrets_conn_ids: str | list[str] | tuple[str, ...] | None = None,
        fernet_secrets_conn_ids_separate: str | bool = False,
        fernet_secrets_conn_ids_separator: str = ",",
        fernet_secrets_key: str | bytes | Fernet | None = None,
        fernet_secrets_backend_file_path: PathType | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            fernet_secrets_conn_ids=fernet_secrets_conn_ids,
            fernet_secrets_conn_ids_separate=fernet_secrets_conn_ids_separate,
            fernet_secrets_conn_ids_separator=fernet_secrets_conn_ids_separator,
            fernet_secrets_key=fernet_secrets_key,
            fernet_secrets_backend_file_path=fernet_secrets_backend_file_path,
            **kwargs,
        )

    @override
    def execute(self, context: Context) -> Any:
        if not self.fernet_secrets_conn_ids:
            self.log.warning("skip empty conn ids.")
            return

        backend = self._backend()
        for conn_id in self.fernet_secrets_conn_ids:
            self._execute_process(conn_id=conn_id, backend=backend, stacklevel=2)

    def _execute_process(
        self, conn_id: str, backend: FernetLocalSecretsBackend, stacklevel: int = 1
    ) -> None:
        if not conn_id:
            self.log.warning("skip empty conn id.")
            return

        conn_value = backend.get_conn_value(conn_id)
        if conn_value:
            self.log.info(
                "secret backend already has %s", conn_id, stacklevel=stacklevel
            )
            return

        connection = Connection.get_connection_from_secrets(conn_id)
        backend.set_connection(conn_id, connection.conn_type, connection)
