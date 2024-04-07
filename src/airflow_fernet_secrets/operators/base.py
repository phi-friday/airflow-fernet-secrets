from __future__ import annotations

from airflow_fernet_secrets.core.config import HAS_AIRFLOW, IS_SERVER_FLAG

if not HAS_AIRFLOW or not IS_SERVER_FLAG:
    raise NotImplementedError

from itertools import chain
from typing import TYPE_CHECKING, Any, Sequence

from airflow.models import BaseOperator
from airflow.models.connection import Connection
from typing_extensions import override

from airflow_fernet_secrets.core.config.common import ensure_fernet
from airflow_fernet_secrets.core.config.server import load_secret_key
from airflow_fernet_secrets.core.utils.cast import ensure_boolean
from airflow_fernet_secrets.secrets.server import ServerFernetLocalSecretsBackend

if TYPE_CHECKING:
    from airflow.utils.context import Context
    from cryptography.fernet import Fernet

    from airflow_fernet_secrets.core.typeshed import PathType


class HasSecrets(BaseOperator):
    template_fields: Sequence[str] = (
        "fernet_secrets_key",
        "fernet_secrets_backend_file_path",
    )

    def __init__(
        self,
        *,
        fernet_secrets_key: str | bytes | Fernet | None = None,
        fernet_secrets_backend_file_path: PathType | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.fernet_secrets_backend_file = fernet_secrets_backend_file_path

        self._fernet_secrets_key = (
            None if fernet_secrets_key is None else ensure_fernet(fernet_secrets_key)
        )
        self._fernet_secrets_backend = None

    def _secret(self) -> Fernet:
        if self._fernet_secrets_key is not None:
            return self._fernet_secrets_key
        return load_secret_key(self.log)

    def _backend(self) -> ServerFernetLocalSecretsBackend:
        if self._fernet_secrets_backend is not None:
            return self._fernet_secrets_backend

        self._fernet_secrets_backend = ServerFernetLocalSecretsBackend(
            fernet_secrets_key=self._fernet_secrets_key,
            fernet_secrets_backend_file_path=self.fernet_secrets_backend_file,
        )
        return self._fernet_secrets_backend


class HasConnIds(HasSecrets):
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
            fernet_secrets_key=fernet_secrets_key,
            fernet_secrets_backend_file_path=fernet_secrets_backend_file_path,
            **kwargs,
        )

        fernet_secrets_conn_ids_separate = ensure_boolean(
            fernet_secrets_conn_ids_separate
        )
        if fernet_secrets_conn_ids is None:
            fernet_secrets_conn_ids = ""
        if isinstance(fernet_secrets_conn_ids, str):
            fernet_secrets_conn_ids = (fernet_secrets_conn_ids,)
        if fernet_secrets_conn_ids_separate:
            fernet_secrets_conn_ids = tuple(
                chain.from_iterable(
                    (
                        x.strip()
                        for x in conn_id.split(fernet_secrets_conn_ids_separator)
                    )
                    for conn_id in fernet_secrets_conn_ids
                )
            )
        else:
            fernet_secrets_conn_ids = tuple(fernet_secrets_conn_ids)

        self.fernet_secrets_conn_ids = fernet_secrets_conn_ids

    @override
    def execute(self, context: Context) -> Any:
        backend = self._backend()
        for conn_id in self.fernet_secrets_conn_ids:
            self._execute_process(conn_id=conn_id, backend=backend, stacklevel=2)

    def _execute_process(
        self,
        conn_id: str,
        backend: ServerFernetLocalSecretsBackend,
        stacklevel: int = 1,
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
