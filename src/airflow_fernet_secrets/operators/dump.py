from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

import sqlalchemy as sa
from airflow.exceptions import AirflowNotFoundException
from airflow.models.connection import Connection
from airflow.models.variable import Variable
from airflow.utils.session import create_session
from typing_extensions import override

from airflow_fernet_secrets.operators.base import HasIds, OperatorResult
from airflow_fernet_secrets.utils.cast import ensure_boolean

if TYPE_CHECKING:
    from airflow.utils.context import Context
    from cryptography.fernet import Fernet, MultiFernet
    from sqlalchemy.orm import Session

    from airflow_fernet_secrets._typeshed import PathType
    from airflow_fernet_secrets.secrets.server import ServerFernetLocalSecretsBackend


__all__ = ["DumpSecretsOperator"]


class DumpSecretsOperator(HasIds):
    template_fields: Sequence[str] = (
        "fernet_secrets_conn_ids",
        "fernet_secrets_var_ids",
        "fernet_secrets_separate",
        "fernet_secrets_separator",
        "fernet_secrets_key",
        "fernet_secrets_backend_file_path",
        "fernet_secrets_overwrite",
    )

    def __init__(
        self,
        *,
        fernet_secrets_conn_ids: str | list[str] | tuple[str, ...] | None = None,
        fernet_secrets_var_ids: str | list[str] | tuple[str, ...] | None = None,
        fernet_secrets_separate: str | bool = False,
        fernet_secrets_separator: str = ",",
        fernet_secrets_key: str | bytes | Fernet | MultiFernet | None = None,
        fernet_secrets_backend_file_path: PathType | None = None,
        fernet_secrets_overwrite: str | bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            fernet_secrets_conn_ids=fernet_secrets_conn_ids,
            fernet_secrets_var_ids=fernet_secrets_var_ids,
            fernet_secrets_separate=fernet_secrets_separate,
            fernet_secrets_separator=fernet_secrets_separator,
            fernet_secrets_key=fernet_secrets_key,
            fernet_secrets_backend_file_path=fernet_secrets_backend_file_path,
            **kwargs,
        )

        self.fernet_secrets_overwrite = fernet_secrets_overwrite

    @override
    def execute(self, context: Context) -> OperatorResult:
        if not self._separated_conn_ids and not self._separated_var_ids:
            self.log.warning("skip: empty conn ids & var ids.")
            return {"connection": [], "variable": []}

        overwrite = ensure_boolean(self.fernet_secrets_overwrite)
        backend = self._backend()
        conn_result: set[str]
        var_result: set[str]
        conn_result, var_result = set(), set()

        with create_session() as session:
            for conn_id in self._separated_conn_ids:
                value = self._execute_conn_process(
                    conn_id=conn_id,
                    backend=backend,
                    session=session,
                    overwrite=overwrite,
                    stacklevel=2,
                )
                if value:
                    conn_result.add(value)

            for key in self._separated_var_ids:
                value = self._execute_var_process(
                    key=key,
                    backend=backend,
                    session=session,
                    overwrite=overwrite,
                    stacklevel=2,
                )
                if value:
                    var_result.add(value)

        return {"connection": sorted(conn_result), "variable": sorted(var_result)}

    def _execute_conn_process(
        self,
        *,
        conn_id: str,
        backend: ServerFernetLocalSecretsBackend,
        session: Session,
        overwrite: bool,
        stacklevel: int = 1,
    ) -> str | None:
        if not conn_id:
            self.log.warning("skip empty conn id.")
            return None

        check = backend.has_connection(conn_id=conn_id)
        if check and not overwrite:
            self.log.info(
                "secret backend already has %s", conn_id, stacklevel=stacklevel
            )
            return None

        connection = session.execute(
            sa.select(Connection).where(Connection.conn_id == conn_id)
        ).scalar_one_or_none()
        if connection is None:
            error_msg = f"there is no connection({conn_id})."
            raise AirflowNotFoundException(error_msg)

        backend.set_connection(conn_id, connection)
        return conn_id

    def _execute_var_process(
        self,
        *,
        key: str,
        backend: ServerFernetLocalSecretsBackend,
        session: Session,
        overwrite: bool,
        stacklevel: int = 1,
    ) -> str | None:
        if not key:
            self.log.warning("skip empty key.")
            return None

        check = backend.has_variable(key=key)
        if check and not overwrite:
            self.log.info("secret backend already has %s", key, stacklevel=stacklevel)
            return None

        variable = session.execute(
            sa.select(Variable).where(Variable.key == key)
        ).scalar_one_or_none()
        if variable is None:
            error_msg = f"there is no variable({key})."
            raise AirflowNotFoundException(error_msg)

        backend.set_variable(key, variable.val)
        return key
