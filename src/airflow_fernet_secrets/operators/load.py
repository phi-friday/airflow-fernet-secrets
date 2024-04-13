from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

import sqlalchemy as sa
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

__all__ = ["LoadConnectionsOperator"]


class LoadConnectionsOperator(HasIds):
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
                    overwrite=overwrite,
                    session=session,
                    stacklevel=2,
                )
                if value:
                    conn_result.add(value)

            for key in self._separated_var_ids:
                value = self._execute_var_process(
                    key=key,
                    backend=backend,
                    overwrite=overwrite,
                    session=session,
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
        overwrite: bool,
        session: Session,
        stacklevel: int = 1,
    ) -> str | None:
        if not conn_id:
            self.log.warning("skip empty conn id.")
            return None

        stmt = sa.select(Connection).where(Connection.conn_id == conn_id)
        old: Connection | None = session.execute(stmt).scalar_one_or_none()
        if old is not None and not overwrite:
            self.log.info("airflow already has %s", conn_id, stacklevel=stacklevel)
            return None

        connection = backend.get_connection(conn_id=conn_id)
        if connection is None:
            raise NotImplementedError

        if old is None:
            session.add(connection)
            session.flush()
            return conn_id

        unset = object()
        for col in (
            "description",
            "host",
            "login",
            "password",
            "schema",
            "port",
            "extra",
        ):
            new = getattr(connection, col, unset)
            if new is unset:
                continue
            setattr(old, col, new)

        session.add(old)
        session.flush()
        return conn_id

    def _execute_var_process(
        self,
        *,
        key: str,
        backend: ServerFernetLocalSecretsBackend,
        overwrite: bool,
        session: Session,
        stacklevel: int = 1,
    ) -> str | None:
        if not key:
            self.log.warning("skip empty key.")
            return None

        stmt = sa.select(Variable).where(Variable.key == key)
        old: Variable | None = session.execute(stmt).scalar_one_or_none()
        if old is not None and not overwrite:
            self.log.info("airflow already has %s", key, stacklevel=stacklevel)
            return None

        variable = backend.get_variable(key=key)
        if variable is None:
            raise NotImplementedError

        if old is None:
            new = Variable(key=key, val=variable)
            session.add(new)
            session.flush()
            return key

        old.set_val(variable)
        session.add(old)
        session.flush()
        return key
