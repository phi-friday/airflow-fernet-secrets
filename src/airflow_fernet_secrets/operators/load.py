from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

import sqlalchemy as sa
from airflow.exceptions import AirflowNotFoundException
from airflow.models.connection import Connection
from airflow.utils.session import create_session
from typing_extensions import override

from airflow_fernet_secrets.core.utils.cast import ensure_boolean
from airflow_fernet_secrets.operators.base import HasConnIds

if TYPE_CHECKING:
    from airflow.utils.context import Context
    from cryptography.fernet import Fernet, MultiFernet
    from sqlalchemy.orm import Session

    from airflow_fernet_secrets.core.typeshed import PathType
    from airflow_fernet_secrets.secrets.server import ServerFernetLocalSecretsBackend

__all__ = ["LoadConnectionsOperator"]


class LoadConnectionsOperator(HasConnIds):
    template_fields: Sequence[str] = (
        "fernet_secrets_conn_ids",
        "fernet_secrets_conn_ids_separate",
        "fernet_secrets_conn_ids_separator",
        "fernet_secrets_key",
        "fernet_secrets_backend_file_path",
        "fernet_secrets_overwrite",
    )

    def __init__(
        self,
        *,
        fernet_secrets_conn_ids: str | list[str] | tuple[str, ...] | None = None,
        fernet_secrets_conn_ids_separate: str | bool = False,
        fernet_secrets_conn_ids_separator: str = ",",
        fernet_secrets_key: str | bytes | Fernet | MultiFernet | None = None,
        fernet_secrets_backend_file_path: PathType | None = None,
        fernet_secrets_overwrite: str | bool = False,
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
        fernet_secrets_overwrite = ensure_boolean(fernet_secrets_overwrite)
        self.fernet_secrets_overwrite = fernet_secrets_overwrite

    @override
    def execute(self, context: Context) -> Any:
        if not self.fernet_secrets_conn_ids:
            self.log.warning("skip empty conn ids.")
            return

        backend = self._backend()
        with create_session() as session:
            for conn_id in self.fernet_secrets_conn_ids:
                self._execute_process(
                    conn_id=conn_id, backend=backend, session=session, stacklevel=2
                )
            session.commit()

    def _execute_process(
        self,
        conn_id: str,
        backend: ServerFernetLocalSecretsBackend,
        session: Session,
        stacklevel: int = 1,
    ) -> None:
        if not conn_id:
            self.log.warning("skip empty conn id.")
            return

        stmt = sa.select(Connection).where(Connection.conn_id == conn_id)
        try:
            old: Connection = session.execute(stmt).scalar_one()
        except AirflowNotFoundException:
            pass
        else:
            if not self.fernet_secrets_overwrite:
                self.log.info("airflow already has %s", conn_id, stacklevel=stacklevel)
                return

        connection = backend.get_connection(conn_id=conn_id)
        if connection is None:
            raise NotImplementedError

        exclude = {"id", "conn_id"}
        table: sa.Table = Connection.__table__
        unset = object()
        for col in table.columns.keys():  # noqa: SIM118
            if col in exclude:
                continue
            new = getattr(connection, col, unset)
            if new is unset:
                continue
            setattr(old, col, new)

        session.add(old)
