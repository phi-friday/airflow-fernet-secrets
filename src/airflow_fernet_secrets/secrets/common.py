from __future__ import annotations

import json
from functools import cached_property
from typing import TYPE_CHECKING, Generic, Literal, cast

import sqlalchemy as sa
from typing_extensions import TypeVar, override

from airflow_fernet_secrets.core.config import (
    ensure_fernet,
    load_backend_file,
    load_secret_key,
)
from airflow_fernet_secrets.core.database import (
    create_sqlite_url,
    ensure_sqlite_engine,
    enter_database,
)
from airflow_fernet_secrets.core.log import LoggingMixin
from airflow_fernet_secrets.core.model import Connection as FernetConnection
from airflow_fernet_secrets.core.model import Variable as FernetVariable

if TYPE_CHECKING:
    from airflow.secrets import BaseSecretsBackend
    from cryptography.fernet import Fernet, MultiFernet
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.result import Result
    from sqlalchemy.engine.url import URL

    from airflow_fernet_secrets.connection import ConnectionDict
    from airflow_fernet_secrets.core.typeshed import PathType

    class BaseFernetLocalSecretsBackend(BaseSecretsBackend, LoggingMixin): ...

else:

    class BaseFernetLocalSecretsBackend(LoggingMixin): ...


__all__ = ["CommonFernetLocalSecretsBackend"]


ConnectionT = TypeVar("ConnectionT", infer_variance=True)


class CommonFernetLocalSecretsBackend(
    BaseFernetLocalSecretsBackend, Generic[ConnectionT]
):
    def __init__(
        self,
        *,
        fernet_secrets_key: str | bytes | Fernet | MultiFernet | None = None,
        fernet_secrets_backend_file_path: PathType | None = None,
    ) -> None:
        super().__init__()
        self.fernet_secrets_backend_file = fernet_secrets_backend_file_path

        self._fernet_secrets_key = (
            None if fernet_secrets_key is None else ensure_fernet(fernet_secrets_key)
        )

    @cached_property
    def _backend_url(self) -> URL:
        if self.fernet_secrets_backend_file is not None:
            return create_sqlite_url(self.fernet_secrets_backend_file)
        file = load_backend_file(self.log)
        return create_sqlite_url(file)

    @cached_property
    def _backend_engine(self) -> Engine:
        return ensure_sqlite_engine(self._backend_url)

    def _secret(self) -> MultiFernet:
        if self._fernet_secrets_key is not None:
            return self._fernet_secrets_key
        return load_secret_key(self.log)

    @override
    def get_conn_value(self, conn_id: str) -> str | None:
        with enter_database(self._backend_engine) as session:
            value = FernetConnection.get(session, conn_id)
            if value is None:
                return None
            value = self._validate_connection(
                conn_id=conn_id, connection=value, when="get"
            )
            return value.encrypted.decode("utf-8")

    def set_conn_value(
        self, conn_id: str, conn_type: str | None, value: str | bytes
    ) -> None:
        if isinstance(value, str):
            value = value.encode("utf-8")
        with enter_database(self._backend_engine) as session:
            connection = FernetConnection.get(
                session, conn_id=conn_id, conn_type=conn_type
            )
            if connection is None:
                connection = FernetConnection(
                    encrypted=value, conn_id=conn_id, conn_type=conn_type
                )
            else:
                secret_key = self._secret()
                FernetConnection.decrypt(connection.encrypted, secret_key)
                connection.encrypted = value
            connection = self._validate_connection(
                conn_id=conn_id, connection=connection, when="set"
            )
            session.add(connection)
            session.commit()

    def _validate_connection(
        self,
        conn_id: str,  # noqa: ARG002
        connection: FernetConnection,
        when: Literal["get", "set"],  # noqa: ARG002
    ) -> FernetConnection:
        return connection

    @override
    def deserialize_connection(self, conn_id: str, value: str | bytes) -> ConnectionT:
        secret_key = self._secret()
        value = secret_key.decrypt(value)
        as_dict = json.loads(value)
        as_dict = self._validate_connection_dict(
            conn_id=conn_id, connection=as_dict, when="deserialize"
        )
        return self._deserialize_connection(conn_id=conn_id, connection=as_dict)

    def _deserialize_connection(
        self, conn_id: str, connection: ConnectionDict
    ) -> ConnectionT:
        raise NotImplementedError

    def serialize_connection(
        self, conn_id: str, connection: ConnectionT
    ) -> str | bytes:
        secret_key = self._secret()
        as_dict = self._serialize_connection(conn_id=conn_id, connection=connection)
        as_dict = self._validate_connection_dict(
            conn_id=conn_id, connection=as_dict, when="serialize"
        )
        value = json.dumps(as_dict)
        return secret_key.encrypt(value.encode("utf-8"))

    def _serialize_connection(
        self, conn_id: str, connection: ConnectionT
    ) -> ConnectionDict:
        raise NotImplementedError

    def _validate_connection_dict(
        self,
        conn_id: str,  # noqa: ARG002
        connection: ConnectionDict,
        when: Literal["serialize", "deserialize"],  # noqa: ARG002
    ) -> ConnectionDict:
        return connection

    @override
    def get_connection(self, conn_id: str) -> ConnectionT | None:
        value = self.get_conn_value(conn_id)
        if value is None:
            return None

        return self.deserialize_connection(conn_id, value)

    def set_connection(
        self, conn_id: str, conn_type: str | None, connection: ConnectionT
    ) -> None:
        value = self.serialize_connection(conn_id, connection)
        self.set_conn_value(conn_id=conn_id, conn_type=conn_type, value=value)

    @override
    def get_variable(self, key: str) -> str | None:
        with enter_database(self._backend_engine) as session:
            value = FernetVariable.get(session, key)
            if value is None:
                return None
            session.expunge(value)

        fernet = self._secret()
        return FernetVariable.decrypt(value.encrypted, fernet)

    def set_variable(self, key: str, value: str) -> None:
        secret_key = self._secret()
        with enter_database(self._backend_engine) as session:
            as_bytes = FernetVariable.encrypt(value, secret_key)
            variable = FernetVariable.get(session, key)
            if variable is None:
                variable = FernetVariable(encrypted=as_bytes, key=key)
            else:
                variable.encrypted = as_bytes
            session.add(variable)
            session.commit()

    @override
    def get_config(self, key: str) -> str | None:
        return None

    def rotate(self) -> None:
        self._rotate_connections()
        self._rotate_variables()

    def _rotate_connections(self) -> None:
        secret_key = self._secret()
        do_rorate = False
        with enter_database(self._backend_engine) as session:
            fetch: Result = session.execute(sa.select(FernetConnection))

            while True:
                connections = cast(
                    "list[FernetConnection]", fetch.scalars().fetchmany(100)
                )
                if not connections:
                    break

                for connection in connections:
                    connection.encrypted = secret_key.rotate(connection.encrypted)

                session.add_all(connections)
                session.flush()
                do_rorate = True

            if do_rorate:
                session.commit()

    def _rotate_variables(self) -> None:
        do_rorate = False
        secret_key = self._secret()
        with enter_database(self._backend_engine) as session:
            fetch: Result = session.execute(sa.select(FernetVariable))

            while True:
                variables = cast("list[FernetVariable]", fetch.scalars().fetchmany(100))
                if not variables:
                    break

                for variable in variables:
                    variable.encrypted = secret_key.rotate(variable.encrypted)

                session.add_all(variables)
                session.flush()
                do_rorate = True

            if do_rorate:
                session.commit()
