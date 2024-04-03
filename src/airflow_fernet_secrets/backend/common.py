from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from airflow_fernet_secrets.common.config import (
    ensure_fernet,
    load_backend_file,
    load_secret_key,
)
from airflow_fernet_secrets.common.database import (
    create_sqlite_url,
    ensure_sqlite_engine,
    enter_database,
)
from airflow_fernet_secrets.common.log import LoggingMixin
from airflow_fernet_secrets.common.model import Connection as FernetConnection
from airflow_fernet_secrets.common.model import Encrypted
from airflow_fernet_secrets.common.model import Variable as FernetVariable

if TYPE_CHECKING:
    from airflow.secrets import BaseSecretsBackend
    from cryptography.fernet import Fernet
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.url import URL

    from airflow_fernet_secrets.common.typeshed import PathType

    class BaseFernetLocalSecretsBackend(BaseSecretsBackend, LoggingMixin): ...

else:

    class BaseFernetLocalSecretsBackend(LoggingMixin): ...


__all__ = ["CommonFernetLocalSecretsBackend"]


class CommonFernetLocalSecretsBackend(BaseFernetLocalSecretsBackend):
    def __init__(
        self,
        *,
        secret_key: str | bytes | Fernet | None = None,
        backend_file_path: PathType | None = None,
    ) -> None:
        super().__init__()
        self.backend_file = backend_file_path

        self._secret_key = None if secret_key is None else ensure_fernet(secret_key)

    @cached_property
    def _backend_url(self) -> URL:
        if self.backend_file is not None:
            return create_sqlite_url(self.backend_file)
        file = load_backend_file(self.log)
        return create_sqlite_url(file)

    @cached_property
    def _backend_engine(self) -> Engine:
        return ensure_sqlite_engine(self._backend_url)

    def _secret(self) -> Fernet:
        if self._secret_key is not None:
            return self._secret_key
        return load_secret_key(self.log)

    @override
    def get_conn_value(self, conn_id: str) -> str | None:
        with enter_database(self._backend_engine) as session:
            value = FernetConnection.get(session, conn_id)
            if value is None:
                return None
            return value.encrypted.decode("utf-8")

    def set_conn_value(self, conn_id: str, value: str | bytes) -> None:
        secret_key = self._secret()
        with enter_database(self._backend_engine) as session:
            value = FernetConnection.encrypt(value, secret_key)
            connection = FernetConnection.get(session, conn_id)
            if connection is None:
                connection = FernetConnection(encrypted=value, conn_id=conn_id)
            else:
                connection.encrypted = value
            session.add(connection)
            session.commit()

    @override
    def deserialize_connection(self, conn_id: str, value: str) -> bytes:
        fernet = self._secret()
        return Encrypted.decrypt(value, fernet)

    def serialize_connection(self, conn_id: str, connection: Any) -> bytes:
        raise NotImplementedError

    @override
    def get_connection(self, conn_id: str) -> Any:
        value = self.get_conn_value(conn_id)
        if value is None:
            return None

        return self.deserialize_connection(conn_id, value)

    def set_connection(self, conn_id: str, connection: Any) -> None:
        value = self.serialize_connection(conn_id, connection)
        self.set_conn_value(conn_id, value)

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
