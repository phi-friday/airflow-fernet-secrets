from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING

from airflow.models.connection import Connection
from airflow.secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin
from typing_extensions import override

from airflow_fernet_secrets.common.config.common import ensure_fernet
from airflow_fernet_secrets.common.config.server import (
    load_connections_file,
    load_secret_key,
    load_variables_file,
)
from airflow_fernet_secrets.common.database import (
    create_sqlite_url,
    ensure_sqlite_engine,
    enter_database,
)
from airflow_fernet_secrets.common.model import Connection as FernetConnection
from airflow_fernet_secrets.common.model import Encrypted
from airflow_fernet_secrets.common.model import Variable as FernetVariable

if TYPE_CHECKING:
    from _typeshed import StrOrBytesPath
    from cryptography.fernet import Fernet
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.url import URL


class FernetLocalSecretsBackend(BaseSecretsBackend, LoggingMixin):
    def __init__(
        self,
        *,
        secret_key: str | bytes | Fernet | None = None,
        variables_file_path: StrOrBytesPath | None = None,
        connections_file_path: StrOrBytesPath | None = None,
    ) -> None:
        super().__init__()
        self.variables_file = variables_file_path
        self.connections_file = connections_file_path

        self._secret_key = None if secret_key is None else ensure_fernet(secret_key)

    @cached_property
    def _variables_url(self) -> URL:
        if self.variables_file is not None:
            return create_sqlite_url(self.variables_file)
        file = load_variables_file(self.log)
        return create_sqlite_url(file)

    @cached_property
    def _variables_engine(self) -> Engine:
        return ensure_sqlite_engine(self._variables_url)

    @cached_property
    def _connections_url(self) -> URL:
        if self.connections_file is not None:
            return create_sqlite_url(self.connections_file)
        file = load_connections_file(self.log)
        return create_sqlite_url(file)

    @cached_property
    def _connections_engine(self) -> Engine:
        return ensure_sqlite_engine(self._connections_url)

    def _secret(self) -> Fernet:
        if self._secret_key is not None:
            return self._secret_key
        return load_secret_key(self.log)

    @override
    def get_conn_value(self, conn_id: str) -> str | None:
        with enter_database(self._connections_engine) as session:
            value = FernetConnection.get(session, conn_id)
            if value is None:
                return None
            return value.encrypted.decode("utf-8")

    @override
    def deserialize_connection(self, conn_id: str, value: str) -> Connection:
        fernet = self._secret()
        data = Encrypted.decrypt(value, fernet)
        return Connection.from_json(data, conn_id)

    @override
    def get_variable(self, key: str) -> str | None:
        with enter_database(self._variables_engine) as session:
            value = FernetVariable.get(session, key)
            if value is None:
                return None
            session.expunge(value)

        fernet = self._secret()
        return FernetVariable.decrypt(value.encrypted, fernet)
