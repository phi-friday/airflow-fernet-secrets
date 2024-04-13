from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from typing_extensions import TypeAlias, override

from airflow_fernet_secrets import const
from airflow_fernet_secrets.config.client import load_backend_file as _load_backend_file
from airflow_fernet_secrets.config.client import load_secret_key as _load_secret_key
from airflow_fernet_secrets.connection import ConnectionDict, parse_driver
from airflow_fernet_secrets.connection.client import (
    convert_connectable_to_dict,
    create_url,
)
from airflow_fernet_secrets.log.client import LoggingMixin
from airflow_fernet_secrets.secrets.common import (
    CommonFernetLocalSecretsBackend as _CommonFernetLocalSecretsBackend,
)

if TYPE_CHECKING:
    from sqlalchemy.engine.url import URL

    from airflow_fernet_secrets.database.model import Connection

__all__ = ["ClientFernetLocalSecretsBackend"]

ConnectionType: TypeAlias = "URL"


class ClientFernetLocalSecretsBackend(
    _CommonFernetLocalSecretsBackend[ConnectionType], LoggingMixin
):
    load_backend_file = staticmethod(_load_backend_file)
    load_secret_key = staticmethod(_load_secret_key)

    @override
    def _get_conn_type(self, connection: URL) -> str:
        return const.SQL_CONN_TYPE

    @override
    def _deserialize_connection(
        self, conn_id: str, connection: ConnectionDict
    ) -> ConnectionType:
        return create_url(connection)

    @override
    def _serialize_connection(
        self, conn_id: str, connection: ConnectionType
    ) -> ConnectionDict:
        return convert_connectable_to_dict(connection)

    @override
    def _validate_connection(
        self, conn_id: str, connection: Connection, when: Literal["get", "set"]
    ) -> Connection:
        connection = super()._validate_connection(
            conn_id=conn_id, connection=connection, when=when
        )
        if when == "set":
            return connection
        if not connection.is_sql_connection:
            raise NotImplementedError
        return connection

    @override
    def _validate_connection_dict(
        self,
        conn_id: str,
        connection: ConnectionDict,
        when: Literal["serialize", "deserialize"],
    ) -> ConnectionDict:
        connection = super()._validate_connection_dict(conn_id, connection, when)
        if when == "set":
            return connection
        driver = parse_driver(connection["driver"])
        if not driver.backend:
            raise NotImplementedError
        return connection
