from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from sqlalchemy.engine.url import URL
from typing_extensions import TypeAlias, override

from airflow_fernet_secrets.connection import ConnectionDict, parse_driver
from airflow_fernet_secrets.connection.client import (
    convert_connectable_to_dict,
    create_url,
)
from airflow_fernet_secrets.secrets.common import (
    CommonFernetLocalSecretsBackend as _CommonFernetLocalSecretsBackend,
)

if TYPE_CHECKING:
    from airflow_fernet_secrets.core.model import Connection

__all__ = ["FernetLocalSecretsBackend"]

ConnectionType: TypeAlias = URL


class FernetLocalSecretsBackend(_CommonFernetLocalSecretsBackend[ConnectionType]):
    @override
    def set_connection(
        self, conn_id: str, conn_type: str | None, connection: ConnectionType
    ) -> None:
        return super().set_connection(
            conn_id=conn_id, conn_type="sql", connection=connection
        )

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
        if (
            connection.conn_type is None
            or connection.conn_type.lower().strip() != "sql"
        ):
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
