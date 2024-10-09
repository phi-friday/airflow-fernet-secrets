from __future__ import annotations

from typing import TYPE_CHECKING

from airflow_fernet_secrets import exceptions as fe
from airflow_fernet_secrets.const import MSSQL_CONN_TYPES as _MSSQL_CONN_TYPES
from airflow_fernet_secrets.const import ODBC_CONN_TYPES as _ODBC_CONN_TYPES
from airflow_fernet_secrets.const import POSTGRESQL_CONN_TYPES as _POSTGRESQL_CONN_TYPES
from airflow_fernet_secrets.const import SQLITE_CONN_TYPES as _SQLITE_CONN_TYPES

if TYPE_CHECKING:
    from airflow.models.connection import Connection

    from airflow_fernet_secrets.typings import ConnectionArgs

__all__ = ["connection_to_args"]


def connection_to_args(connection: Connection) -> ConnectionArgs:
    """get sqlalchemy url, connect_args, engine_kwargs from airflow connection"""
    if not isinstance(connection.conn_type, str):
        error_msg = (
            f"invalid conn_type attribute type: {type(connection.conn_type).__name__}"
        )
        raise fe.FernetSecretsTypeError(error_msg)

    conn_type = connection.conn_type
    if conn_type in _SQLITE_CONN_TYPES:
        from airflow_fernet_secrets.connection.dump.sqlite import (
            connection_to_args as _connection_to_args,
        )

        return _connection_to_args(connection)

    if conn_type in _POSTGRESQL_CONN_TYPES:
        from airflow_fernet_secrets.connection.dump.postgresql import (
            connection_to_args as _connection_to_args,
        )

        return _connection_to_args(connection)
    if conn_type in _ODBC_CONN_TYPES:
        from airflow_fernet_secrets.connection.dump.odbc import (
            connection_to_args as _connection_to_args,
        )

        return _connection_to_args(connection)

    if conn_type in _MSSQL_CONN_TYPES:
        from airflow_fernet_secrets.connection.dump.mssql import (
            connection_to_args as _connection_to_args,
        )

        return _connection_to_args(connection)

    raise fe.FernetSecretsNotImplementedError(conn_type)
