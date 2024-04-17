from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING, Literal

from sqlalchemy.engine.url import make_url
from typing_extensions import TypeAlias, override

from airflow_fernet_secrets import const
from airflow_fernet_secrets.config.client import load_backend_file as _load_backend_file
from airflow_fernet_secrets.config.client import load_secret_key as _load_secret_key
from airflow_fernet_secrets.connection import (
    ConnectionArgs,
    ConnectionDict,
    convert_args_from_jsonable,
    convert_args_to_jsonable,
)
from airflow_fernet_secrets.connection.client import (
    convert_connectable_to_dict,
    create_url,
)
from airflow_fernet_secrets.log.client import LoggingMixin
from airflow_fernet_secrets.secrets.common import (
    CommonFernetLocalSecretsBackend as _CommonFernetLocalSecretsBackend,
)

if TYPE_CHECKING:
    from airflow_fernet_secrets.database.model import Connection

__all__ = ["ClientFernetLocalSecretsBackend", "DirectFernetLocalSecretsBackend"]

ConnectionType: TypeAlias = "ConnectionArgs"


class ClientFernetLocalSecretsBackend(
    _CommonFernetLocalSecretsBackend[ConnectionType], LoggingMixin
):
    load_backend_file = staticmethod(_load_backend_file)
    load_secret_key = staticmethod(_load_secret_key)

    @override
    def _get_conn_type(self, connection: ConnectionType) -> str:
        return const.SQL_CONN_TYPE

    @override
    def _deserialize_connection(
        self, conn_id: str, connection: ConnectionDict
    ) -> ConnectionType:
        url = create_url(connection)

        args = connection["args"] or {}
        connect_args = args.get("connect_args") or {}
        engine_kwargs = args.get("engine_kwargs") or {}

        connection_args: ConnectionArgs = {
            "url": url,
            "connect_args": connect_args,
            "engine_kwargs": engine_kwargs,
        }

        return convert_args_from_jsonable(connection_args)

    @override
    def _serialize_connection(
        self, conn_id: str, connection: ConnectionType
    ) -> ConnectionDict:
        url = connection["url"]
        as_dict = convert_connectable_to_dict(url)
        as_dict["args"] = convert_args_to_jsonable(connection)
        return as_dict

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
        args = connection["args"]
        if args is None:
            raise NotImplementedError
        url = args["url"]
        make_url(url)
        return connection


class DirectFernetLocalSecretsBackend(
    _CommonFernetLocalSecretsBackend[ConnectionDict], LoggingMixin
):
    load_backend_file = staticmethod(_load_backend_file)
    load_secret_key = staticmethod(_load_secret_key)

    @override
    def _get_conn_type(self, connection: ConnectionDict) -> str:
        args = connection["args"]
        if args is not None:
            with suppress(Exception):
                make_url(args["url"])
                return const.SQL_CONN_TYPE

        conn_type = connection.get("conn_type")
        if conn_type is not None:
            return conn_type

        raise NotImplementedError

    @override
    def _deserialize_connection(
        self, conn_id: str, connection: ConnectionDict
    ) -> ConnectionDict:
        return connection

    @override
    def _serialize_connection(
        self, conn_id: str, connection: ConnectionDict
    ) -> ConnectionDict:
        args = connection["args"]
        if args is not None:
            connection["args"] = convert_args_to_jsonable(args)
        return connection
