from __future__ import annotations

import json
import warnings
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Generator, Literal

import sqlalchemy as sa
from airflow.models.connection import Connection
from airflow.providers.common.sql.hooks.sql import DbApiHook
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL

from airflow_fernet_secrets.connection.server import convert_connection_to_dict
from airflow_fernet_secrets.utils.re import camel_to_snake

if TYPE_CHECKING:
    from airflow.hooks.base import BaseHook
    from typing_extensions import TypeAlias

    from airflow_fernet_secrets._typeshed import PathType
    from airflow_fernet_secrets.secrets.common import CommonFernetLocalSecretsBackend

BackendType: TypeAlias = "type[CommonFernetLocalSecretsBackend]"


class BaseTestClientAndServer:
    @staticmethod
    def find_backend_side(
        backend_class: BackendType,
    ) -> Literal["client", "server", "direct"]:
        side = backend_class.__module__.split(".")[-1]
        if side == "server":
            return side
        if side != "client":
            raise NotImplementedError

        name = camel_to_snake(backend_class.__qualname__)
        prefix = name.split("_", 1)[0]
        if prefix == "client" or prefix == "direct":  # noqa: PLR1714
            return prefix

        raise NotImplementedError

    @classmethod
    def backend(cls, backend_class: BackendType) -> CommonFernetLocalSecretsBackend:
        side = cls.find_backend_side(backend_class)
        if side == "client":
            from airflow_fernet_secrets.secrets.client import (
                ClientFernetLocalSecretsBackend as FernetLocalSecretsBackend,
            )
        elif side == "server":
            from airflow_fernet_secrets.secrets.server import (
                ServerFernetLocalSecretsBackend as FernetLocalSecretsBackend,
            )
        elif side == "direct":
            from airflow_fernet_secrets.secrets.client import (
                DirectFernetLocalSecretsBackend as FernetLocalSecretsBackend,
            )
        else:
            raise NotImplementedError
        return FernetLocalSecretsBackend()

    @classmethod
    def assert_connection_type(
        cls, backend_class: BackendType, connection: Any
    ) -> None:
        side = cls.find_backend_side(backend_class)
        if side == "client":
            assert isinstance(connection, URL)
        elif side == "server":
            assert isinstance(connection, Connection)
        elif side == "direct":
            assert isinstance(connection, dict)
        else:
            raise NotImplementedError

    @classmethod
    def create_connection(
        cls,
        backend_class: BackendType,
        *,
        conn_id: str | None,
        conn_type: str = "sqlite",
        file: PathType,
        extra: dict[str, Any] | None = None,
        is_async: bool = False,
        **kwargs: Any,
    ) -> Any:
        side = cls.find_backend_side(backend_class)
        if side == "client":
            from airflow_fernet_secrets.database.connect import create_sqlite_url

            return create_sqlite_url(file, is_async=is_async, query=extra, **kwargs)
        if side == "server":
            return Connection(
                conn_id=conn_id,
                conn_type=conn_type,
                host=str(file),
                extra=extra,
                **kwargs,
            )
        if side == "direct":
            connection = Connection(
                conn_id=conn_id,
                conn_type=conn_type,
                host=str(file),
                extra=extra,
                **kwargs,
            )
            return convert_connection_to_dict(connection)
        raise NotImplementedError

    @classmethod
    def dump_connection(cls, backend_class: BackendType, connection: Any) -> str:
        side = cls.find_backend_side(backend_class)
        if side == "client":
            assert isinstance(connection, URL)
            return connection.render_as_string(hide_password=False)
        if side == "server":
            assert isinstance(connection, Connection)
            return connection.get_uri()
        if side == "direct":
            assert isinstance(connection, dict)
            return json.dumps(connection)
        raise NotImplementedError

    @classmethod
    def create_engine(cls, backend_class: BackendType, connection: Any) -> Engine:
        side = cls.find_backend_side(backend_class)
        if side == "client":
            assert isinstance(connection, URL)
            return sa.create_engine(connection)
        if side == "server":
            hook = get_hook(connection)
            assert isinstance(hook, DbApiHook)
            engine = hook.get_sqlalchemy_engine()
            assert isinstance(engine, Engine)
            return engine
        raise NotImplementedError


@contextmanager
def ignore_warnings() -> Generator[None, None, None]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        yield


def get_hook(connection: Connection) -> BaseHook:
    with ignore_warnings():
        return connection.get_hook()
