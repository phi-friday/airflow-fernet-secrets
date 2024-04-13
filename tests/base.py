from __future__ import annotations

import json
import warnings
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Generator, Literal

import pytest
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

BackendType: TypeAlias = "type[CommonFernetLocalSecretsBackend[Any]]"
SideLiteral = Literal["client", "server", "direct"]


class BaseTestClientAndServer:
    @pytest.fixture(scope="class", autouse=True)
    def _init_backend(
        self,
        request: pytest.FixtureRequest,
        backend_class: BackendType,
        secret_key,
        backend_path,
    ) -> None:
        cls = request.cls
        assert cls is not None

        self._backend: CommonFernetLocalSecretsBackend[Any]
        self._side: SideLiteral
        self._backend = cls._backend = backend_class(
            fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
        )
        self._side = cls._side = self.find_backend_side(backend_class)

    @property
    def backend(self) -> CommonFernetLocalSecretsBackend[Any]:
        return self._backend

    @property
    def side(self) -> SideLiteral:
        return self._side

    @staticmethod
    def find_backend_side(backend_class: BackendType) -> SideLiteral:
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

    def assert_connection_type(self, connection: Any) -> None:
        if self.side == "client":
            assert isinstance(connection, URL)
        elif self.side == "server":
            assert isinstance(connection, Connection)
        elif self.side == "direct":
            assert isinstance(connection, dict)
        else:
            raise NotImplementedError

    def create_connection(
        self,
        *,
        conn_id: str | None,
        conn_type: str = "sqlite",
        file: PathType,
        extra: dict[str, Any] | None = None,
        is_async: bool = False,
        **kwargs: Any,
    ) -> Any:
        if self.side == "client":
            from airflow_fernet_secrets.database.connect import create_sqlite_url

            return create_sqlite_url(file, is_async=is_async, query=extra, **kwargs)
        if self.side == "server":
            return Connection(
                conn_id=conn_id,
                conn_type=conn_type,
                host=str(file),
                extra=extra,
                **kwargs,
            )
        if self.side == "direct":
            connection = Connection(
                conn_id=conn_id,
                conn_type=conn_type,
                host=str(file),
                extra=extra,
                **kwargs,
            )
            return convert_connection_to_dict(connection)
        raise NotImplementedError

    def dump_connection(self, connection: Any) -> str:
        if self.side == "client":
            assert isinstance(connection, URL)
            return connection.render_as_string(hide_password=False)
        if self.side == "server":
            assert isinstance(connection, Connection)
            return connection.get_uri()
        if self.side == "direct":
            assert isinstance(connection, dict)
            return json.dumps(connection)
        raise NotImplementedError

    def create_engine(self, connection: Any) -> Engine:
        if self.side == "client":
            assert isinstance(connection, URL)
            return sa.create_engine(connection)
        if self.side == "server":
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
