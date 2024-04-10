from __future__ import annotations

import warnings
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Literal

import pytest
import sqlalchemy as sa
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from airflow.providers.common.sql.hooks.sql import DbApiHook
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Session

if TYPE_CHECKING:
    from airflow_fernet_secrets._typeshed import PathType
    from airflow_fernet_secrets.secrets.common import CommonFernetLocalSecretsBackend


@pytest.fixture()
def backend_class(
    request: pytest.FixtureRequest,
) -> type[CommonFernetLocalSecretsBackend]:
    side = request.param
    if side == "client":
        from airflow_fernet_secrets.secrets.client import (
            ClientFernetLocalSecretsBackend as FernetLocalSecretsBackend,
        )
    elif side == "server":
        from airflow_fernet_secrets.secrets.server import (
            ServerFernetLocalSecretsBackend as FernetLocalSecretsBackend,
        )
    else:
        raise NotImplementedError
    return FernetLocalSecretsBackend


@pytest.mark.parametrize("backend_class", ["client", "server"], indirect=True)
class BaseTestClientAndServer:
    @staticmethod
    def find_backend_side(
        backend_class: type[CommonFernetLocalSecretsBackend],
    ) -> Literal["client", "server"]:
        side = backend_class.__module__.split(".")[-1]
        if side == "client" or side == "server":  # noqa: PLR1714
            return side
        raise NotImplementedError

    @classmethod
    def backend(
        cls, backend_class: type[CommonFernetLocalSecretsBackend]
    ) -> CommonFernetLocalSecretsBackend:
        side = cls.find_backend_side(backend_class)
        if side == "client":
            from airflow_fernet_secrets.secrets.client import (
                ClientFernetLocalSecretsBackend as FernetLocalSecretsBackend,
            )
        elif side == "server":
            from airflow_fernet_secrets.secrets.server import (
                ServerFernetLocalSecretsBackend as FernetLocalSecretsBackend,
            )
        else:
            raise NotImplementedError
        return FernetLocalSecretsBackend()

    @classmethod
    def assert_connection_type(
        cls, backend_class: type[CommonFernetLocalSecretsBackend], connection: Any
    ) -> None:
        side = cls.find_backend_side(backend_class)
        if side == "client":
            assert isinstance(connection, URL)
        elif side == "server":
            assert isinstance(connection, Connection)
        else:
            raise NotImplementedError

    @classmethod
    def create_connection(
        cls,
        backend_class: type[CommonFernetLocalSecretsBackend],
        *,
        conn_id: str | None,
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
                conn_type="sqlite",
                host=str(file),
                extra=extra,
                **kwargs,
            )
        raise NotImplementedError

    @classmethod
    def dump_connection(
        cls, backend_class: type[CommonFernetLocalSecretsBackend], connection: Any
    ) -> str:
        side = cls.find_backend_side(backend_class)
        if side == "client":
            assert isinstance(connection, URL)
            return connection.render_as_string(hide_password=False)
        if side == "server":
            assert isinstance(connection, Connection)
            return connection.get_uri()
        raise NotImplementedError

    @classmethod
    def create_engine(
        cls, backend_class: type[CommonFernetLocalSecretsBackend], connection: Any
    ) -> Engine:
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


class TestSyncClientAndServer(BaseTestClientAndServer):
    def test_get_connection(
        self,
        backend_class: type[CommonFernetLocalSecretsBackend],
        secret_key,
        backend_path,
        default_conn_id,
    ):
        backend = backend_class(
            fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
        )
        conn_value = backend.get_conn_value(default_conn_id)
        assert conn_value is not None

        connection = backend.get_connection(default_conn_id)
        assert connection is not None
        self.assert_connection_type(backend_class, connection)

    def test_delete_connection(
        self,
        backend_class: type[CommonFernetLocalSecretsBackend],
        secret_key: bytes,
        backend_path: Path,
        temp_file: Path,
    ) -> None:
        backend = backend_class(
            fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
        )

        conn_id = temp_file.stem
        conn = backend.get_connection(conn_id)
        assert conn is None

        connection = self.create_connection(
            backend_class,
            conn_id=conn_id,
            file=temp_file,
            extra={"some_key": "some_value"},
        )
        with ignore_warnings():
            backend.set_connection(conn_id=conn_id, connection=connection)

        conn = backend.get_connection(conn_id)
        assert conn is not None

        backend.delete_connection(conn_id)
        conn = backend.get_connection(conn_id)
        assert conn is None

    def test_set_connection(
        self,
        backend_class: type[CommonFernetLocalSecretsBackend],
        secret_key,
        backend_path,
        temp_file,
    ):
        backend = backend_class(
            fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
        )

        conn_id = temp_file.stem
        old = self.create_connection(
            backend_class,
            conn_id=conn_id,
            file=temp_file,
            extra={"some_key": "some_value"},
        )
        with ignore_warnings():
            backend.set_connection(conn_id, old)
        new = backend.get_connection(conn_id)
        assert new is not None
        old_str = self.dump_connection(backend_class, old)
        new_str = self.dump_connection(backend_class, new)
        assert old_str == new_str

    def test_connection_touch(self, backend_class, default_conn_id):
        backend = self.backend(backend_class)
        connection = backend.get_connection(default_conn_id)
        assert connection is not None
        self.assert_connection_type(backend_class, connection)

        engine = self.create_engine(backend_class, connection)
        with Session(engine) as session:
            values = session.execute(sa.text("select 1")).all()

        assert values == [(1,)]


@pytest.mark.anyio()
class TestAsyncClientAndServer(BaseTestClientAndServer):
    async def test_aget_connection(
        self,
        backend_class: type[CommonFernetLocalSecretsBackend],
        secret_key,
        backend_path,
        default_conn_id,
    ):
        backend = backend_class(
            fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
        )
        conn_value = await backend.aget_conn_value(default_conn_id)
        assert conn_value is not None

        connection = await backend.aget_connection(default_conn_id)
        assert connection is not None
        self.assert_connection_type(backend_class, connection)

    async def test_adelete_connection(
        self,
        backend_class: type[CommonFernetLocalSecretsBackend],
        secret_key: bytes,
        backend_path: Path,
        temp_file: Path,
    ) -> None:
        backend = backend_class(
            fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
        )

        conn_id = temp_file.stem
        conn = await backend.aget_connection(conn_id)
        assert conn is None

        connection = self.create_connection(
            backend_class,
            conn_id=conn_id,
            file=temp_file,
            extra={"some_key": "some_value"},
        )
        await backend.aset_connection(conn_id=conn_id, connection=connection)

        conn = await backend.aget_connection(conn_id)
        assert conn is not None

        await backend.adelete_connection(conn_id)
        conn = await backend.aget_connection(conn_id)
        assert conn is None

    async def test_aset_connection(
        self,
        backend_class: type[CommonFernetLocalSecretsBackend],
        secret_key,
        backend_path,
        temp_file,
    ):
        backend = backend_class(
            fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
        )

        conn_id = temp_file.stem
        old = self.create_connection(
            backend_class,
            conn_id=conn_id,
            file=temp_file,
            extra={"some_key": "some_value"},
        )
        await backend.aset_connection(conn_id, old)
        new = await backend.aget_connection(conn_id)
        assert new is not None
        old_str = self.dump_connection(backend_class, old)
        new_str = self.dump_connection(backend_class, new)
        assert old_str == new_str

    async def test_connection_atouch(
        self,
        backend_class: type[CommonFernetLocalSecretsBackend],
        default_async_conn_id,
    ):
        side = self.find_backend_side(backend_class)
        if side == "server":
            pytest.skip()

        backend = self.backend(backend_class)
        connection = await backend.aget_connection(default_async_conn_id)
        assert connection is not None
        assert isinstance(connection, URL)

        engine = create_async_engine(connection)
        async with AsyncSession(engine) as session:
            fetch = await session.execute(sa.text("select 1"))
            values = fetch.all()

        assert values == [(1,)]


def test_server_to_client(server_backend, client_backend, temp_file):
    conn_id = temp_file.stem
    connection = Connection(
        conn_id=conn_id,
        conn_type="sqlite",
        host=str(temp_file),
        extra={"some_key": "some_value"},
    )
    server_backend.set_connection(conn_id=conn_id, connection=connection)
    hook = get_hook(connection)
    assert isinstance(hook, DbApiHook)
    server_url = hook.get_uri()

    connection = client_backend.get_connection(conn_id=conn_id)
    assert connection is not None
    assert isinstance(connection, URL)
    client_url = connection.render_as_string()
    assert server_url == client_url


""" airflow does not support async url
@pytest.mark.anyio()
async def test_server_ato_client(server_backend, client_backend, temp_file):
    ...
"""


def test_client_to_server(server_backend, client_backend, temp_file):
    conn_id = temp_file.stem
    client_url: str = URL.create(
        "sqlite", database=str(temp_file), query={"some_key": "some_value"}
    ).render_as_string()
    client_backend.set_connection(conn_id=conn_id, connection=client_url)

    connection = server_backend.get_connection(conn_id=conn_id)
    assert connection is not None
    hook = get_hook(connection)
    assert isinstance(hook, DbApiHook)
    server_url = hook.get_uri()
    assert server_url == client_url


""" airflow does not support async url
@pytest.mark.anyio()
def test_client_ato_server(server_backend, client_backend, temp_file):
    ...
"""


@contextmanager
def ignore_warnings() -> Generator[None, None, None]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        yield


def get_hook(connection: Connection) -> BaseHook:
    with ignore_warnings():
        return connection.get_hook()
