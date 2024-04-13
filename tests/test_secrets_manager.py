from __future__ import annotations

from pathlib import Path

import pytest
import sqlalchemy as sa
from airflow.hooks.filesystem import FSHook
from airflow.models.connection import Connection
from airflow.providers.common.sql.hooks.sql import DbApiHook
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Session

from tests.base import BaseTestClientAndServer, get_hook, ignore_warnings


@pytest.mark.parametrize("backend_class", ["client", "server"], indirect=True)
class TestSyncClientAndServer(BaseTestClientAndServer):
    def test_get_connection(self, default_conn_id):
        conn_value = self.backend.get_conn_value(default_conn_id)
        assert conn_value is not None

        connection = self.backend.get_connection(default_conn_id)
        assert connection is not None
        self.assert_connection_type(connection)

    def test_delete_connection(self, temp_file: Path) -> None:
        conn_id = temp_file.stem
        conn = self.backend.get_connection(conn_id)
        assert conn is None

        connection = self.create_connection(
            conn_id=conn_id, file=temp_file, extra={"some_key": "some_value"}
        )
        with ignore_warnings():
            self.backend.set_connection(conn_id=conn_id, connection=connection)

        conn = self.backend.get_connection(conn_id)
        assert conn is not None

        self.backend.delete_connection(conn_id)
        conn = self.backend.get_connection(conn_id)
        assert conn is None

    def test_set_connection(self, temp_file):
        conn_id = temp_file.stem
        old = self.create_connection(
            conn_id=conn_id, file=temp_file, extra={"some_key": "some_value"}
        )
        with ignore_warnings():
            self.backend.set_connection(conn_id, old)
        new = self.backend.get_connection(conn_id)
        assert new is not None
        old_str = self.dump_connection(old)
        new_str = self.dump_connection(new)
        assert old_str == new_str

    def test_connection_touch(self, default_conn_id):
        connection = self.backend.get_connection(default_conn_id)
        assert connection is not None
        self.assert_connection_type(connection)

        engine = self.create_engine(connection)
        with Session(engine) as session:
            values = session.execute(sa.text("select 1")).all()

        assert values == [(1,)]

    def test_has_connection(self, default_conn_id, temp_file):
        check = self.backend.has_connection(default_conn_id)
        assert check is True
        conn = self.backend.get_connection(default_conn_id)
        assert conn is not None

        conn_id = temp_file.stem
        check = self.backend.has_connection(conn_id)
        assert check is False
        conn = self.backend.get_connection(conn_id)
        assert conn is None


@pytest.mark.parametrize("backend_class", ["client", "server"], indirect=True)
@pytest.mark.anyio()
class TestAsyncClientAndServer(BaseTestClientAndServer):
    async def test_aget_connection(self, default_conn_id):
        conn_value = await self.backend.aget_conn_value(default_conn_id)
        assert conn_value is not None

        connection = await self.backend.aget_connection(default_conn_id)
        assert connection is not None
        self.assert_connection_type(connection)

    async def test_adelete_connection(self, temp_file: Path) -> None:
        conn_id = temp_file.stem
        conn = await self.backend.aget_connection(conn_id)
        assert conn is None

        connection = self.create_connection(
            conn_id=conn_id, file=temp_file, extra={"some_key": "some_value"}
        )
        await self.backend.aset_connection(conn_id=conn_id, connection=connection)

        conn = await self.backend.aget_connection(conn_id)
        assert conn is not None

        await self.backend.adelete_connection(conn_id)
        conn = await self.backend.aget_connection(conn_id)
        assert conn is None

    async def test_aset_connection(self, temp_file):
        conn_id = temp_file.stem
        old = self.create_connection(
            conn_id=conn_id, file=temp_file, extra={"some_key": "some_value"}
        )
        await self.backend.aset_connection(conn_id, old)
        new = await self.backend.aget_connection(conn_id)
        assert new is not None
        old_str = self.dump_connection(old)
        new_str = self.dump_connection(new)
        assert old_str == new_str

    async def test_connection_atouch(self, default_async_conn_id):
        if self.side == "server":
            pytest.skip()

        connection = await self.backend.aget_connection(default_async_conn_id)
        assert connection is not None
        assert isinstance(connection, URL)

        engine = create_async_engine(connection)
        async with AsyncSession(engine) as session:
            fetch = await session.execute(sa.text("select 1"))
            values = fetch.all()

        assert values == [(1,)]

    async def test_ahas_connection(self, default_conn_id, temp_file):
        check = await self.backend.ahas_connection(default_conn_id)
        assert check is True
        conn = await self.backend.aget_connection(default_conn_id)
        assert conn is not None

        conn_id = temp_file.stem
        check = await self.backend.ahas_connection(conn_id)
        assert check is False
        conn = await self.backend.aget_connection(conn_id)
        assert conn is None


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


def test_filesystem_connection(server_backend, temp_file):
    conn_id = temp_file.stem
    connection = Connection(
        conn_id=conn_id, conn_type="fs", extra={"path": str(temp_file)}
    )
    server_backend.set_connection(conn_id=conn_id, connection=connection)

    load = server_backend.get_connection(conn_id=conn_id)
    assert load is not None
    assert load.conn_type == "fs"

    hook = load.get_hook()
    assert isinstance(hook, FSHook)
    path = hook.get_path()
    assert path == str(temp_file)
