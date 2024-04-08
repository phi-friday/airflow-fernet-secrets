from __future__ import annotations

import warnings
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import pytest
import sqlalchemy as sa
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from airflow.providers.common.sql.hooks.sql import DbApiHook
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Session


@pytest.mark.parametrize("side", ["client", "server"])
def test_get_connection(
    side: str,
    secret_key,
    backend_path,
    default_conn_id,
    client_backend,  # noqa: ARG001 # init
):
    setup(is_server=side == "server")

    from airflow_fernet_secrets.secrets import FernetLocalSecretsBackend

    backend = FernetLocalSecretsBackend(
        fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
    )
    conn_value = backend.get_conn_value(default_conn_id)
    assert conn_value is not None

    connection = backend.get_connection(default_conn_id)
    assert connection is not None

    if side == "client":
        assert isinstance(connection, URL)
    elif side == "server":
        assert isinstance(connection, Connection)
    else:
        raise NotImplementedError


@pytest.mark.parametrize("side", ["client", "server"])
@pytest.mark.anyio()
async def test_aget_connection(
    side: str,
    secret_key,
    backend_path,
    default_conn_id,
    client_backend,  # noqa: ARG001 # init
):
    setup(is_server=side == "server")

    from airflow_fernet_secrets.secrets import FernetLocalSecretsBackend

    backend = FernetLocalSecretsBackend(
        fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
    )
    conn_value = await backend.aget_conn_value(default_conn_id)
    assert conn_value is not None

    connection = await backend.aget_connection(default_conn_id)
    assert connection is not None

    if side == "client":
        assert isinstance(connection, URL)
    elif side == "server":
        assert isinstance(connection, Connection)
    else:
        raise NotImplementedError


def test_set_client_connection(secret_key, backend_path, temp_file):
    setup(is_server=False)

    from airflow_fernet_secrets.core.database import create_sqlite_url
    from airflow_fernet_secrets.secrets.client import ClientFernetLocalSecretsBackend

    backend = ClientFernetLocalSecretsBackend(
        fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
    )

    conn_id = temp_file.stem
    old = create_sqlite_url(temp_file, is_async=False, query={"some_key": "some_value"})
    backend.set_connection(conn_id, old)
    new = backend.get_connection(conn_id)
    assert new is not None
    old_str = old.render_as_string(hide_password=False)
    new_str = new.render_as_string(hide_password=False)
    assert old_str == new_str


def test_set_server_connection(secret_key, backend_path, temp_file):
    setup(is_server=False)

    from airflow_fernet_secrets.secrets.server import ServerFernetLocalSecretsBackend

    backend = ServerFernetLocalSecretsBackend(
        fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
    )

    conn_id = temp_file.stem
    old = Connection(
        conn_id=conn_id,
        conn_type="sqlite",
        host=str(temp_file),
        extra={"some_key": "some_value"},
    )
    with ignore_warnings():
        backend.set_connection(conn_id, old)
    new = backend.get_connection(conn_id)
    assert new is not None
    old_str = old.get_uri()
    new_str = new.get_uri()
    assert old_str == new_str


@pytest.mark.anyio()
async def test_aset_client_connection(secret_key, backend_path, temp_file):
    setup(is_server=False)

    from airflow_fernet_secrets.core.database import create_sqlite_url
    from airflow_fernet_secrets.secrets.client import ClientFernetLocalSecretsBackend

    backend = ClientFernetLocalSecretsBackend(
        fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
    )

    conn_id = temp_file.stem
    old = create_sqlite_url(temp_file, is_async=False, query={"some_key": "some_value"})
    await backend.aset_connection(conn_id, old)
    new = await backend.aget_connection(conn_id)
    assert new is not None
    old_str = old.render_as_string(hide_password=False)
    new_str = new.render_as_string(hide_password=False)
    assert old_str == new_str


@pytest.mark.anyio()
async def test_aset_server_connection(secret_key, backend_path, temp_file):
    setup(is_server=False)

    from airflow_fernet_secrets.secrets.server import ServerFernetLocalSecretsBackend

    backend = ServerFernetLocalSecretsBackend(
        fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
    )

    conn_id = temp_file.stem
    old = Connection(
        conn_id=conn_id,
        conn_type="sqlite",
        host=str(temp_file),
        extra={"some_key": "some_value"},
    )
    with ignore_warnings():
        await backend.aset_connection(conn_id, old)
    new = await backend.aget_connection(conn_id)
    assert new is not None
    old_str = old.get_uri()
    new_str = new.get_uri()
    assert old_str == new_str


@pytest.mark.parametrize("side", ["client", "server"])
def test_delete_connection(
    side: str, secret_key: bytes, backend_path: Path, temp_file: Path
) -> None:
    setup(is_server=side == "server")

    from airflow_fernet_secrets.core.database import create_sqlite_url
    from airflow_fernet_secrets.secrets import FernetLocalSecretsBackend

    backend = FernetLocalSecretsBackend(
        fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
    )

    conn_id = temp_file.stem
    conn = backend.get_connection(conn_id)
    assert conn is None

    if side == "client":
        connection = create_sqlite_url(
            temp_file, is_async=False, query={"some_key": "some_value"}
        )
    elif side == "server":
        connection = Connection(
            conn_id=conn_id,
            conn_type="sqlite",
            host=str(temp_file),
            extra={"some_key": "some_value"},
        )
    else:
        raise NotImplementedError
    backend.set_connection(conn_id=conn_id, connection=connection)

    conn = backend.get_connection(conn_id)
    assert conn is not None

    backend.delete_connection(conn_id)
    conn = backend.get_connection(conn_id)
    assert conn is None


@pytest.mark.parametrize("side", ["client", "server"])
@pytest.mark.anyio()
async def test_adelete_connection(
    side: str, secret_key: bytes, backend_path: Path, temp_file: Path
) -> None:
    setup(is_server=side == "server")

    from airflow_fernet_secrets.core.database import create_sqlite_url
    from airflow_fernet_secrets.secrets import FernetLocalSecretsBackend

    backend = FernetLocalSecretsBackend(
        fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
    )

    conn_id = temp_file.stem
    conn = await backend.aget_connection(conn_id)
    assert conn is None

    if side == "client":
        connection = create_sqlite_url(
            temp_file, is_async=False, query={"some_key": "some_value"}
        )
    elif side == "server":
        connection = Connection(
            conn_id=conn_id,
            conn_type="sqlite",
            host=str(temp_file),
            extra={"some_key": "some_value"},
        )
    else:
        raise NotImplementedError
    await backend.aset_connection(conn_id=conn_id, connection=connection)

    conn = await backend.aget_connection(conn_id)
    assert conn is not None

    await backend.adelete_connection(conn_id)
    conn = await backend.aget_connection(conn_id)
    assert conn is None


def test_client_connection_touch(client_backend, default_conn_id):
    connection = client_backend.get_connection(default_conn_id)
    assert connection is not None
    assert isinstance(connection, URL)

    engine = sa.create_engine(connection)
    with Session(engine) as session:
        values = session.execute(sa.text("select 1")).all()

    assert values == [(1,)]


@pytest.mark.anyio()
async def test_client_connection_atouch(client_backend, default_async_conn_id):
    connection = await client_backend.aget_connection(default_async_conn_id)
    assert connection is not None
    assert isinstance(connection, URL)

    engine = create_async_engine(connection)
    async with AsyncSession(engine) as session:
        fetch = await session.execute(sa.text("select 1"))
        values = fetch.all()

    assert values == [(1,)]


def test_server_connection_touch(server_backend, default_conn_id):
    connection = server_backend.get_connection(default_conn_id)
    assert connection is not None
    assert isinstance(connection, Connection)

    hook = get_hook(connection)
    assert isinstance(hook, DbApiHook)
    engine = hook.get_sqlalchemy_engine()
    assert isinstance(engine, Engine)
    with Session(engine) as session:
        values = session.execute(sa.text("select 1")).all()

    assert values == [(1,)]


""" airflow does not support async url
@pytest.mark.anyio()
def test_server_connection_atouch(server_backend, default_conn_id): ...
"""


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


def setup(*, is_server: bool) -> None:
    import os

    from airflow_fernet_secrets.core.config import const
    from airflow_fernet_secrets.core.utils.reload import reload

    key = (const.CLIENT_ENV_PREFIX + const.ENV_IS_SERVER).upper()
    os.environ[key] = str(is_server)
    reload()

    from airflow_fernet_secrets.core.config import IS_SERVER_FLAG
    from airflow_fernet_secrets.secrets import FernetLocalSecretsBackend

    assert IS_SERVER_FLAG is is_server
    assert (
        FernetLocalSecretsBackend.__module__.split(".")[-1] == "server"
        if is_server
        else "client"
    )


@contextmanager
def ignore_warnings() -> Generator[None, None, None]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        yield


def get_hook(connection: Connection) -> BaseHook:
    with ignore_warnings():
        return connection.get_hook()
