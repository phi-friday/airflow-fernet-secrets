from __future__ import annotations

import warnings

import pytest
import sqlalchemy as sa
from airflow.models.connection import Connection
from airflow.providers.common.sql.hooks.sql import DbApiHook
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL
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


def test_client_connection_touch(client_backend, default_conn_id):
    connection = client_backend.get_connection(default_conn_id)
    assert connection is not None
    assert isinstance(connection, URL)

    engine = sa.create_engine(connection)
    with Session(engine) as session:
        values = session.execute(sa.text("select 1")).all()

    assert values == [(1,)]


def test_server_connection_touch(server_backend, default_conn_id):
    connection = server_backend.get_connection(default_conn_id)
    assert connection is not None
    assert isinstance(connection, Connection)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        hook = connection.get_hook()
    assert isinstance(hook, DbApiHook)
    engine = hook.get_sqlalchemy_engine()
    assert isinstance(engine, Engine)
    with Session(engine) as session:
        values = session.execute(sa.text("select 1")).all()

    assert values == [(1,)]


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
