from __future__ import annotations

import json
from os import environ
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest
from cryptography.fernet import Fernet
from sqlalchemy.engine.url import URL

if TYPE_CHECKING:
    from airflow_fernet_secrets.secrets.client import ClientFernetLocalSecretsBackend
    from airflow_fernet_secrets.secrets.server import ServerFernetLocalSecretsBackend


def _set_backend_kwargs(key: str, value: Any) -> None:
    json_value = environ.get("AIRFLOW__SECRETS__BACKEND_KWARGS")

    kwargs: dict[str, Any]
    kwargs = json.loads(json_value) if json_value else {}
    kwargs[key] = value

    environ["AIRFLOW__SECRETS__BACKEND_KWARGS"] = json.dumps(kwargs)


@pytest.fixture(scope="session")
def _init_envs() -> None:
    environ["AIRFLOW__SECRETS__BACKEND"] = (
        "airflow.providers.fernet_secrets.secrets.secret_manager.FernetLocalSecretsBackend"
    )


@pytest.fixture(scope="session")
def temp_path():
    with TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture(scope="session")
def backend_path(temp_path: Path, _init_envs):
    from airflow_fernet_secrets.core.database import (
        create_sqlite_url,
        ensure_sqlite_engine,
    )
    from airflow_fernet_secrets.core.model import migrate

    file = temp_path / str(uuid4())
    file.touch()

    url = create_sqlite_url(file)
    engine = ensure_sqlite_engine(url)
    migrate(engine)

    _set_backend_kwargs("fernet_secrets_backend_file_path", str(file))

    return file


@pytest.fixture(scope="session")
def secret_key(_init_envs):
    key = Fernet.generate_key()
    _set_backend_kwargs("fernet_secrets_key", key.decode("utf-8"))
    return key


@pytest.fixture(scope="session")
def default_conn_id():
    return "default"


@pytest.fixture(scope="session")
def default_conn(temp_path: Path) -> URL:
    file = temp_path / str(uuid4())
    return URL.create("sqlite", database=str(file))


@pytest.fixture()
def client_backend(
    secret_key, backend_path, default_conn_id, default_conn
) -> ClientFernetLocalSecretsBackend:
    from airflow_fernet_secrets.secrets.client import ClientFernetLocalSecretsBackend

    backend = ClientFernetLocalSecretsBackend(
        fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
    )

    value = backend.get_conn_value(default_conn_id)
    if value is not None:
        return backend

    backend.set_connection(default_conn_id, None, default_conn)
    return backend


@pytest.fixture()
def server_backend(secret_key, backend_path) -> ServerFernetLocalSecretsBackend:
    from airflow_fernet_secrets.secrets.server import ServerFernetLocalSecretsBackend

    return ServerFernetLocalSecretsBackend(
        fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
    )
