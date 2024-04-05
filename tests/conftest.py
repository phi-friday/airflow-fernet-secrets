from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import pytest
from cryptography.fernet import Fernet
from sqlalchemy.engine.url import URL


@pytest.fixture(scope="session")
def temp_path():
    with TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture(scope="session")
def backend_path(temp_path: Path):
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

    return file


@pytest.fixture(scope="session")
def secret_key():
    return Fernet.generate_key()


@pytest.fixture(scope="session")
def default_conn_id():
    return "default"


@pytest.fixture(scope="session")
def default_conn(temp_path: Path) -> URL:
    file = temp_path / str(uuid4())
    return URL.create("sqlite", database=str(file))


@pytest.fixture()
def client_backend(secret_key, backend_path, default_conn_id, default_conn):
    from airflow_fernet_secrets.secrets.client import FernetLocalSecretsBackend

    backend = FernetLocalSecretsBackend(
        secret_key=secret_key, backend_file_path=backend_path
    )

    value = backend.get_conn_value(default_conn_id)
    if value is not None:
        return value

    backend.set_connection(default_conn_id, None, default_conn)
    return backend


@pytest.fixture()
def server_backend(secret_key, backend_path):
    from airflow_fernet_secrets.secrets.server import FernetLocalSecretsBackend

    return FernetLocalSecretsBackend(
        secret_key=secret_key, backend_file_path=backend_path
    )
