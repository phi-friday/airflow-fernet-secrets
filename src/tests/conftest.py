# pyright: reportUnusedFunction=false
from __future__ import annotations

import json
import sys
import warnings
from os import environ
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import filelock
import pytest
from cryptography.fernet import Fernet

if TYPE_CHECKING:
    from airflow_fernet_secrets.secrets.client import ClientFernetLocalSecretsBackend
    from airflow_fernet_secrets.secrets.common import CommonFernetLocalSecretsBackend
    from airflow_fernet_secrets.secrets.server import ServerFernetLocalSecretsBackend
    from airflow_fernet_secrets.typings import ConnectionArgs

AIRFLOW_SERVER_FLAG = sys.version_info < (3, 13)


def _set_backend_kwargs(key: str, value: Any) -> None:
    json_value = environ.get("AIRFLOW__SECRETS__BACKEND_KWARGS")

    kwargs: dict[str, Any]
    kwargs = json.loads(json_value) if json_value else {}
    kwargs[key] = value

    environ["AIRFLOW__SECRETS__BACKEND_KWARGS"] = json.dumps(kwargs)


@pytest.fixture(
    params=[
        pytest.param(("asyncio", {"use_uvloop": False}), id="asyncio"),
        pytest.param(("asyncio", {"use_uvloop": True}), id="asyncio-uvloop"),
    ]
)
def anyio_backend(request) -> tuple[str, dict[str, Any]]:
    return request.param


@pytest.fixture(scope="session", autouse=True)
def _init_envs() -> None:
    environ["AIRFLOW__SECRETS__BACKEND"] = (
        "airflow.providers.fernet_secrets.secrets.secret_manager.FernetLocalSecretsBackend"
    )


@pytest.fixture(scope="session", autouse=True)
def _init_database(tmp_path_factory: pytest.TempPathFactory, worker_id) -> None:
    if not AIRFLOW_SERVER_FLAG:
        return

    from airflow.utils.db import initdb

    if worker_id == "master":
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            initdb()
        return

    root_tmp_dir = tmp_path_factory.getbasetemp().parent
    flag = root_tmp_dir / "initdb.flag"
    lockfile = flag.with_suffix(".lock")
    with filelock.FileLock(lockfile):
        if flag.exists():
            return
        flag.touch()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            initdb()


@pytest.fixture(scope="session")
def temp_path(tmp_path_factory: pytest.TempPathFactory):
    return tmp_path_factory.getbasetemp().parent


@pytest.fixture(scope="session", autouse=True)
def backend_path(tmp_path_factory: pytest.TempPathFactory, worker_id, temp_path: Path):
    from airflow_fernet_secrets import const
    from airflow_fernet_secrets.database.connect import (
        create_sqlite_url,
        ensure_sqlite_sync_engine,
    )
    from airflow_fernet_secrets.database.model import migrate

    if worker_id == "master":
        file = temp_path / str(uuid4())
        file.touch()

        url = create_sqlite_url(file)
        engine = ensure_sqlite_sync_engine(url)
        migrate(engine)

        _set_backend_kwargs("fernet_secrets_backend_file_path", str(file))
        environ.setdefault(
            const.CLIENT_ENV_PREFIX + const.ENV_BACKEND_FILE.upper(), str(file)
        )

        return file

    root_tmp_dir = tmp_path_factory.getbasetemp().parent
    flag = root_tmp_dir / "backend.flag"
    lockfile = flag.with_suffix(".lock")
    with filelock.FileLock(lockfile):
        if flag.exists():
            with flag.open() as f:
                file = Path(f.read())

            _set_backend_kwargs("fernet_secrets_backend_file_path", str(file))
            environ.setdefault(
                const.CLIENT_ENV_PREFIX + const.ENV_BACKEND_FILE.upper(), str(file)
            )

            return file

        file = temp_path / str(uuid4())
        with flag.open("w+") as f:
            f.write(str(file))

        url = create_sqlite_url(file)
        engine = ensure_sqlite_sync_engine(url)
        migrate(engine)

        _set_backend_kwargs("fernet_secrets_backend_file_path", str(file))
        environ.setdefault(
            const.CLIENT_ENV_PREFIX + const.ENV_BACKEND_FILE.upper(), str(file)
        )

        return file


@pytest.fixture(scope="session", autouse=True)
def secret_key(tmp_path_factory: pytest.TempPathFactory, worker_id):
    from airflow_fernet_secrets import const

    if worker_id == "master":
        key = Fernet.generate_key()
        _set_backend_kwargs("fernet_secrets_key", key.decode("utf-8"))
        environ.setdefault(
            const.CLIENT_ENV_PREFIX + const.ENV_SECRET_KEY.upper(), key.decode("utf-8")
        )
        return key

    root_tmp_dir = tmp_path_factory.getbasetemp().parent
    flag = root_tmp_dir / "key.flag"
    lockfile = flag.with_suffix(".lock")
    with filelock.FileLock(lockfile):
        if flag.exists():
            with flag.open("rb") as f:
                key = f.read()
            _set_backend_kwargs("fernet_secrets_key", key.decode("utf-8"))
            environ.setdefault(
                const.CLIENT_ENV_PREFIX + const.ENV_SECRET_KEY.upper(),
                key.decode("utf-8"),
            )
            return key

        key = Fernet.generate_key()
        with flag.open("wb+") as f:
            f.write(key)
        _set_backend_kwargs("fernet_secrets_key", key.decode("utf-8"))
        environ.setdefault(
            const.CLIENT_ENV_PREFIX + const.ENV_SECRET_KEY.upper(), key.decode("utf-8")
        )
        return key


@pytest.fixture(scope="session")
def default_conn_id():
    return "default"


@pytest.fixture(scope="session")
def default_async_conn_id(default_conn_id):
    return f"{default_conn_id}-async"


@pytest.fixture(scope="session")
def default_conn(temp_path: Path) -> ConnectionArgs:
    from sqlalchemy.engine.url import URL

    file = temp_path / str(uuid4())
    url = URL.create("sqlite", database=str(file))
    return {"url": url, "connect_args": {}, "engine_kwargs": {}}


@pytest.fixture(scope="session")
def default_async_conn(default_conn: ConnectionArgs) -> ConnectionArgs:
    from sqlalchemy.engine.url import make_url

    url = make_url(default_conn["url"])
    url = url.set(drivername="sqlite+aiosqlite")
    return {
        "url": url,
        "connect_args": default_conn["connect_args"],
        "engine_kwargs": default_conn["engine_kwargs"],
    }


@pytest.fixture(scope="session", autouse=True)
def _init_connections(
    secret_key,
    backend_path,
    default_conn_id,
    default_conn,
    default_async_conn_id,
    default_async_conn,
) -> None:
    from airflow_fernet_secrets.secrets.client import ClientFernetLocalSecretsBackend

    backend = ClientFernetLocalSecretsBackend(
        fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
    )

    backend.set_connection(default_conn_id, default_conn)
    backend.set_connection(default_async_conn_id, default_async_conn)


@pytest.fixture
def client_backend(secret_key, backend_path) -> ClientFernetLocalSecretsBackend:
    from airflow_fernet_secrets.secrets.client import ClientFernetLocalSecretsBackend

    return ClientFernetLocalSecretsBackend(
        fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
    )


@pytest.fixture
def server_backend(secret_key, backend_path) -> ServerFernetLocalSecretsBackend:
    from airflow_fernet_secrets.secrets.server import ServerFernetLocalSecretsBackend

    return ServerFernetLocalSecretsBackend(
        fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
    )


@pytest.fixture
def backend(
    request: pytest.FixtureRequest, client_backend, server_backend
) -> CommonFernetLocalSecretsBackend[Any]:
    side = request.param
    if side == "client":
        return client_backend
    if side == "server":
        return server_backend

    error_msg = f"invalid backend side: {side}"
    raise TypeError(error_msg)


@pytest.fixture
def temp_dir():
    with TemporaryDirectory() as temp_dir_str:
        yield Path(temp_dir_str)


@pytest.fixture
def temp_file(temp_dir: Path):
    return temp_dir / str(uuid4())


@pytest.fixture(scope="class")
def backend_class(
    request: pytest.FixtureRequest,
) -> type[CommonFernetLocalSecretsBackend[Any]]:
    side = request.param
    if not AIRFLOW_SERVER_FLAG and side in {"server", "direct"}:
        pytest.skip("airflow does not support 3.13 yet")

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
        error_msg = f"invalid backend side: {side}"
        raise TypeError(error_msg)
    return FernetLocalSecretsBackend
