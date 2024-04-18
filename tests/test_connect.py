from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, wait
from functools import partial
from random import shuffle
from threading import Event
from typing import Callable
from uuid import uuid4

import anyio
import pytest

from tests.base import BaseTestClientAndServer

from airflow_fernet_secrets.secrets.common import CommonFernetLocalSecretsBackend


def set_airflow_env():
    import os

    os.environ["AIRFLOW__SECRETS__BACKEND"] = (
        "airflow_fernet_secrets.secrets.server.ServerFernetLocalSecretsBackend"
    )


def set_random_variable(
    backend_getter: Callable[[], CommonFernetLocalSecretsBackend],
    event: Event,
    key: str | None = None,
) -> None:
    event.wait()
    backend = backend_getter()
    if key is None:
        key = str(uuid4())
    value = str(uuid4())
    backend.set_variable(key, value)


def get_random_variable(
    backend_getter: Callable[[], CommonFernetLocalSecretsBackend],
    event: Event,
    key: str | None = None,
) -> None:
    event.wait()
    backend = backend_getter()
    if key is None:
        key = str(uuid4())
    backend.get_variable(key)


async def aset_random_variable(
    backend_getter: Callable[[], CommonFernetLocalSecretsBackend],
    key: str | None = None,
) -> None:
    backend = backend_getter()
    if key is None:
        key = str(uuid4())
    value = str(uuid4())
    await backend.aset_variable(key, value)


async def aget_random_variable(
    backend_getter: Callable[[], CommonFernetLocalSecretsBackend],
    key: str | None = None,
) -> None:
    backend = backend_getter()
    if key is None:
        key = str(uuid4())
    await backend.aget_variable(key)


def getter(backend: CommonFernetLocalSecretsBackend) -> CommonFernetLocalSecretsBackend:
    key = backend._fernet_secrets_key  # noqa: SLF001
    file = backend.fernet_secrets_backend_file

    return type(backend)(fernet_secrets_key=key, fernet_secrets_backend_file_path=file)


@pytest.mark.parametrize("backend_class", ["client"], indirect=True)
class TestConcurrency(BaseTestClientAndServer):
    def test_parallel(self):
        index = list(range(1000))
        shuffle(index)

        backend_getter = partial(getter, self.backend)

        event = Event()
        random_setter = partial(set_random_variable, backend_getter, event)
        random_getter = partial(get_random_variable, backend_getter, event)

        with ThreadPoolExecutor(100, initializer=set_airflow_env) as pool:
            futures = [
                pool.submit(random_setter) if x % 2 else pool.submit(random_getter)
                for x in index
            ]
            event.set()
            done = wait(futures, return_when="FIRST_EXCEPTION")
            pool.shutdown()
        for future in done.done:
            error = future.exception()
            if error is None:
                continue
            raise error

    @pytest.mark.anyio()
    async def test_concurrency(self):
        index = list(range(1000))
        shuffle(index)

        backend_getter = partial(getter, self.backend)

        random_setter = partial(aset_random_variable, backend_getter)
        random_getter = partial(aget_random_variable, backend_getter)

        async with anyio.create_task_group() as task_group:
            for x in index:
                task_group.start_soon(random_setter if x % 2 else random_getter)
