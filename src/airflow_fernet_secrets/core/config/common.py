from __future__ import annotations

import shlex
import subprocess
import uuid
from functools import wraps
from pathlib import Path
from tempfile import gettempdir
from typing import TYPE_CHECKING, Callable

from cryptography.fernet import Fernet, MultiFernet

from airflow_fernet_secrets.core.config import const

if TYPE_CHECKING:
    from logging import Logger

    from typing_extensions import ParamSpec, TypeVar

    T = TypeVar("T", bound="str | bytes | Fernet | MultiFernet", infer_variance=True)
    P = ParamSpec("P")

__all__ = [
    "create_backend_file",
    "load_from_cmd",
    "ensure_fernet",
    "ensure_fernet_return",
]


def create_backend_file(logger: Logger, stacklevel: int = 2) -> str:
    logger.info("create new backend file", stacklevel=stacklevel)
    temp_dir = gettempdir()
    temp_path = Path(temp_dir)
    temp_file = (temp_path / str(uuid.uuid4())).with_suffix(
        const.DEFAULT_BACKEND_SUFFIX
    )
    return temp_file.as_posix()


def load_from_cmd(cmd: str) -> str:
    process = subprocess.run(
        shlex.split(cmd),  # noqa: S603
        text=True,
        capture_output=True,
        check=True,
    )
    return process.stdout.strip()


def ensure_fernet(secret_key: str | bytes | Fernet | MultiFernet) -> MultiFernet:
    if isinstance(secret_key, MultiFernet):
        return secret_key
    if isinstance(secret_key, Fernet):
        return MultiFernet([secret_key])
    if isinstance(secret_key, str):
        return MultiFernet([Fernet(x.strip()) for x in secret_key.split(",")])
    secret_key = Fernet(secret_key)
    return MultiFernet([secret_key])


def ensure_fernet_return(func: Callable[P, T]) -> Callable[P, MultiFernet]:
    @wraps(func)
    def inner(*args: P.args, **kwargs: P.kwargs) -> MultiFernet:
        value: T = func(*args, **kwargs)
        return ensure_fernet(value)

    return inner
