from __future__ import annotations

import shlex
import subprocess
import uuid
from functools import wraps
from pathlib import Path
from tempfile import gettempdir
from typing import TYPE_CHECKING, Callable

from cryptography.fernet import Fernet

from airflow_fernet_secrets.common.config import const

if TYPE_CHECKING:
    from logging import Logger

    from typing_extensions import ParamSpec, TypeVar

    T = TypeVar("T", bound="str | bytes | Fernet", infer_variance=True)
    P = ParamSpec("P")


def create_variables_file(logger: Logger) -> str:
    logger.info("create new variables file", stacklevel=3)
    temp_dir = gettempdir()
    temp_path = Path(temp_dir)
    temp_file = (temp_path / str(uuid.uuid4())).with_suffix(
        const.DEFAULT_VARIABLES_SUFFIX
    )
    return temp_file.as_posix()


def create_connections_file(logger: Logger) -> str:
    logger.info("create new connections file", stacklevel=3)
    temp_dir = gettempdir()
    temp_path = Path(temp_dir)
    temp_file = (temp_path / str(uuid.uuid4())).with_suffix(
        const.DEFAULT_CONNECTIONS_SUFFIX
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


def ensure_fernet(secret_key: str | bytes | Fernet) -> Fernet:
    if isinstance(secret_key, Fernet):
        return secret_key
    return Fernet(secret_key)


def ensure_fernet_return(func: Callable[P, T]) -> Callable[P, Fernet]:
    @wraps(func)
    def inner(*args: P.args, **kwargs: P.kwargs) -> Fernet:
        value: T = func(*args, **kwargs)
        return ensure_fernet(value)

    return inner
