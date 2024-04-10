from __future__ import annotations

from os import getenv
from typing import TYPE_CHECKING

from airflow_fernet_secrets.config import const
from airflow_fernet_secrets.config.common import (
    create_backend_file,
    ensure_fernet_return,
    load_from_cmd,
)

if TYPE_CHECKING:
    from logging import Logger

__all__ = ["load_secret_key", "load_backend_file"]


@ensure_fernet_return
def load_secret_key(logger: Logger) -> str:
    value = _get_env_variable(const.ENV_SECRET_KEY)
    if value:
        return value

    cmd = _get_env_variable(const.ENV_SECRET_KEY_CMD)
    if cmd:
        value = load_from_cmd(cmd)

    if value:
        return value

    logger.error("need secret_key")
    raise NotImplementedError


def load_backend_file(logger: Logger) -> str:
    file = _get_env_variable(const.ENV_BACKEND_FILE)
    if file:
        return file

    cmd = _get_env_variable(const.ENV_BACKEND_FILE_CMD)
    if cmd:
        file = load_from_cmd(cmd)

    if file:
        return file

    return create_backend_file(logger, stacklevel=3)


def _get_env_variable(name: str, default_value: str | None = None) -> str:
    key = const.CLIENT_ENV_PREFIX + name.upper().strip("_")
    return getenv(key, default_value or "")
