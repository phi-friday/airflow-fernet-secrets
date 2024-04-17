from __future__ import annotations

from os import getenv
from typing import TYPE_CHECKING

from airflow_fernet_secrets import const
from airflow_fernet_secrets import exceptions as fe
from airflow_fernet_secrets.config.common import (
    create_backend_file,
    ensure_fernet_return,
    load_from_cmd,
    load_from_file,
)

if TYPE_CHECKING:
    from logging import Logger

__all__ = ["load_secret_key", "load_backend_file"]


@ensure_fernet_return
def load_secret_key(logger: Logger) -> str:  # noqa: ARG001
    """load secret key in environment variables"""
    value = _get_env_variable(const.ENV_SECRET_KEY)
    if value:
        return value

    cmd = _get_env_variable(const.ENV_SECRET_KEY, cmd=True)
    if cmd:
        value = load_from_cmd(cmd)
    if value:
        return value

    secret = _get_env_variable(const.ENV_SECRET_KEY, secret=True)
    if secret:
        value = load_from_file(secret)
    if value:
        return value

    raise fe.FernetSecretsKeyError("need secret_key")


def load_backend_file(logger: Logger) -> str:
    """load backend file in environment variables"""
    file = _get_env_variable(const.ENV_BACKEND_FILE)
    if file:
        return file

    cmd = _get_env_variable(const.ENV_BACKEND_FILE, cmd=True)
    if cmd:
        file = load_from_cmd(cmd)
    if file:
        return file

    secret = _get_env_variable(const.ENV_BACKEND_FILE, secret=True)
    if secret:
        file = load_from_file(secret)
    if file:
        return file

    return create_backend_file(logger, stacklevel=3)


def _get_env_variable(
    name: str,
    default_value: str | None = None,
    *,
    cmd: bool = False,
    secret: bool = False,
) -> str:
    key = const.CLIENT_ENV_PREFIX + name.upper().strip("_")
    if cmd:
        key = key + "_CMD"
    elif secret:
        key = key + "_SECRET"
    return getenv(key, default_value or "")
