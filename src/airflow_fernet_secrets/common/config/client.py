from __future__ import annotations

from os import getenv
from typing import TYPE_CHECKING

from airflow_fernet_secrets.common.config.common import (
    create_connections_file,
    create_variables_file,
    ensure_fernet_return,
    load_from_cmd,
)

if TYPE_CHECKING:
    from logging import Logger


_PREFIX = "AIRFLOW__PROVIDERS_FERNET_SECRETS__"


@ensure_fernet_return
def load_secret_key(logger: Logger) -> str:
    env = _env_variable("secret_key")
    value = getenv(env, "")
    if value:
        return value

    env = _env_variable("secret_key_cmd")
    cmd = getenv(env, "")
    if cmd:
        value = load_from_cmd(cmd)

    if value:
        return value

    logger.error("need secret_key")
    raise NotImplementedError


def load_variables_file(logger: Logger) -> str:
    env = _env_variable("variables_file")
    file = getenv(env, "")
    if file:
        return file

    env = _env_variable("variables_file_cmd")
    cmd = getenv(env, "")
    if cmd:
        file = load_from_cmd(cmd)

    if file:
        return file

    return create_variables_file(logger)


def load_connections_file(logger: Logger) -> str:
    env = _env_variable("connections_file")
    file = getenv(env, "")
    if file:
        return file

    env = _env_variable("connections_file_cmd")
    cmd = getenv(env, "")
    if cmd:
        file = load_from_cmd(cmd)

    if file:
        return file

    return create_connections_file(logger)


def _env_variable(name: str) -> str:
    return _PREFIX + name.upper().strip("_")
