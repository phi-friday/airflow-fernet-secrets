from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.configuration import conf

from airflow_fernet_secrets.common.config.common import (
    create_connections_file,
    create_variables_file,
    ensure_fernet_return,
    load_from_cmd,
)

if TYPE_CHECKING:
    from logging import Logger


@ensure_fernet_return
def load_secret_key(logger: Logger) -> str:
    value = conf.get("providers.fernet_secrets", "secret_key", "")
    if value:
        return value

    cmd = conf.get("providers.fernet_secrets", "secret_key_cmd", "")
    if cmd:
        value = load_from_cmd(cmd)

    if value:
        return value

    logger.warning("empty secret_key. use airflow core secret key.")
    value = conf.get("core", "fernet_key", "")
    if value:
        return value

    raise NotImplementedError


def load_variables_file(logger: Logger) -> str:
    file = conf.get("providers.fernet_secrets", "variables_file", "")
    if file:
        return file

    cmd = conf.get("providers.fernet_secrets", "variables_file_cmd", "")
    if cmd:
        file = load_from_cmd(cmd)

    if file:
        return file

    return create_variables_file(logger)


def load_connections_file(logger: Logger) -> str:
    file = conf.get("providers.fernet_secrets", "connections_file", "")
    if file:
        return file

    cmd = conf.get("providers.fernet_secrets", "connections_file_cmd", "")
    if cmd:
        file = load_from_cmd(cmd)

    if file:
        return file

    return create_connections_file(logger)
