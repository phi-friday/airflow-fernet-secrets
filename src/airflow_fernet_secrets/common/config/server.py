from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.configuration import conf

from airflow_fernet_secrets.common.config import const
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
    value = _get_from_conf(const.ENV_SECRET_KEY)
    if value:
        return value

    cmd = _get_from_conf(const.ENV_SECRET_KEY_CMD)
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
    file = _get_from_conf(const.ENV_VARIABLES_FILE)
    if file:
        return file

    cmd = _get_from_conf(const.ENV_VARIABLES_FILE_CMD)
    if cmd:
        file = load_from_cmd(cmd)

    if file:
        return file

    return create_variables_file(logger)


def load_connections_file(logger: Logger) -> str:
    file = _get_from_conf(const.ENV_CONNECTIONS_FILE)
    if file:
        return file

    cmd = _get_from_conf(const.ENV_CONNECTIONS_FILE_CMD)
    if cmd:
        file = load_from_cmd(cmd)

    if file:
        return file

    return create_connections_file(logger)


def _get_from_conf(
    key: str, default_value: str = "", *, section: str | None = None
) -> str:
    section = section or const.SERVER_CONF_SECTION
    return conf.get(section, key, default_value)
