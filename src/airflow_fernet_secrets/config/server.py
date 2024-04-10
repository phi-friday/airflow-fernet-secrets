from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING

from airflow_fernet_secrets import const
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
    value = _get_from_conf(const.ENV_SECRET_KEY)
    if value:
        return value

    cmd = _get_from_conf(const.ENV_SECRET_KEY_CMD)
    if cmd:
        value = load_from_cmd(cmd)

    if value:
        return value

    logger.error("need secret_key")
    raise NotImplementedError


def load_backend_file(logger: Logger) -> str:
    file = _get_from_conf(const.ENV_BACKEND_FILE)
    if file:
        return file

    cmd = _get_from_conf(const.ENV_BACKEND_FILE_CMD)
    if cmd:
        file = load_from_cmd(cmd)

    if file:
        return file

    return create_backend_file(logger, stacklevel=3)


def _get_from_conf(
    key: str, default_value: str = "", *, section: str | None = None
) -> str:
    from airflow.configuration import conf
    from airflow.exceptions import AirflowConfigException

    section = section or const.SERVER_CONF_SECTION
    with suppress(AirflowConfigException):
        return conf.get(section, key, fallback=default_value, suppress_warnings=True)
    return default_value
