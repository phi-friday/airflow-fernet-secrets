from __future__ import annotations

from contextlib import suppress
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
    """load provider fernet key in airflow conf"""
    value = _get_from_conf(const.ENV_SECRET_KEY)
    if value:
        return value

    cmd = _get_from_conf(const.ENV_SECRET_KEY, cmd=True)
    if cmd:
        value = load_from_cmd(cmd)
    if value:
        return value

    secret = _get_from_conf(const.ENV_SECRET_KEY, secret=True)
    if secret:
        value = load_from_file(secret)
    if value:
        return value

    raise fe.FernetSecretsKeyError("need secret_key")


def load_backend_file(logger: Logger) -> str:
    """load provider backend file in airflow conf"""
    file = _get_from_conf(const.ENV_BACKEND_FILE)
    if file:
        return file

    cmd = _get_from_conf(const.ENV_BACKEND_FILE, cmd=True)
    if cmd:
        file = load_from_cmd(cmd)
    if file:
        return file

    secret = _get_from_conf(const.ENV_BACKEND_FILE, secret=True)
    if secret:
        file = load_from_file(secret)
    if file:
        return file

    return create_backend_file(logger, stacklevel=3)


def _get_from_conf(
    key: str,
    default_value: str = "",
    *,
    section: str | None = None,
    cmd: bool = False,
    secret: bool = False,
) -> str:
    from airflow.configuration import conf
    from airflow.exceptions import AirflowConfigException

    if cmd:
        key = key + "_cmd"
    elif secret:
        key = key + "_secret"

    section = section or const.SERVER_CONF_SECTION
    with suppress(AirflowConfigException):
        return conf.get(section, key, fallback=default_value, suppress_warnings=True)
    return default_value
