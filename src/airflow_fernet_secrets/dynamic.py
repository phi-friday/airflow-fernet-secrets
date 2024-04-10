from __future__ import annotations

from importlib.util import find_spec
from os import getenv

from airflow_fernet_secrets.const import CLIENT_ENV_PREFIX as _CLIENT_ENV_PREFIX
from airflow_fernet_secrets.const import ENV_IS_SERVER as _ENV_IS_SERVER
from airflow_fernet_secrets.utils.cast import ensure_boolean

HAS_AIRFLOW = find_spec("airflow") is not None
IS_SERVER_FLAG = (
    ensure_boolean(_server_env)
    if (_server_env := getenv((_CLIENT_ENV_PREFIX + _ENV_IS_SERVER).upper(), ""))
    else HAS_AIRFLOW
)


__all__ = ["HAS_AIRFLOW", "IS_SERVER_FLAG"]
