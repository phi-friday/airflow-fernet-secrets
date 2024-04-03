from __future__ import annotations

from importlib.util import find_spec
from os import getenv

from airflow_fernet_secrets.common.config.common import ensure_fernet
from airflow_fernet_secrets.common.config.const import (
    CLIENT_ENV_PREFIX as _CLIENT_ENV_PREFIX,
)
from airflow_fernet_secrets.common.config.const import ENV_IS_SERVER as _ENV_IS_SERVER
from airflow_fernet_secrets.common.utils.cast import ensure_boolean

HAS_AIRFLOW = find_spec("airflow") is None
IS_SERVER_FLAG = (
    ensure_boolean(_server_env)
    if (_server_env := getenv((_CLIENT_ENV_PREFIX + _ENV_IS_SERVER).upper(), ""))
    else HAS_AIRFLOW
)

if IS_SERVER_FLAG:
    from airflow_fernet_secrets.common.config.server import (
        load_connections_file,
        load_secret_key,
        load_variables_file,
    )
else:
    from airflow_fernet_secrets.common.config.client import (
        load_connections_file,
        load_secret_key,
        load_variables_file,
    )

__all__ = [
    "HAS_AIRFLOW",
    "IS_SERVER_FLAG",
    "load_secret_key",
    "load_connections_file",
    "load_variables_file",
    "ensure_fernet",
]
