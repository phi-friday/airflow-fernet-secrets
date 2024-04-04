from __future__ import annotations

from importlib.util import find_spec
from os import getenv

from airflow_fernet_secrets.core.config.common import ensure_fernet
from airflow_fernet_secrets.core.config.const import (
    CLIENT_ENV_PREFIX as _CLIENT_ENV_PREFIX,
)
from airflow_fernet_secrets.core.config.const import ENV_IS_SERVER as _ENV_IS_SERVER
from airflow_fernet_secrets.core.utils.cast import ensure_boolean

HAS_AIRFLOW = find_spec("airflow") is None
IS_SERVER_FLAG = (
    ensure_boolean(_server_env)
    if (_server_env := getenv((_CLIENT_ENV_PREFIX + _ENV_IS_SERVER).upper(), ""))
    else HAS_AIRFLOW
)

if IS_SERVER_FLAG:
    from airflow_fernet_secrets.core.config.server import (
        load_backend_file,
        load_secret_key,
    )
else:
    from airflow_fernet_secrets.core.config.client import (
        load_backend_file,
        load_secret_key,
    )

__all__ = [
    "HAS_AIRFLOW",
    "IS_SERVER_FLAG",
    "load_secret_key",
    "load_backend_file",
    "ensure_fernet",
]
