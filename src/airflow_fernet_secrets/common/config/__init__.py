from __future__ import annotations

from importlib.util import find_spec

if find_spec("airflow") is None:
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

__all__ = ["load_secret_key", "load_connections_file", "load_variables_file"]
