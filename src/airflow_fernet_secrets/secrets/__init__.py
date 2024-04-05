from __future__ import annotations

from airflow_fernet_secrets.core.config import IS_SERVER_FLAG

if IS_SERVER_FLAG:
    from airflow_fernet_secrets.secrets.server import FernetLocalSecretsBackend
else:
    from airflow_fernet_secrets.secrets.client import FernetLocalSecretsBackend

__all__ = ["FernetLocalSecretsBackend"]
