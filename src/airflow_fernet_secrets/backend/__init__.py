from __future__ import annotations

from airflow_fernet_secrets.common.config import IS_SERVER_FLAG

if IS_SERVER_FLAG:
    from airflow_fernet_secrets.backend.server import FernetLocalSecretsBackend
else:
    from airflow_fernet_secrets.backend.client import FernetLocalSecretsBackend

__all__ = ["FernetLocalSecretsBackend"]
