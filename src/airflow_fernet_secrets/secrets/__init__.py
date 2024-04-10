from __future__ import annotations

from airflow_fernet_secrets.config import IS_SERVER_FLAG

if IS_SERVER_FLAG:
    from airflow_fernet_secrets.secrets.server import (
        ServerFernetLocalSecretsBackend as FernetLocalSecretsBackend,
    )
else:
    from airflow_fernet_secrets.secrets.client import (
        ClientFernetLocalSecretsBackend as FernetLocalSecretsBackend,
    )

__all__ = ["FernetLocalSecretsBackend"]
