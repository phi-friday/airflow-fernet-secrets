from __future__ import annotations

from airflow_fernet_secrets.secrets.server import (
    ServerFernetLocalSecretsBackend as FernetLocalSecretsBackend,
)

__all__ = ["FernetLocalSecretsBackend"]
