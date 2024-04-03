from __future__ import annotations

from airflow_fernet_secrets.client.backend import (
    FernetLocalSecretsBackend as FernetLocalSecretsBackendClient,
)


class FernetLocalSecretsBackend(FernetLocalSecretsBackendClient): ...
