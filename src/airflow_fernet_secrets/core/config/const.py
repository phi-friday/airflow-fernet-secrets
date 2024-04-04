from __future__ import annotations

CLIENT_ENV_PREFIX = "AIRFLOW__PROVIDERS_FERNET_SECRETS__"
SERVER_CONF_SECTION = "providers.fernet_secrets"

DEFAULT_BACKEND_SUFFIX = ".backend.sqlite3"

ENV_SECRET_KEY = "secret_key"  # noqa: S105
ENV_SECRET_KEY_CMD = "secret_key_cmd"  # noqa: S105

ENV_BACKEND_FILE = "backend_file"
ENV_BACKEND_FILE_CMD = "backend_file_cmd"

ENV_IS_SERVER = "is_server"

LOGGER_NAME = "airflow.fernet_secrets"

CONNECTION_DRIVER_FORMAT = "{backend}:{dialect}:{conn_type}"
RE_CONNECTION_DRIVER_FORMAT = (
    r"(?P<backend>[a-zA-Z0-9_-]*?)"
    r":(?P<dialect>[a-zA-Z0-9_-]*?)"
    r":(?P<conn_type>[a-zA-Z0-9_-]*)"
)
