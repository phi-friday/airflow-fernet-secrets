from __future__ import annotations

CLIENT_ENV_PREFIX = "AIRFLOW__PROVIDERS_FERNET_SECRETS__"
SERVER_CONF_SECTION = "providers.fernet_secrets"

DEFAULT_VARIABLES_SUFFIX = ".variables.sqlite3"
DEFAULT_CONNECTIONS_SUFFIX = ".connections.sqlite3"

ENV_SECRET_KEY = "secret_key"  # noqa: S105
ENV_SECRET_KEY_CMD = "secret_key_cmd"  # noqa: S105

ENV_VARIABLES_FILE = "variables_file"
ENV_VARIABLES_FILE_CMD = "variables_file_cmd"

ENV_CONNECTIONS_FILE = "connections_file"
ENV_CONNECTIONS_FILE_CMD = "connections_file_cmd"

ENV_IS_SERVER = "is_server"

LOGGER_NAME = "airflow.fernet_secrets"
