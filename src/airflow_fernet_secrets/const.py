from __future__ import annotations

CLIENT_ENV_PREFIX = "AIRFLOW__PROVIDERS_FERNET_SECRETS__"
SERVER_CONF_SECTION = "providers.fernet_secrets"

DEFAULT_BACKEND_SUFFIX = ".backend.sqlite3"

ENV_SECRET_KEY = "secret_key"  # noqa: S105
ENV_BACKEND_FILE = "backend_file"

ENV_IS_SERVER = "is_server"

LOGGER_NAME = "airflow.fernet_secrets"

SQL_CONN_TYPE = "sql"
SA_DATACLASS_METADATA_KEY = "sa"

JSONABLE_CLASS_TYPE_FLAG = "_jsonable_class_type_flag_"
CONNECTION_BEGIN_INFO_KEY = "is_begin_immediate"

# connection type
SQLITE_CONN_TYPE = "sqlite"
SQLITE_CONN_TYPES = frozenset({SQLITE_CONN_TYPE, "sqlite"})
POSTGRESQL_CONN_TYPE = "postgresql"
POSTGRESQL_CONN_TYPES = frozenset({POSTGRESQL_CONN_TYPE, "postgres", "postgresql"})
ODBC_CONN_TYPE = "odbc"
ODBC_CONN_TYPES = frozenset({ODBC_CONN_TYPE, "odbc"})
MSSQL_CONN_TYPE = "mssql"
MSSQL_CONN_TYPES = frozenset({MSSQL_CONN_TYPE, "odbc"})
