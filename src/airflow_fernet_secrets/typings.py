from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping, Sequence

from typing_extensions import Required, TypeAlias, TypedDict

if TYPE_CHECKING:
    from cryptography.fernet import Fernet, MultiFernet
    from sqlalchemy.engine.url import URL

    from airflow_fernet_secrets._typeshed import PathType

__all__ = ["SecretsParameter", "ConnectionArgs", "ConnectionDict"]

SecretsConnIds: TypeAlias = "str | Sequence[str]"
SecretsVarIds: TypeAlias = "str | Sequence[str]"
SecretsRename: TypeAlias = "str | Sequence[Sequence[str]] | Mapping[str, str]"
SecretsSeparate: TypeAlias = "str | bool"
SecretsSeparator: TypeAlias = str
SecretsKey: TypeAlias = "str | bytes | Fernet | MultiFernet"
SecretsBackendFilePath: TypeAlias = "PathType"
SecretsOverwrite: TypeAlias = "str | bool"


class SecretsParameter(TypedDict, total=False):
    """airflow-fernet-secrets parameter."""

    conn_ids: SecretsConnIds | None
    var_ids: SecretsVarIds | None
    rename: SecretsRename | None
    separate: SecretsSeparate
    separator: SecretsSeparator
    key: SecretsKey | None
    backend_file_path: SecretsBackendFilePath | None
    overwrite: SecretsOverwrite


class ConnectionArgs(TypedDict, total=True):
    """connection args using in sqlalchemy"""

    url: str | URL
    """sqlalchemy url"""
    connect_args: dict[str, Any]
    """dbapi connection args"""
    engine_kwargs: dict[str, Any]
    """sqlalchemy engine args"""


class ConnectionDict(TypedDict, total=False):
    """serialized connection dict"""

    conn_type: str
    """conn type(== airflow `Connection.conn_type`)"""
    host: str
    """`Connection.host`"""
    login: str
    """`Connection.login`"""
    password: str
    """`Connection.password`"""
    schema: str
    """`Connection.schema`"""
    port: int
    """`Connection.port`"""
    extra: Required[dict[str, Any]]
    """`Connection.extra`"""
    args: Required[ConnectionArgs | None]
    """`ConnectionArgs`"""
