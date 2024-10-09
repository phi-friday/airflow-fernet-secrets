from __future__ import annotations

from typing import TYPE_CHECKING, Mapping, Sequence

from typing_extensions import TypeAlias, TypedDict

if TYPE_CHECKING:
    from cryptography.fernet import Fernet, MultiFernet

    from airflow_fernet_secrets._typeshed import PathType

__all__ = ["SecretsParameter"]

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
