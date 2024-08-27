from __future__ import annotations

from airflow_fernet_secrets.dynamic import HAS_AIRFLOW, IS_SERVER_FLAG

if not HAS_AIRFLOW or not IS_SERVER_FLAG:
    raise ImportError("not has airflow or not in server")

import json
from functools import cached_property
from itertools import chain
from typing import TYPE_CHECKING, Any, Mapping, Sequence

from typing_extensions import TypedDict

from airflow.models import BaseOperator

from airflow_fernet_secrets import exceptions as fe
from airflow_fernet_secrets.config.common import ensure_fernet
from airflow_fernet_secrets.config.server import load_secret_key
from airflow_fernet_secrets.secrets.server import ServerFernetLocalSecretsBackend
from airflow_fernet_secrets.utils.cast import ensure_boolean

if TYPE_CHECKING:
    from cryptography.fernet import Fernet, MultiFernet

    from airflow_fernet_secrets._typeshed import PathType


class OperatorResult(TypedDict, total=True):
    """airflow-fernet-secrets operators result type"""

    connection: list[str]
    """connection ids"""
    variable: list[str]
    """variable keys"""


class HasSecrets(BaseOperator):
    """base operator with secrets"""

    template_fields: Sequence[str] = (
        "fernet_secrets_key",
        "fernet_secrets_backend_file_path",
    )

    def __init__(
        self,
        *,
        fernet_secrets_key: str | bytes | Fernet | MultiFernet | None = None,
        fernet_secrets_backend_file_path: PathType | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.fernet_secrets_key = fernet_secrets_key
        self.fernet_secrets_backend_file_path = fernet_secrets_backend_file_path

        self._fernet_secrets_backend = None

    def _load_secret_from_attr(self) -> MultiFernet | None:
        if self.fernet_secrets_key is None:
            return None
        return ensure_fernet(self.fernet_secrets_key)

    def _secret(self) -> MultiFernet:
        fernet_secrets_key = self._load_secret_from_attr()
        if fernet_secrets_key is not None:
            return fernet_secrets_key
        return load_secret_key(self.log)

    def _backend(self) -> ServerFernetLocalSecretsBackend:
        if self._fernet_secrets_backend is not None:
            return self._fernet_secrets_backend

        self._fernet_secrets_backend = ServerFernetLocalSecretsBackend(
            fernet_secrets_key=self.fernet_secrets_key,
            fernet_secrets_backend_file_path=self.fernet_secrets_backend_file_path,
        )
        return self._fernet_secrets_backend


class HasIds(HasSecrets):
    """base operator with templated ids"""

    template_fields: Sequence[str] = (
        "fernet_secrets_conn_ids",
        "fernet_secrets_var_ids",
        "fernet_secrets_rename",
        "fernet_secrets_separate",
        "fernet_secrets_separator",
        "fernet_secrets_key",
        "fernet_secrets_backend_file_path",
    )

    def __init__(  # noqa: PLR0913
        self,
        *,
        fernet_secrets_conn_ids: str | Sequence[str] | None = None,
        fernet_secrets_var_ids: str | Sequence[str] | None = None,
        fernet_secrets_rename: str
        | bytes
        | Sequence[Sequence[str]]
        | Mapping[str, str]
        | None = None,
        fernet_secrets_separate: str | bool = False,
        fernet_secrets_separator: str = ",",
        fernet_secrets_key: str | bytes | Fernet | MultiFernet | None = None,
        fernet_secrets_backend_file_path: PathType | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            fernet_secrets_key=fernet_secrets_key,
            fernet_secrets_backend_file_path=fernet_secrets_backend_file_path,
            **kwargs,
        )

        self.fernet_secrets_conn_ids = fernet_secrets_conn_ids
        self.fernet_secrets_var_ids = fernet_secrets_var_ids
        self.fernet_secrets_rename = fernet_secrets_rename
        self.fernet_secrets_separate = fernet_secrets_separate
        self.fernet_secrets_separator = fernet_secrets_separator

    @cached_property
    def _rename_mapping(self) -> dict[str, str]:
        if self.fernet_secrets_rename is None:
            return {}
        if isinstance(self.fernet_secrets_rename, (str, bytes, bytearray)):
            value = json.loads(self.fernet_secrets_rename)
        else:
            value = self.fernet_secrets_rename

        if isinstance(value, Sequence):
            return {str(x[0]): str(x[1]) for x in value}
        if isinstance(value, Mapping):
            return {str(key): str(value) for key, value in value.items()}

        error_msg = f"invalid rename mapping type: {type(value).__name__}"
        raise fe.FernetSecretsTypeError(error_msg)

    @cached_property
    def _separated_conn_ids(self) -> tuple[str, ...]:
        return _separated_ids(
            self.fernet_secrets_conn_ids,
            self.fernet_secrets_separate,
            self.fernet_secrets_separator,
        )

    @cached_property
    def _separated_var_ids(self) -> tuple[str, ...]:
        return _separated_ids(
            self.fernet_secrets_var_ids,
            self.fernet_secrets_separate,
            self.fernet_secrets_separator,
        )


def _separated_ids(
    ids: str | Sequence[str] | None, separate: str | bool, separator: str
) -> tuple[str, ...]:
    separate = ensure_boolean(separate)

    if ids is None:
        ids = ""
    if isinstance(ids, str):
        ids = (ids,)
    if separate:
        return tuple(
            chain.from_iterable(
                (x.strip() for x in sub.split(separator)) for sub in ids
            )
        )
    return tuple(ids)
