from __future__ import annotations

from airflow_fernet_secrets.dynamic import HAS_AIRFLOW, IS_SERVER_FLAG

if not HAS_AIRFLOW or not IS_SERVER_FLAG:
    raise NotImplementedError

from functools import cached_property
from itertools import chain
from typing import TYPE_CHECKING, Any, Sequence

from airflow.models import BaseOperator

from airflow_fernet_secrets.config.common import ensure_fernet
from airflow_fernet_secrets.config.server import load_secret_key
from airflow_fernet_secrets.secrets.server import ServerFernetLocalSecretsBackend
from airflow_fernet_secrets.utils.cast import ensure_boolean

if TYPE_CHECKING:
    from cryptography.fernet import Fernet, MultiFernet

    from airflow_fernet_secrets._typeshed import PathType


class HasSecrets(BaseOperator):
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


class HasConnIds(HasSecrets):
    template_fields: Sequence[str] = (
        "fernet_secrets_conn_ids",
        "fernet_secrets_conn_ids_separate",
        "fernet_secrets_conn_ids_separator",
        "fernet_secrets_key",
        "fernet_secrets_backend_file_path",
    )

    def __init__(
        self,
        *,
        fernet_secrets_conn_ids: str | list[str] | tuple[str, ...] | None = None,
        fernet_secrets_conn_ids_separate: str | bool = False,
        fernet_secrets_conn_ids_separator: str = ",",
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
        self.fernet_secrets_conn_ids_separate = fernet_secrets_conn_ids_separate
        self.fernet_secrets_conn_ids_separator = fernet_secrets_conn_ids_separator

    @cached_property
    def _separated_conn_ids(self) -> tuple[str, ...]:
        fernet_secrets_conn_ids_separate = ensure_boolean(
            self.fernet_secrets_conn_ids_separate
        )

        fernet_secrets_conn_ids = self.fernet_secrets_conn_ids
        if fernet_secrets_conn_ids is None:
            fernet_secrets_conn_ids = ""
        if isinstance(fernet_secrets_conn_ids, str):
            fernet_secrets_conn_ids = (fernet_secrets_conn_ids,)
        if fernet_secrets_conn_ids_separate:
            fernet_secrets_conn_ids = tuple(
                chain.from_iterable(
                    (
                        x.strip()
                        for x in conn_id.split(self.fernet_secrets_conn_ids_separator)
                    )
                    for conn_id in fernet_secrets_conn_ids
                )
            )
        else:
            fernet_secrets_conn_ids = tuple(fernet_secrets_conn_ids)

        return fernet_secrets_conn_ids
