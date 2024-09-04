from __future__ import annotations

from airflow_fernet_secrets.dynamic import HAS_AIRFLOW, IS_SERVER_FLAG

if not HAS_AIRFLOW or not IS_SERVER_FLAG:
    raise ImportError("not has airflow or not in server")

import warnings
from typing import TYPE_CHECKING, Any, Callable, Collection, Mapping, Sequence

from typing_extensions import TypedDict, override

from airflow.decorators.base import DecoratedOperator
from airflow.utils.context import Context, context_merge
from airflow.utils.operator_helpers import determine_kwargs

if TYPE_CHECKING:
    from cryptography.fernet import Fernet, MultiFernet

    from airflow_fernet_secrets._typeshed import PathType

__all__ = []

_PREFIX = "fernet_secrets_"


class SecretsParameters(TypedDict, total=False):
    conn_ids: str | Sequence[str] | None
    var_ids: str | Sequence[str] | None
    rename: str | Sequence[Sequence[str]] | Mapping[str, str] | None
    separate: str | bool
    separator: str
    key: str | bytes | Fernet | MultiFernet | None
    backend_file_path: PathType | None
    overwrite: str | bool


class FernetDecoratedOperator(DecoratedOperator):
    template_fields = ("op_args", "op_kwargs")

    custom_operator_name: str
    python_callable: Callable[..., SecretsParameters]

    @override
    def __init__(
        self,
        *,
        python_callable: Callable[..., SecretsParameters],
        task_id: str,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        if kwargs.pop("multiple_outputs", None):
            warnings.warn(
                "`multiple_outputs=True` is not supported in "
                f"{self.custom_operator_name} tasks. Ignoring.",
                UserWarning,
                stacklevel=3,
            )
        super().__init__(
            python_callable=python_callable,
            task_id=task_id,
            op_args=op_args,
            op_kwargs=op_kwargs,
            **kwargs,
        )

    @override
    def execute(self, context: Context) -> Any:
        context_merge(context, self.op_kwargs)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)
        operator_args = self.python_callable(*self.op_args, **kwargs)

        if not isinstance(operator_args, Mapping):
            error_msg = f"invalid return type: {type(operator_args).__name__}"
            raise TypeError(error_msg)

        operator_args = self.render_template(operator_args, context)
        for key, value in operator_args.items():
            setattr(self, f"{_PREFIX!s}{key!s}", value)

        return super(DecoratedOperator, self).execute(context)
