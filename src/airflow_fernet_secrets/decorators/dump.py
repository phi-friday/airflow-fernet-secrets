from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

from airflow.decorators.base import TaskDecorator, task_decorator_factory

from airflow_fernet_secrets.decorators.base import FernetDecoratedOperator
from airflow_fernet_secrets.operators.dump import DumpSecretsOperator

if TYPE_CHECKING:
    from airflow_fernet_secrets.decorators.base import SecretsParameters

__all__ = ["dump_fernet_task"]


class _DumpDecoratedOperator(FernetDecoratedOperator, DumpSecretsOperator):
    custom_operator_name: str = "@task.dump_fernet"


def dump_fernet_task(
    python_callable: Callable[..., SecretsParameters] | None = None, **kwargs: Any
) -> TaskDecorator:
    """wrap a function into DumpSecretsOperator."""
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_DumpDecoratedOperator,
        **kwargs,
    )
