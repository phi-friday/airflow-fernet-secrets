from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

from airflow.decorators.base import TaskDecorator, task_decorator_factory

from airflow_fernet_secrets.decorators.base import FernetDecoratedOperator
from airflow_fernet_secrets.operators.load import LoadSecretsOperator

if TYPE_CHECKING:
    from airflow_fernet_secrets.decorators.base import SecretsParameters

__all__ = ["load_fernet_task"]


class _LoadDecoratedOperator(FernetDecoratedOperator, LoadSecretsOperator):
    custom_operator_name: str = "@task.load_fernet"


def load_fernet_task(
    python_callable: Callable[..., SecretsParameters] | None = None, **kwargs: Any
) -> TaskDecorator:
    """wrap a function into LoadSecretsOperator."""
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_LoadDecoratedOperator,
        **kwargs,
    )
