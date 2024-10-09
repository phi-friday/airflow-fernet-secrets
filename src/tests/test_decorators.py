from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

import pytest
from typing_extensions import override

from tests.base_airflow import BaseAirflowTaskTest

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.dag import DAG
    from airflow.models.xcom_arg import XComArg

_PREFIX = "fernet_secrets_"
_RE_PREFIX = re.compile(rf"^{_PREFIX}")

pytestmark = pytest.mark.airflow


class TestDecorator(BaseAirflowTaskTest):
    @staticmethod
    @override
    def create_dump_operator(
        *, task_id: str, dag: DAG, **kwargs: Any
    ) -> BaseOperator | XComArg:
        from airflow.decorators import task

        result = {_RE_PREFIX.sub("", key, 1): value for key, value in kwargs.items()}

        @task.dump_fernet(task_id=task_id, dag=dag)
        def f() -> Any:
            return result.copy()

        return f()

    @staticmethod
    @override
    def create_load_operator(
        *, task_id: str, dag: DAG, **kwargs: Any
    ) -> BaseOperator | XComArg:
        from airflow.decorators import task

        result = {_RE_PREFIX.sub("", key, 1): value for key, value in kwargs.items()}

        @task.load_fernet(task_id=task_id, dag=dag)
        def f() -> Any:
            return result.copy()

        return f()
