from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from typing_extensions import override

from tests.base_airflow import BaseAirflowTaskTest

if TYPE_CHECKING:
    from airflow import DAG
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.xcom_arg import XComArg

pytestmark = pytest.mark.airflow


class TestOeprator(BaseAirflowTaskTest):
    @staticmethod
    @override
    def create_dump_operator(
        *, task_id: str, dag: DAG, **kwargs: Any
    ) -> BaseOperator | XComArg:
        from airflow_fernet_secrets.operators.dump import DumpSecretsOperator

        return DumpSecretsOperator(task_id=task_id, dag=dag, **kwargs)

    @staticmethod
    @override
    def create_load_operator(
        *, task_id: str, dag: DAG, **kwargs: Any
    ) -> BaseOperator | XComArg:
        from airflow_fernet_secrets.operators.load import LoadSecretsOperator

        return LoadSecretsOperator(task_id=task_id, dag=dag, **kwargs)
