from __future__ import annotations

from typing import Any

from typing_extensions import override

from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.models.xcom_arg import XComArg
from tests.base_airflow import BaseAirflowTaskTest

from airflow_fernet_secrets.operators.dump import DumpSecretsOperator
from airflow_fernet_secrets.operators.load import LoadSecretsOperator


class TestOeprator(BaseAirflowTaskTest):
    @staticmethod
    @override
    def create_dump_operator(
        *, task_id: str, dag: DAG, **kwargs: Any
    ) -> BaseOperator | XComArg:
        return DumpSecretsOperator(task_id=task_id, dag=dag, **kwargs)

    @staticmethod
    @override
    def create_load_operator(
        *, task_id: str, dag: DAG, **kwargs: Any
    ) -> BaseOperator | XComArg:
        return LoadSecretsOperator(task_id=task_id, dag=dag, **kwargs)
