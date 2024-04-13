from __future__ import annotations

from typing import Iterable
from uuid import uuid4

import pytest
import sqlalchemy as sa
from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.models.dagrun import DagRun
from airflow.models.xcom import XCom
from airflow.utils.db import initdb
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from pendulum.datetime import DateTime

from tests.base import BaseTestClientAndServer, ignore_warnings


@pytest.mark.parametrize("backend_class", ["server"], indirect=True)
class TestOeprator(BaseTestClientAndServer):
    @pytest.fixture(scope="class", autouse=True)
    def _init_database(self) -> None:
        with ignore_warnings():
            initdb()

    def test_dump_connection(self, secret_key, backend_path, temp_file):
        from airflow.models.connection import Connection
        from airflow.utils.session import create_session

        from airflow_fernet_secrets.operators.dump import DumpConnectionsOperator

        conn_id = temp_file.stem
        assert not self.backend.has_connection(conn_id)

        conn = Connection(conn_id=conn_id, conn_type="fs", extra={"fs": str(temp_file)})
        with ignore_warnings(), create_session() as session:
            session.add(conn)
            session.commit()

        now = DateTime.utcnow()
        dag = DAG(dag_id="test_dump", schedule=None)
        dag_run = dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=now,
            start_date=now,
            state=DagRunState.RUNNING,
            external_trigger=False,
            data_interval=(now, now),
        )
        task = DumpConnectionsOperator(
            task_id="dump",
            dag=dag,
            fernet_secrets_conn_ids=conn_id,
            fernet_secrets_key=secret_key,
            fernet_secrets_backend_file_path=backend_path,
        )
        task.run(
            start_date=now,
            end_date=now,
            ignore_first_depends_on_past=True,
            ignore_ti_state=True,
        )

        check = self.backend.get_connection(conn_id=conn_id)
        assert check is not None

        assert conn.conn_id == check.conn_id
        assert conn.conn_type == check.conn_type
        assert conn.extra_dejson == check.extra_dejson

        check_task_output(dag_run, task, [conn_id])

    def test_dump_variable(self, secret_key, backend_path):
        from airflow.models.variable import Variable
        from airflow.utils.session import create_session

        from airflow_fernet_secrets.operators.dump import DumpConnectionsOperator

        key, value = str(uuid4()), str(uuid4())
        assert not self.backend.has_variable(key)

        variable = Variable(key, value)
        with ignore_warnings(), create_session() as session:
            session.add(variable)
            session.commit()

        now = DateTime.utcnow()
        dag = DAG(dag_id="test_dump", schedule=None)
        dag_run = dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=now,
            start_date=now,
            state=DagRunState.RUNNING,
            external_trigger=False,
            data_interval=(now, now),
        )
        task = DumpConnectionsOperator(
            task_id="dump",
            dag=dag,
            fernet_secrets_var_ids=key,
            fernet_secrets_key=secret_key,
            fernet_secrets_backend_file_path=backend_path,
        )
        task.run(
            start_date=now,
            end_date=now,
            ignore_first_depends_on_past=True,
            ignore_ti_state=True,
        )

        check = self.backend.get_variable(key=key)
        assert check is not None
        assert check == value

        check_task_output(dag_run, task, None, [key])

    def test_load_connection(self, secret_key, backend_path, temp_file):
        from airflow.models.connection import Connection
        from airflow.utils.session import create_session

        from airflow_fernet_secrets.operators.load import LoadConnectionsOperator

        conn_id = temp_file.stem
        select = sa.select(Connection).where(Connection.conn_id == conn_id)
        with create_session() as session:
            check = session.scalars(select).one_or_none()
        assert check is None

        conn = Connection(conn_id=conn_id, conn_type="fs", extra={"fs": str(temp_file)})
        self.backend.set_connection(conn_id=conn_id, connection=conn)

        with create_session() as session:
            check = session.scalars(select).one_or_none()
        assert check is None

        now = DateTime.utcnow()
        dag = DAG(dag_id="test_load", schedule=None)
        dag_run = dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=now,
            start_date=now,
            state=DagRunState.RUNNING,
            external_trigger=False,
            data_interval=(now, now),
        )
        task = LoadConnectionsOperator(
            task_id="load",
            dag=dag,
            fernet_secrets_conn_ids=conn_id,
            fernet_secrets_key=secret_key,
            fernet_secrets_backend_file_path=backend_path,
        )
        task.run(
            start_date=now,
            end_date=now,
            ignore_first_depends_on_past=True,
            ignore_ti_state=True,
        )

        with create_session() as session:
            check = session.scalars(select).one_or_none()
        assert check is not None

        assert conn.conn_id == check.conn_id
        assert conn.conn_type == check.conn_type
        assert conn.extra_dejson == check.extra_dejson

        check_task_output(dag_run, task, [conn_id])

    def test_load_variable(self, secret_key, backend_path, temp_file):
        from airflow.models.variable import Variable
        from airflow.utils.session import create_session

        from airflow_fernet_secrets.operators.load import LoadConnectionsOperator

        key, value = str(uuid4()), str(uuid4())
        select = sa.select(Variable).where(Variable.key == key)
        with create_session() as session:
            check = session.scalars(select).one_or_none()
        assert check is None

        self.backend.set_variable(key, value)

        with create_session() as session:
            check = session.scalars(select).one_or_none()
        assert check is None

        now = DateTime.utcnow()
        dag = DAG(dag_id="test_load", schedule=None)
        dag_run = dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=now,
            start_date=now,
            state=DagRunState.RUNNING,
            external_trigger=False,
            data_interval=(now, now),
        )
        task = LoadConnectionsOperator(
            task_id="load",
            dag=dag,
            fernet_secrets_var_ids=key,
            fernet_secrets_key=secret_key,
            fernet_secrets_backend_file_path=backend_path,
        )
        task.run(
            start_date=now,
            end_date=now,
            ignore_first_depends_on_past=True,
            ignore_ti_state=True,
        )

        with create_session() as session:
            check = session.scalars(select).one_or_none()
        assert check is not None
        assert isinstance(check, Variable)
        assert check.val == value

        check_task_output(dag_run, task, None, [key])


def check_task_output(
    dag_run: DagRun,
    task: BaseOperator,
    conn_ids: Iterable[str] | None = None,
    var_ids: Iterable[str] | None = None,
) -> None:
    output = XCom.get_one(
        dag_id=task.dag_id, task_id=task.task_id, run_id=str(dag_run.run_id)
    )

    assert isinstance(output, dict)
    assert "connection" in output
    assert "variable" in output

    output_connection, output_variable = output["connection"], output["variable"]
    assert isinstance(output_connection, list)
    assert isinstance(output_variable, list)

    conn_ids = [] if conn_ids is None else conn_ids
    var_ids = [] if var_ids is None else var_ids
    assert set(output_connection) == set(conn_ids)
    assert set(output_variable) == set(var_ids)
