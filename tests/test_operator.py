from __future__ import annotations

import json
from typing import Iterable
from uuid import uuid4

import pytest
import sqlalchemy as sa
from airflow.models.baseoperator import BaseOperator
from airflow.models.connection import Connection
from airflow.models.dagrun import DagRun
from airflow.models.variable import Variable
from airflow.models.xcom import BaseXCom
from airflow.utils.db import initdb

from tests.base import BaseTestClientAndServer, ignore_warnings


@pytest.mark.parametrize("backend_class", ["server"], indirect=True)
class TestOeprator(BaseTestClientAndServer):
    @pytest.fixture(scope="class", autouse=True)
    def _init_database(self) -> None:
        with ignore_warnings():
            initdb()

    def test_dump_connection(self, secret_key, backend_path, temp_file):
        from airflow_fernet_secrets.operators.dump import DumpConnectionsOperator

        conn_id = temp_file.stem
        assert not self.backend.has_connection(conn_id)

        conn = Connection(
            conn_id=conn_id, conn_type="fs", extra={"path": str(temp_file)}
        )
        with self.create_session() as session:
            session.add(conn)
            session.commit()

        dag = self.dag(dag_id="test_dump", schedule=None)
        dag_run, now = self.create_dagrun(dag)
        task = DumpConnectionsOperator(
            task_id="dump",
            dag=dag,
            fernet_secrets_conn_ids=conn_id,
            fernet_secrets_key=secret_key,
            fernet_secrets_backend_file_path=backend_path,
        )
        self.run_task(task, now=now)

        check = self.backend.get_connection(conn_id=conn_id)
        assert check is not None

        assert conn.conn_id == check.conn_id
        assert conn.conn_type == check.conn_type
        assert conn.extra_dejson == check.extra_dejson

        self.check_task_output(dag_run, task, [conn_id])

    def test_dump_variable(self, secret_key, backend_path):
        from airflow_fernet_secrets.operators.dump import DumpConnectionsOperator

        key, value = str(uuid4()), str(uuid4())
        assert not self.backend.has_variable(key)

        variable = Variable(key, value)
        with self.create_session() as session:
            session.add(variable)
            session.commit()

        dag = self.dag(dag_id="test_dump", schedule=None)
        dag_run, now = self.create_dagrun(dag)
        task = DumpConnectionsOperator(
            task_id="dump",
            dag=dag,
            fernet_secrets_var_ids=key,
            fernet_secrets_key=secret_key,
            fernet_secrets_backend_file_path=backend_path,
        )
        self.run_task(task, now=now)

        check = self.backend.get_variable(key=key)
        assert check is not None
        assert check == value

        self.check_task_output(dag_run, task, None, [key])

    def test_load_connection(self, secret_key, backend_path, temp_file):
        from airflow_fernet_secrets.operators.load import LoadConnectionsOperator

        conn_id = temp_file.stem
        select = sa.select(Connection).where(Connection.conn_id == conn_id)
        with self.create_session() as session:
            check = session.scalars(select).one_or_none()
        assert check is None

        conn = Connection(
            conn_id=conn_id, conn_type="fs", extra={"path": str(temp_file)}
        )
        self.backend.set_connection(conn_id=conn_id, connection=conn)

        with self.create_session() as session:
            check = session.scalars(select).one_or_none()
        assert check is None

        dag = self.dag(dag_id="test_load", schedule=None)
        dag_run, now = self.create_dagrun(dag)
        task = LoadConnectionsOperator(
            task_id="load",
            dag=dag,
            fernet_secrets_conn_ids=conn_id,
            fernet_secrets_key=secret_key,
            fernet_secrets_backend_file_path=backend_path,
        )
        self.run_task(task, now=now)

        with self.create_session() as session:
            check = session.scalars(select).one_or_none()
        assert check is not None

        assert conn.conn_id == check.conn_id
        assert conn.conn_type == check.conn_type
        assert conn.extra_dejson == check.extra_dejson

        self.check_task_output(dag_run, task, [conn_id])

    def test_load_variable(self, secret_key, backend_path):
        from airflow_fernet_secrets.operators.load import LoadConnectionsOperator

        key, value = str(uuid4()), str(uuid4())
        select = sa.select(Variable).where(Variable.key == key)
        with self.create_session() as session:
            check = session.scalars(select).one_or_none()
        assert check is None

        self.backend.set_variable(key, value)

        with self.create_session() as session:
            check = session.scalars(select).one_or_none()
        assert check is None

        dag = self.dag(dag_id="test_load", schedule=None)
        dag_run, now = self.create_dagrun(dag)
        task = LoadConnectionsOperator(
            task_id="load",
            dag=dag,
            fernet_secrets_var_ids=key,
            fernet_secrets_key=secret_key,
            fernet_secrets_backend_file_path=backend_path,
        )
        self.run_task(task, now=now)

        with self.create_session() as session:
            check = session.scalars(select).one_or_none()
        assert check is not None
        assert isinstance(check, Variable)
        assert check.val == value

        self.check_task_output(dag_run, task, None, [key])

    def test_dump_connection_using_conf(self, secret_key, backend_path, temp_file):
        from airflow_fernet_secrets.operators.dump import DumpConnectionsOperator

        conn_id = temp_file.stem
        assert not self.backend.has_connection(conn_id)

        conn = Connection(
            conn_id=conn_id, conn_type="fs", extra={"path": str(temp_file)}
        )
        with self.create_session() as session:
            session.add(conn)
            session.commit()

        conf = {"target_conn_id": conn_id}
        dag = self.dag(dag_id="test_dump", schedule=None)
        dag_run, now = self.create_dagrun(dag, conf=conf)
        task = DumpConnectionsOperator(
            task_id="dump",
            dag=dag,
            fernet_secrets_conn_ids="{{ dag_run.conf.target_conn_id }}",
            fernet_secrets_key=secret_key,
            fernet_secrets_backend_file_path=backend_path,
        )
        self.run_task(task, now=now)

        check = self.backend.get_connection(conn_id=conn_id)
        assert check is not None

        assert conn.conn_id == check.conn_id
        assert conn.conn_type == check.conn_type
        assert conn.extra_dejson == check.extra_dejson

        self.check_task_output(dag_run, task, [conn_id])

    def test_dump_variable_using_conf(self, secret_key, backend_path):
        from airflow_fernet_secrets.operators.dump import DumpConnectionsOperator

        key, value = str(uuid4()), str(uuid4())
        assert not self.backend.has_variable(key)

        variable = Variable(key, value)
        with self.create_session() as session:
            session.add(variable)
            session.commit()

        conf = {"target_variable_key": key}
        dag = self.dag(dag_id="test_dump", schedule=None)
        dag_run, now = self.create_dagrun(dag, conf=conf)
        task = DumpConnectionsOperator(
            task_id="dump",
            dag=dag,
            fernet_secrets_var_ids="{{ dag_run.conf.target_variable_key }}",
            fernet_secrets_key=secret_key,
            fernet_secrets_backend_file_path=backend_path,
        )
        self.run_task(task, now=now)

        check = self.backend.get_variable(key=key)
        assert check is not None
        assert check == value

        self.check_task_output(dag_run, task, None, [key])

    def test_load_connection_using_conf(self, secret_key, backend_path, temp_file):
        from airflow_fernet_secrets.operators.load import LoadConnectionsOperator

        conn_id = temp_file.stem
        select = sa.select(Connection).where(Connection.conn_id == conn_id)
        with self.create_session() as session:
            check = session.scalars(select).one_or_none()
        assert check is None

        conn = Connection(
            conn_id=conn_id, conn_type="fs", extra={"path": str(temp_file)}
        )
        self.backend.set_connection(conn_id=conn_id, connection=conn)

        with self.create_session() as session:
            check = session.scalars(select).one_or_none()
        assert check is None

        conf = {"target_conn_id": conn_id}
        dag = self.dag(dag_id="test_load", schedule=None)
        dag_run, now = self.create_dagrun(dag, conf=conf)
        task = LoadConnectionsOperator(
            task_id="load",
            dag=dag,
            fernet_secrets_conn_ids="{{ dag_run.conf.target_conn_id }}",
            fernet_secrets_key=secret_key,
            fernet_secrets_backend_file_path=backend_path,
        )
        self.run_task(task, now=now)

        with self.create_session() as session:
            check = session.scalars(select).one_or_none()
        assert check is not None

        assert conn.conn_id == check.conn_id
        assert conn.conn_type == check.conn_type
        assert conn.extra_dejson == check.extra_dejson

        self.check_task_output(dag_run, task, [conn_id])

    def test_load_variable_using_conf(self, secret_key, backend_path):
        from airflow_fernet_secrets.operators.load import LoadConnectionsOperator

        key, value = str(uuid4()), str(uuid4())
        select = sa.select(Variable).where(Variable.key == key)
        with self.create_session() as session:
            check = session.scalars(select).one_or_none()
        assert check is None

        self.backend.set_variable(key, value)

        with self.create_session() as session:
            check = session.scalars(select).one_or_none()
        assert check is None

        conf = {"target_variable_key": key}
        dag = self.dag(dag_id="test_load", schedule=None)
        dag_run, now = self.create_dagrun(dag, conf=conf)
        task = LoadConnectionsOperator(
            task_id="load",
            dag=dag,
            fernet_secrets_var_ids="{{ dag_run.conf.target_variable_key }}",
            fernet_secrets_key=secret_key,
            fernet_secrets_backend_file_path=backend_path,
        )
        self.run_task(task, now=now)

        with self.create_session() as session:
            check = session.scalars(select).one_or_none()
        assert check is not None
        assert isinstance(check, Variable)
        assert check.val == value

        self.check_task_output(dag_run, task, None, [key])

    def check_task_output(
        self,
        dag_run: DagRun,
        task: BaseOperator,
        conn_ids: Iterable[str] | None = None,
        var_ids: Iterable[str] | None = None,
    ) -> None:
        stmt = sa.select(BaseXCom).where(
            BaseXCom.dag_id == task.dag_id,
            BaseXCom.task_id == task.task_id,
            BaseXCom.run_id == dag_run.run_id,
        )
        with self.create_session() as session:
            output = session.scalars(stmt.with_only_columns(BaseXCom.value)).one()

        assert isinstance(output, (str, bytes))
        output = json.loads(output)

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
