from __future__ import annotations

import json
from itertools import product
from typing import Iterable
from uuid import uuid4

import pytest
import sqlalchemy as sa
from airflow.models.baseoperator import BaseOperator
from airflow.models.connection import Connection
from airflow.models.dagrun import DagRun
from airflow.models.variable import Variable
from airflow.models.xcom import BaseXCom

from tests.base import BaseTestClientAndServer

from airflow_fernet_secrets.operators.dump import DumpSecretsOperator
from airflow_fernet_secrets.operators.load import LoadSecretsOperator


@pytest.mark.parametrize("backend_class", ["server"], indirect=True)
class TestOeprator(BaseTestClientAndServer):
    def test_dump_connection(self, secret_key, backend_path, temp_file):
        conn_id = temp_file.stem
        assert not self.backend.has_connection(conn_id)

        conn = Connection(
            conn_id=conn_id, conn_type="fs", extra={"path": str(temp_file)}
        )
        self.add_in_airflow(conn)

        dag = self.dag(dag_id="test_dump", schedule=None)
        dag_run, now = self.create_dagrun(dag)
        task = DumpSecretsOperator(
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
        key, value = str(uuid4()), str(uuid4())
        assert not self.backend.has_variable(key)

        variable = Variable(key, value)
        self.add_in_airflow(variable)

        dag = self.dag(dag_id="test_dump", schedule=None)
        dag_run, now = self.create_dagrun(dag)
        task = DumpSecretsOperator(
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
        conn_id = temp_file.stem
        check = self.get_connection_in_airflow(conn_id)
        assert check is None

        conn = Connection(
            conn_id=conn_id, conn_type="fs", extra={"path": str(temp_file)}
        )
        self.backend.set_connection(conn_id=conn_id, connection=conn)

        check = self.get_connection_in_airflow(conn_id)
        assert check is None

        dag = self.dag(dag_id="test_load", schedule=None)
        dag_run, now = self.create_dagrun(dag)
        task = LoadSecretsOperator(
            task_id="load",
            dag=dag,
            fernet_secrets_conn_ids=conn_id,
            fernet_secrets_key=secret_key,
            fernet_secrets_backend_file_path=backend_path,
        )
        self.run_task(task, now=now)

        check = self.get_connection_in_airflow(conn_id)
        assert check is not None

        assert conn.conn_id == check.conn_id
        assert conn.conn_type == check.conn_type
        assert conn.extra_dejson == check.extra_dejson

        self.check_task_output(dag_run, task, [conn_id])

    def test_load_variable(self, secret_key, backend_path):
        key, value = str(uuid4()), str(uuid4())
        check = self.get_variable_in_airflow(key)
        assert check is None

        self.backend.set_variable(key, value)

        check = self.get_variable_in_airflow(key)
        assert check is None

        dag = self.dag(dag_id="test_load", schedule=None)
        dag_run, now = self.create_dagrun(dag)
        task = LoadSecretsOperator(
            task_id="load",
            dag=dag,
            fernet_secrets_var_ids=key,
            fernet_secrets_key=secret_key,
            fernet_secrets_backend_file_path=backend_path,
        )
        self.run_task(task, now=now)

        check = self.get_variable_in_airflow(key)
        assert check is not None
        assert isinstance(check, Variable)
        assert check.val == value

        self.check_task_output(dag_run, task, None, [key])

    @pytest.mark.parametrize(
        ("conf_conn_id", "conf_secret_key", "conf_backend_path"),
        (
            pytest.param(
                *param,
                id="conn={:^5s}:secret={:^5s}:path:{:^5s}".format(*map(str, param)),
            )
            for param in product((True, False), repeat=3)
        ),
    )
    def test_dump_connection_using_jinja(
        self,
        secret_key: bytes,
        backend_path,
        temp_file,
        conf_conn_id: bool,
        conf_secret_key: bool,
        conf_backend_path: bool,
    ):
        conn_id = temp_file.stem
        assert not self.backend.has_connection(conn_id)

        conn = Connection(
            conn_id=conn_id, conn_type="fs", extra={"path": str(temp_file)}
        )
        self.add_in_airflow(conn)

        conf = {}
        for flag, key, value in zip(
            (conf_conn_id, conf_secret_key, conf_backend_path),
            ("conf_conn_id", "conf_secret_key", "conf_backend_path"),
            (conn_id, secret_key.decode("utf-8"), str(backend_path)),
        ):
            if flag:
                conf[key] = value

        dag = self.dag(dag_id="test_dump", schedule=None)
        dag_run, now = self.create_dagrun(dag, conf=conf)
        task = DumpSecretsOperator(
            task_id="dump",
            dag=dag,
            fernet_secrets_conn_ids="{{ dag_run.conf.conf_conn_id }}"
            if conf_conn_id
            else conn_id,
            fernet_secrets_key="{{ dag_run.conf.conf_secret_key }}"
            if conf_secret_key
            else secret_key,
            fernet_secrets_backend_file_path="{{ dag_run.conf.conf_backend_path }}"
            if conf_backend_path
            else backend_path,
        )
        self.run_task(task, now=now)

        check = self.backend.get_connection(conn_id=conn_id)
        assert check is not None

        assert conn.conn_id == check.conn_id
        assert conn.conn_type == check.conn_type
        assert conn.extra_dejson == check.extra_dejson

        self.check_task_output(dag_run, task, [conn_id])

    @pytest.mark.parametrize(
        ("conf_variable_key", "conf_secret_key", "conf_backend_path"),
        (
            pytest.param(
                *param,
                id="key={:^5s}:secret={:^5s}:path:{:^5s}".format(*map(str, param)),
            )
            for param in product((True, False), repeat=3)
        ),
    )
    def test_dump_variable_using_jinja(
        self,
        secret_key: bytes,
        backend_path,
        conf_variable_key: bool,
        conf_secret_key: bool,
        conf_backend_path: bool,
    ):
        variable_key, variable_value = str(uuid4()), str(uuid4())
        assert not self.backend.has_variable(variable_key)

        variable = Variable(variable_key, variable_value)
        self.add_in_airflow(variable)

        conf = {}
        for flag, conf_key, value in zip(
            (conf_variable_key, conf_secret_key, conf_backend_path),
            ("conf_variable_key", "conf_secret_key", "conf_backend_path"),
            (variable_key, secret_key.decode("utf-8"), str(backend_path)),
        ):
            if flag:
                conf[conf_key] = value

        dag = self.dag(dag_id="test_dump", schedule=None)
        dag_run, now = self.create_dagrun(dag, conf=conf)
        task = DumpSecretsOperator(
            task_id="dump",
            dag=dag,
            fernet_secrets_var_ids="{{ dag_run.conf.conf_variable_key }}"
            if conf_variable_key
            else variable_key,
            fernet_secrets_key="{{ dag_run.conf.conf_secret_key }}"
            if conf_secret_key
            else secret_key,
            fernet_secrets_backend_file_path="{{ dag_run.conf.conf_backend_path }}"
            if conf_backend_path
            else backend_path,
        )
        self.run_task(task, now=now)

        check = self.backend.get_variable(key=variable_key)
        assert check is not None
        assert check == variable_value

        self.check_task_output(dag_run, task, None, [variable_key])

    @pytest.mark.parametrize(
        ("conf_conn_id", "conf_secret_key", "conf_backend_path"),
        (
            pytest.param(
                *param,
                id="conn={:^5s}:secret={:^5s}:path:{:^5s}".format(*map(str, param)),
            )
            for param in product((True, False), repeat=3)
        ),
    )
    def test_load_connection_using_conf(
        self,
        secret_key: bytes,
        backend_path,
        temp_file,
        conf_conn_id: bool,
        conf_secret_key: bool,
        conf_backend_path: bool,
    ):
        conn_id = temp_file.stem
        check = self.get_connection_in_airflow(conn_id)
        assert check is None

        conn = Connection(
            conn_id=conn_id, conn_type="fs", extra={"path": str(temp_file)}
        )
        self.backend.set_connection(conn_id=conn_id, connection=conn)

        check = self.get_connection_in_airflow(conn_id)
        assert check is None

        conf = {}
        for flag, key, value in zip(
            (conf_conn_id, conf_secret_key, conf_backend_path),
            ("conf_conn_id", "conf_secret_key", "conf_backend_path"),
            (conn_id, secret_key.decode("utf-8"), str(backend_path)),
        ):
            if flag:
                conf[key] = value

        dag = self.dag(dag_id="test_load", schedule=None)
        dag_run, now = self.create_dagrun(dag, conf=conf)
        task = LoadSecretsOperator(
            task_id="load",
            dag=dag,
            fernet_secrets_conn_ids="{{ dag_run.conf.conf_conn_id }}"
            if conf_conn_id
            else conn_id,
            fernet_secrets_key="{{ dag_run.conf.conf_secret_key }}"
            if conf_secret_key
            else secret_key,
            fernet_secrets_backend_file_path="{{ dag_run.conf.conf_backend_path }}"
            if conf_backend_path
            else backend_path,
        )
        self.run_task(task, now=now)

        check = self.get_connection_in_airflow(conn_id)
        assert check is not None

        assert conn.conn_id == check.conn_id
        assert conn.conn_type == check.conn_type
        assert conn.extra_dejson == check.extra_dejson

        self.check_task_output(dag_run, task, [conn_id])

    @pytest.mark.parametrize(
        ("conf_variable_key", "conf_secret_key", "conf_backend_path"),
        (
            pytest.param(
                *param,
                id="key={:^5s}:secret={:^5s}:path:{:^5s}".format(*map(str, param)),
            )
            for param in product((True, False), repeat=3)
        ),
    )
    def test_load_variable_using_conf(
        self,
        secret_key: bytes,
        backend_path,
        conf_variable_key: bool,
        conf_secret_key: bool,
        conf_backend_path: bool,
    ):
        variable_key, variable_value = str(uuid4()), str(uuid4())
        check = self.get_variable_in_airflow(variable_key)
        assert check is None

        self.backend.set_variable(variable_key, variable_value)

        check = self.get_variable_in_airflow(variable_key)
        assert check is None

        conf = {}
        for flag, conf_key, value in zip(
            (conf_variable_key, conf_secret_key, conf_backend_path),
            ("conf_variable_key", "conf_secret_key", "conf_backend_path"),
            (variable_key, secret_key.decode("utf-8"), str(backend_path)),
        ):
            if flag:
                conf[conf_key] = value

        dag = self.dag(dag_id="test_load", schedule=None)
        dag_run, now = self.create_dagrun(dag, conf=conf)
        task = LoadSecretsOperator(
            task_id="load",
            dag=dag,
            fernet_secrets_var_ids="{{ dag_run.conf.conf_variable_key }}"
            if conf_variable_key
            else variable_key,
            fernet_secrets_key="{{ dag_run.conf.conf_secret_key }}"
            if conf_secret_key
            else secret_key,
            fernet_secrets_backend_file_path="{{ dag_run.conf.conf_backend_path }}"
            if conf_backend_path
            else backend_path,
        )
        self.run_task(task, now=now)

        check = self.get_variable_in_airflow(variable_key)
        assert check is not None
        assert isinstance(check, Variable)
        assert check.val == variable_value

        self.check_task_output(dag_run, task, None, [variable_key])

    def test_dump_connection_rename(self, secret_key, backend_path, temp_file):
        conn_id = temp_file.stem
        new_id = str(uuid4())
        conn = Connection(
            conn_id=conn_id, conn_type="fs", extra={"path": str(temp_file)}
        )
        self.add_in_airflow(conn)

        dag = self.dag(dag_id="test_dump", schedule=None)
        dag_run, now = self.create_dagrun(dag)
        task = DumpSecretsOperator(
            task_id="dump",
            dag=dag,
            fernet_secrets_conn_ids=conn_id,
            fernet_secrets_key=secret_key,
            fernet_secrets_backend_file_path=backend_path,
            fernet_secrets_rename={conn_id: new_id},
        )
        self.run_task(task, now=now)

        check = self.backend.has_connection(conn_id=conn_id)
        assert check is False
        check = self.backend.get_connection(conn_id=new_id)
        assert check is not None

        assert conn.conn_type == check.conn_type
        assert conn.extra_dejson == check.extra_dejson

        self.check_task_output(dag_run, task, [new_id])

    def test_dump_variable_rename(self, secret_key, backend_path):
        key, value, new_key = str(uuid4()), str(uuid4()), str(uuid4())
        variable = Variable(key, value)
        self.add_in_airflow(variable)

        dag = self.dag(dag_id="test_dump", schedule=None)
        dag_run, now = self.create_dagrun(dag)
        task = DumpSecretsOperator(
            task_id="dump",
            dag=dag,
            fernet_secrets_var_ids=key,
            fernet_secrets_key=secret_key,
            fernet_secrets_backend_file_path=backend_path,
            fernet_secrets_rename={key: new_key},
        )
        self.run_task(task, now=now)

        check = self.backend.has_variable(key=key)
        assert check is False

        check = self.backend.get_variable(key=new_key)
        assert check is not None
        assert check == value

        self.check_task_output(dag_run, task, None, [new_key])

    def test_load_connection_rename(self, secret_key, backend_path, temp_file):
        conn_id = temp_file.stem
        conn = Connection(
            conn_id=conn_id, conn_type="fs", extra={"path": str(temp_file)}
        )
        self.backend.set_connection(conn_id=conn_id, connection=conn)
        new_conn_id = str(uuid4())

        dag = self.dag(dag_id="test_load", schedule=None)
        dag_run, now = self.create_dagrun(dag)
        task = LoadSecretsOperator(
            task_id="load",
            dag=dag,
            fernet_secrets_conn_ids=conn_id,
            fernet_secrets_key=secret_key,
            fernet_secrets_backend_file_path=backend_path,
            fernet_secrets_rename={conn_id: new_conn_id},
        )
        self.run_task(task, now=now)

        check = self.get_connection_in_airflow(conn_id)
        assert check is None
        check = self.get_connection_in_airflow(new_conn_id)
        assert check is not None

        assert conn.conn_type == check.conn_type
        assert conn.extra_dejson == check.extra_dejson

        self.check_task_output(dag_run, task, [new_conn_id])

    def test_load_variable_rename(self, secret_key, backend_path):
        key, value, new_key = str(uuid4()), str(uuid4()), str(uuid4())
        self.backend.set_variable(key, value)

        dag = self.dag(dag_id="test_load", schedule=None)
        dag_run, now = self.create_dagrun(dag)
        task = LoadSecretsOperator(
            task_id="load",
            dag=dag,
            fernet_secrets_var_ids=key,
            fernet_secrets_key=secret_key,
            fernet_secrets_backend_file_path=backend_path,
            fernet_secrets_rename={key: new_key},
        )
        self.run_task(task, now=now)

        check = self.get_variable_in_airflow(key)
        assert check is None
        check = self.get_variable_in_airflow(new_key)
        assert check is not None

        assert isinstance(check, Variable)
        assert check.val == value

        self.check_task_output(dag_run, task, None, [new_key])

    @pytest.mark.parametrize(
        ("conn_ids", "var_ids"),
        [
            (["conn_1", "conn_2"], []),
            ([], ["var_1", "var_2"]),
            (["conn_1", "conn_2"], ["var_1", "var_2"]),
        ],
    )
    def test_dump_many(
        self, secret_key, backend_path, conn_ids: list[str], var_ids: list[str]
    ):
        salt = str(uuid4())
        conn_ids = [f"{salt}-{x}" for x in conn_ids]
        var_ids = [f"{salt}-{x}" for x in var_ids]

        conn_values: dict[str, Connection] = {}
        var_values: dict[str, Variable] = {}
        for conn_id in conn_ids:
            conn = Connection(
                conn_id=conn_id, conn_type="fs", extra={"path": "/some_path"}
            )
            self.add_in_airflow(conn)
            conn_values[conn_id] = conn
        for var_id in var_ids:
            var = Variable(key=var_id, val=str(uuid4()))
            self.add_in_airflow(var)
            var_values[var_id] = var

        separator = ","
        conn_ids_str = separator.join(conn_ids)
        var_ids_str = separator.join(var_ids)

        conf = {
            "conn_ids": conn_ids_str,
            "var_ids": var_ids_str,
            "separator": separator,
            "separate": "true",
        }
        dag = self.dag(dag_id="test_dump", schedule=None)
        dag_run, now = self.create_dagrun(dag, conf=conf)
        task = DumpSecretsOperator(
            task_id="dump",
            dag=dag,
            fernet_secrets_conn_ids="{{ dag_run.conf.conn_ids }}",
            fernet_secrets_var_ids="{{ dag_run.conf.var_ids }}",
            fernet_secrets_separate="{{ dag_run.conf.separate }}",
            fernet_secrets_separator="{{ dag_run.conf.separator }}",
            fernet_secrets_key=secret_key,
            fernet_secrets_backend_file_path=backend_path,
        )
        self.run_task(task, now=now)

        self.check_task_output(dag_run, task, conn_ids, var_ids)

        for key, value in conn_values.items():
            conn = self.backend.get_connection(key)
            assert conn is not None
            assert conn.conn_id == value.conn_id
            assert conn.conn_type == value.conn_type
            assert conn.extra_dejson == value.extra_dejson

        for key, value in var_values.items():
            var = self.backend.get_variable(key)
            assert var is not None
            assert var == value.val

    @pytest.mark.parametrize(
        ("conn_ids", "var_ids"),
        [
            (["conn_1", "conn_2"], []),
            ([], ["var_1", "var_2"]),
            (["conn_1", "conn_2"], ["var_1", "var_2"]),
        ],
    )
    def test_load_many(
        self, secret_key, backend_path, conn_ids: list[str], var_ids: list[str]
    ):
        salt = str(uuid4())
        conn_ids = [f"{salt}-{x}" for x in conn_ids]
        var_ids = [f"{salt}-{x}" for x in var_ids]

        conn_values: dict[str, Connection] = {}
        var_values: dict[str, str] = {}
        for conn_id in conn_ids:
            conn = Connection(
                conn_id=conn_id, conn_type="fs", extra={"path": "/some_path"}
            )
            self.backend.set_connection(conn_id=conn_id, connection=conn)
            conn_values[conn_id] = conn
        for var_id in var_ids:
            var = str(uuid4())
            self.backend.set_variable(key=var_id, value=var)
            var_values[var_id] = var

        separator = ","
        conn_ids_str = separator.join(conn_ids)
        var_ids_str = separator.join(var_ids)

        conf = {
            "conn_ids": conn_ids_str,
            "var_ids": var_ids_str,
            "separator": separator,
            "separate": "true",
        }
        dag = self.dag(dag_id="test_load", schedule=None)
        dag_run, now = self.create_dagrun(dag, conf=conf)
        task = LoadSecretsOperator(
            task_id="load",
            dag=dag,
            fernet_secrets_conn_ids="{{ dag_run.conf.conn_ids }}",
            fernet_secrets_var_ids="{{ dag_run.conf.var_ids }}",
            fernet_secrets_separate="{{ dag_run.conf.separate }}",
            fernet_secrets_separator="{{ dag_run.conf.separator }}",
            fernet_secrets_key=secret_key,
            fernet_secrets_backend_file_path=backend_path,
        )
        self.run_task(task, now=now)

        self.check_task_output(dag_run, task, conn_ids, var_ids)

        for key, value in conn_values.items():
            conn = self.get_connection_in_airflow(key)
            assert conn is not None
            assert conn.conn_id == value.conn_id
            assert conn.conn_type == value.conn_type
            assert conn.extra_dejson == value.extra_dejson

        for key, value in var_values.items():
            var = self.get_variable_in_airflow(key)
            assert var is not None
            assert var.val == value

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

    def add_in_airflow(self, value: Connection | Variable) -> None:
        with self.create_session() as session:
            session.add(value)
            session.commit()

    def get_connection_in_airflow(self, conn_id: str) -> Connection | None:
        stmt = sa.select(Connection).where(Connection.conn_id == conn_id)
        with self.create_session() as session:
            result = session.scalars(stmt).one_or_none()
            if result is None:
                return None
            session.expunge(result)
            return result

    def get_variable_in_airflow(self, key: str) -> Variable | None:
        stmt = sa.select(Variable).where(Variable.key == key)
        with self.create_session() as session:
            result = session.scalars(stmt).one_or_none()
            if result is None:
                return None
            session.expunge(result)
            return result
