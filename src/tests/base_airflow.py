from __future__ import annotations

import json
from itertools import product
from typing import TYPE_CHECKING, Any, Iterable
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.engine.url import URL, make_url

from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.models.variable import Variable
from airflow.models.xcom import BaseXCom
from airflow.models.xcom_arg import XComArg
from airflow.providers.common.sql.hooks.sql import DbApiHook
from tests.base import BaseTestClientAndServer, get_hook

from airflow_fernet_secrets import exceptions as fe

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.dagrun import DagRun


class BaseAirflowTestClientAndServer(BaseTestClientAndServer):
    def assert_connection_type(self, connection: Any) -> None:
        if self.side == "client":
            assert isinstance(connection, dict)
            assert "url" in connection
            assert "connect_args" in connection
            assert "engine_kwargs" in connection
            url, connect_args, engine_kwargs = (
                connection["url"],
                connection["connect_args"],
                connection["engine_kwargs"],
            )
            assert isinstance(url, (str, URL))
            assert isinstance(connect_args, dict)
            assert isinstance(engine_kwargs, dict)
        elif self.side == "server":
            assert isinstance(connection, Connection)
        elif self.side == "direct":
            assert isinstance(connection, dict)
        else:
            error_msg = f"invalid backend side: {self.side}"
            raise fe.FernetSecretsTypeError(error_msg)

    @classmethod
    def check_task_output(
        cls,
        dag_run: DagRun,
        task: BaseOperator | XComArg,
        conn_ids: Iterable[str] | None = None,
        var_ids: Iterable[str] | None = None,
    ) -> None:
        task = cls.xcom_to_operator(task)
        stmt = sa.select(BaseXCom).where(
            BaseXCom.dag_id == task.dag_id,
            BaseXCom.task_id == task.task_id,
            BaseXCom.run_id == dag_run.run_id,
        )
        with cls.create_session() as session:
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

    def dump_connection(self, connection: Any) -> str:
        if self.side == "client":
            self.assert_connection_type(connection)
            url: str | URL = connection["url"]
            url = make_url(url)
            return url.render_as_string(hide_password=False)
        if self.side == "server":
            assert isinstance(connection, Connection)
            return connection.get_uri()
        if self.side == "direct":
            assert isinstance(connection, dict)
            return json.dumps(connection)
        error_msg = f"invalid backend side: {self.side}"
        raise fe.FernetSecretsTypeError(error_msg)

    def create_engine(self, connection: Any) -> Engine:
        if self.side == "client":
            self.assert_connection_type(connection)
            return create_engine(
                connection["url"],
                connect_args=connection["connect_args"],
                **connection["engine_kwargs"],
            )
        if self.side == "server":
            hook = get_hook(connection)
            assert isinstance(hook, DbApiHook)
            engine = hook.get_sqlalchemy_engine()
            assert isinstance(engine, Engine)
            return engine
        error_msg = f"invalid backend side: {self.side}"
        raise fe.FernetSecretsTypeError(error_msg)


@pytest.mark.parametrize("backend_class", ["server"], indirect=True)
class BaseAirflowTaskTest(BaseAirflowTestClientAndServer):
    @staticmethod
    def create_dump_operator(
        *, task_id: str, dag: DAG, **kwargs: Any
    ) -> BaseOperator | XComArg:
        raise NotImplementedError

    @staticmethod
    def create_load_operator(
        *, task_id: str, dag: DAG, **kwargs: Any
    ) -> BaseOperator | XComArg:
        raise NotImplementedError

    def test_dump_connection(self, secret_key, backend_path, temp_file):
        conn_id = temp_file.stem
        assert not self.backend.has_connection(conn_id)

        conn = Connection(
            conn_id=conn_id, conn_type="fs", extra={"path": str(temp_file)}
        )
        self.add_in_airflow(conn)

        dag = self.dag(dag_id="test_dump", schedule=None)
        dag_run, now = self.create_dagrun(dag)
        task = self.create_dump_operator(
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
        task = self.create_dump_operator(
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
        task = self.create_load_operator(
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
        task = self.create_load_operator(
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

        conf = {
            key: value
            for flag, key, value in zip(
                (conf_conn_id, conf_secret_key, conf_backend_path),
                ("conf_conn_id", "conf_secret_key", "conf_backend_path"),
                (conn_id, secret_key.decode("utf-8"), str(backend_path)),
            )
            if flag
        }

        dag = self.dag(dag_id="test_dump", schedule=None)
        dag_run, now = self.create_dagrun(dag, conf=conf)
        task = self.create_dump_operator(
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

        conf = {
            key: value
            for flag, key, value in zip(
                (conf_variable_key, conf_secret_key, conf_backend_path),
                ("conf_variable_key", "conf_secret_key", "conf_backend_path"),
                (variable_key, secret_key.decode("utf-8"), str(backend_path)),
            )
            if flag
        }

        dag = self.dag(dag_id="test_dump", schedule=None)
        dag_run, now = self.create_dagrun(dag, conf=conf)
        task = self.create_dump_operator(
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

        conf = {
            key: value
            for flag, key, value in zip(
                (conf_conn_id, conf_secret_key, conf_backend_path),
                ("conf_conn_id", "conf_secret_key", "conf_backend_path"),
                (conn_id, secret_key.decode("utf-8"), str(backend_path)),
            )
            if flag
        }

        dag = self.dag(dag_id="test_load", schedule=None)
        dag_run, now = self.create_dagrun(dag, conf=conf)
        task = self.create_load_operator(
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

        conf = {
            key: value
            for flag, key, value in zip(
                (conf_variable_key, conf_secret_key, conf_backend_path),
                ("conf_variable_key", "conf_secret_key", "conf_backend_path"),
                (variable_key, secret_key.decode("utf-8"), str(backend_path)),
            )
            if flag
        }

        dag = self.dag(dag_id="test_load", schedule=None)
        dag_run, now = self.create_dagrun(dag, conf=conf)
        task = self.create_load_operator(
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
        task = self.create_dump_operator(
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
        task = self.create_dump_operator(
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
        task = self.create_load_operator(
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
        task = self.create_load_operator(
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
        task = self.create_dump_operator(
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
        task = self.create_load_operator(
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
