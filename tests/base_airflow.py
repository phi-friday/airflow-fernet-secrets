# pyright: reportUnknownParameterType=false
# pyright: reportMissingParameterType=false
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Iterable

import sqlalchemy as sa
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.engine.url import URL, make_url

from airflow.models.connection import Connection
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
