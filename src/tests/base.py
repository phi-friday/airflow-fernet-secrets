from __future__ import annotations

import warnings
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Generator, Literal, cast

import pytest
import sqlalchemy as sa
from pendulum.datetime import DateTime

from airflow import DAG
from airflow.models.connection import Connection
from airflow.models.variable import Variable
from airflow.models.xcom_arg import XComArg
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from airflow_fernet_secrets import exceptions as fe
from airflow_fernet_secrets.connection.server import convert_connection_to_dict
from airflow_fernet_secrets.utils.re import camel_to_snake

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy.orm import Session
    from typing_extensions import TypeAlias

    from airflow.hooks.base import BaseHook
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.dagrun import DagRun

    from airflow_fernet_secrets._typeshed import PathType
    from airflow_fernet_secrets.secrets.common import CommonFernetLocalSecretsBackend

BackendType: TypeAlias = "type[CommonFernetLocalSecretsBackend[Any]]"
SideLiteral = Literal["client", "server", "direct"]


class BaseTestClientAndServer:
    @pytest.fixture(scope="class", autouse=True)
    def _init_backend(
        self,
        request: pytest.FixtureRequest,
        backend_class: BackendType,
        secret_key,
        backend_path,
    ) -> None:
        cls = request.cls
        assert cls is not None

        self._backend: CommonFernetLocalSecretsBackend[Any]
        self._side: SideLiteral
        self._backend = cls._backend = backend_class(
            fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
        )
        self._side = cls._side = self.find_backend_side(backend_class)

    @property
    def backend(self) -> CommonFernetLocalSecretsBackend[Any]:
        return self._backend

    @property
    def side(self) -> SideLiteral:
        return self._side

    @property
    def dag(self) -> type[DAG]:
        return DAG

    @staticmethod
    @contextmanager
    def create_session() -> Generator[Session, None, None]:
        with ignore_warnings(), create_session() as session:
            yield session

    @staticmethod
    def find_backend_side(backend_class: BackendType) -> SideLiteral:
        side = backend_class.__module__.split(".")[-1]
        if side == "server":
            return side
        if side != "client":
            error_msg = f"invalid backend side: {side}"
            raise fe.FernetSecretsTypeError(error_msg)

        name = camel_to_snake(backend_class.__qualname__)
        prefix = name.split("_", 1)[0]
        if prefix == "client" or prefix == "direct":  # noqa: PLR1714
            return prefix

        error_msg = f"invalid backend side: {side}"
        raise fe.FernetSecretsTypeError(error_msg)

    def create_connection(
        self,
        *,
        conn_id: str | None,
        conn_type: str = "sqlite",
        file: PathType,
        extra: dict[str, Any] | None = None,
        is_async: bool = False,
        **kwargs: Any,
    ) -> Any:
        if self.side == "client":
            from airflow_fernet_secrets.database.connect import create_sqlite_url

            url = create_sqlite_url(file, is_async=is_async, query=extra, **kwargs)
            return {"url": url, "connect_args": {}, "engine_kwargs": {}}
        if self.side == "server":
            return Connection(
                conn_id=conn_id,
                conn_type=conn_type,
                host=str(file),
                extra=extra,
                **kwargs,
            )
        if self.side == "direct":
            connection = Connection(
                conn_id=conn_id,
                conn_type=conn_type,
                host=str(file),
                extra=extra,
                **kwargs,
            )
            return convert_connection_to_dict(connection)
        error_msg = f"invalid backend side: {self.side}"
        raise fe.FernetSecretsTypeError(error_msg)

    @staticmethod
    def create_dagrun(
        dag: DAG, now: datetime | None = None, **kwargs: Any
    ) -> tuple[DagRun, datetime]:
        if now is None:
            now = DateTime.utcnow()
        default = {
            "run_type": DagRunType.MANUAL,
            "execution_date": now,
            "start_date": now,
            "state": DagRunState.RUNNING,
            "external_trigger": False,
            "data_interval": (now, now),
        }
        for key, value in default.items():
            kwargs.setdefault(key, value)
        dag_run = dag.create_dagrun(**kwargs)
        return dag_run, cast(DateTime, dag_run.start_date)

    @staticmethod
    def xcom_to_operator(task: BaseOperator | XComArg) -> BaseOperator:
        if isinstance(task, XComArg):
            return task.operator  # pyright: ignore[reportAttributeAccessIssue]
        return task

    @classmethod
    def run_task(
        cls, task: BaseOperator | XComArg, now: datetime | None = None, **kwargs: Any
    ) -> None:
        if now is None:
            now = DateTime.utcnow()
        default = {
            "start_date": now,
            "end_date": now,
            "ignore_first_depends_on_past": True,
            "ignore_ti_state": True,
        }
        for key, value in default.items():
            kwargs.setdefault(key, value)

        if isinstance(task, XComArg):
            task = cls.xcom_to_operator(task)
        task.run(**kwargs)

    @classmethod
    def add_in_airflow(cls, value: Connection | Variable) -> None:
        with cls.create_session() as session:
            session.add(value)
            session.commit()

    @classmethod
    def get_connection_in_airflow(cls, conn_id: str) -> Connection | None:
        stmt = sa.select(Connection).where(Connection.conn_id == conn_id)
        with cls.create_session() as session:
            result = session.scalars(stmt).one_or_none()
            if result is None:
                return None
            session.expunge(result)
            return result

    @classmethod
    def get_variable_in_airflow(cls, key: str) -> Variable | None:
        stmt = sa.select(Variable).where(Variable.key == key)
        with cls.create_session() as session:
            result = session.scalars(stmt).one_or_none()
            if result is None:
                return None
            session.expunge(result)
            return result


@contextmanager
def ignore_warnings() -> Generator[None, None, None]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        yield


def get_hook(connection: Connection) -> BaseHook:
    with ignore_warnings():
        return connection.get_hook()
