from __future__ import annotations

import json
from contextlib import suppress
from os import devnull, environ
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, Generator, Iterable, Protocol, cast
from uuid import uuid4

import pytest
from cryptography.fernet import Fernet

if TYPE_CHECKING:
    from _pytest.nodes import Node
    from airflow.models.dag import DAG
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    from pendulum.datetime import DateTime
    from sqlalchemy.engine.url import URL
    from sqlalchemy.orm import Session
    from typing_extensions import Self

    from airflow_fernet_secrets.secrets.client import ClientFernetLocalSecretsBackend
    from airflow_fernet_secrets.secrets.server import ServerFernetLocalSecretsBackend

    class DagMaker(Protocol):
        def __enter__(self) -> DAG: ...
        def __exit__(
            self, exc_type: object, exc_value: object, exc_traceback: object
        ) -> None: ...
        def create_dagrun(self, **kwargs: Any) -> DagRun: ...
        def create_dagrun_after(self, dagrun: DagRun, **kwargs: Any) -> DagRun: ...
        def __call__(  # noqa: PLR0913
            self,
            *,
            dag_id: str = ...,
            serialized: bool = ...,
            fileloc: str | None = None,
            processor_subdir: str | None = None,
            session: Session | None = None,
            **kwargs: Any,
        ) -> Self: ...


def _set_backend_kwargs(key: str, value: Any) -> None:
    json_value = environ.get("AIRFLOW__SECRETS__BACKEND_KWARGS")

    kwargs: dict[str, Any]
    kwargs = json.loads(json_value) if json_value else {}
    kwargs[key] = value

    environ["AIRFLOW__SECRETS__BACKEND_KWARGS"] = json.dumps(kwargs)


@pytest.fixture(
    params=[
        pytest.param(("asyncio", {"use_uvloop": False}), id="asyncio"),
        pytest.param(("asyncio", {"use_uvloop": True}), id="asyncio-uvloop"),
    ]
)
def anyio_backend(request) -> tuple[str, dict[str, Any]]:
    return request.param


@pytest.fixture(scope="session")
def _init_envs() -> None:
    environ["AIRFLOW__SECRETS__BACKEND"] = (
        "airflow.providers.fernet_secrets.secrets.secret_manager.FernetLocalSecretsBackend"
    )


@pytest.fixture(scope="session")
def temp_path():
    with TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture(scope="session")
def backend_path(temp_path: Path, _init_envs):
    from airflow_fernet_secrets.database.connect import (
        create_sqlite_url,
        ensure_sqlite_sync_engine,
    )
    from airflow_fernet_secrets.database.model import migrate

    file = temp_path / str(uuid4())
    file.touch()

    url = create_sqlite_url(file)
    engine = ensure_sqlite_sync_engine(url)
    migrate(engine)

    _set_backend_kwargs("fernet_secrets_backend_file_path", str(file))

    return file


@pytest.fixture(scope="session")
def secret_key(_init_envs):
    key = Fernet.generate_key()
    _set_backend_kwargs("fernet_secrets_key", key.decode("utf-8"))
    return key


@pytest.fixture(scope="session")
def default_conn_id():
    return "default"


@pytest.fixture(scope="session")
def default_async_conn_id(default_conn_id):
    return f"{default_conn_id}-async"


@pytest.fixture(scope="session")
def default_conn(temp_path: Path) -> URL:
    from sqlalchemy.engine.url import URL

    file = temp_path / str(uuid4())
    return URL.create("sqlite", database=str(file))


@pytest.fixture(scope="session")
def default_async_conn(default_conn: URL) -> URL:
    return default_conn.set(drivername="sqlite+aiosqlite")


@pytest.fixture()
def client_backend(  # noqa: PLR0913
    secret_key,
    backend_path,
    default_conn_id,
    default_conn,
    default_async_conn_id,
    default_async_conn,
) -> ClientFernetLocalSecretsBackend:
    from airflow_fernet_secrets.secrets.client import ClientFernetLocalSecretsBackend

    backend = ClientFernetLocalSecretsBackend(
        fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
    )

    value = backend.get_conn_value(default_conn_id)
    if value is not None:
        return backend

    backend.set_connection(default_conn_id, default_conn)
    backend.set_connection(default_async_conn_id, default_async_conn)
    return backend


@pytest.fixture()
def server_backend(secret_key, backend_path) -> ServerFernetLocalSecretsBackend:
    from airflow_fernet_secrets.secrets.server import ServerFernetLocalSecretsBackend

    return ServerFernetLocalSecretsBackend(
        fernet_secrets_key=secret_key, fernet_secrets_backend_file_path=backend_path
    )


@pytest.fixture()
def temp_dir():
    with TemporaryDirectory() as temp_dir_str:
        yield Path(temp_dir_str)


@pytest.fixture()
def temp_file(temp_dir: Path):
    return temp_dir / str(uuid4())


@pytest.fixture()
def create_dag_fixture(  # noqa: C901
    request: pytest.FixtureRequest,
) -> Generator[DagMaker, None, None]:
    """obtained from airflow.tests.conftest.dag_maker"""
    import lazy_object_proxy
    import sqlalchemy as sa
    from airflow import settings
    from airflow.models import DagBag, DagModel, DagRun, TaskInstance, XCom
    from airflow.models.dag import DAG
    from airflow.models.dataset import DatasetEvent
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.models.taskmap import TaskMap
    from airflow.utils import timezone
    from airflow.utils.log.logging_mixin import LoggingMixin
    from airflow.utils.retries import run_with_db_retries
    from airflow.utils.state import State
    from airflow.utils.types import DagRunType

    want_serialized: bool = False

    node = cast("Node", request.node)
    serialized_marker = node.get_closest_marker("need_serialized_dag")
    if serialized_marker:
        (want_serialized,) = serialized_marker.args or (True,)

    class DagFactory(LoggingMixin):
        _own_session = False

        def __init__(self) -> None:
            self.dagbag = DagBag(
                devnull, include_examples=False, read_dags_from_db=False
            )

        def __enter__(self) -> DAG:
            self.dag.__enter__()
            if self.want_serialized:
                return lazy_object_proxy.Proxy(self._serialized_dag)
            return self.dag

        def _serialized_dag(self) -> DAG:
            return self.serialized_model.dag

        def get_serialized_data(self) -> dict[str, Any] | None:
            try:
                data = self.serialized_model.data
            except AttributeError as exc:
                raise RuntimeError("DAG serialization not requested") from exc
            if isinstance(data, str):
                return json.loads(data)
            return data

        def __exit__(
            self, exc_type: object, exc_value: object, exc_traceback: object
        ) -> None:
            dag = self.dag
            dag.__exit__(exc_type, exc_value, exc_traceback)
            if exc_type is not None:
                return

            dag.clear(session=self.session)
            dag.sync_to_db(processor_subdir=self.processor_subdir, session=self.session)
            self.dag_model = self.session.get(DagModel, dag.dag_id)

            if self.want_serialized:
                self.serialized_model = SerializedDagModel(
                    dag,
                    processor_subdir=cast(
                        "str | None", getattr(self.dag_model, "processor_subdir", None)
                    ),
                )
                self.session.merge(self.serialized_model)
                serialized_dag = self._serialized_dag()
                self.dagbag.bag_dag(serialized_dag, root_dag=serialized_dag)
                self.session.flush()
            else:
                self.dagbag.bag_dag(self.dag, self.dag)

        def create_dagrun(self, **kwargs: Any) -> DagRun:
            dag = self.dag
            kwargs = {
                "state": State.RUNNING,
                "start_date": self.start_date,
                "session": self.session,
                **kwargs,
            }
            if "run_id" not in kwargs and "run_type" not in kwargs:
                kwargs["run_id"] = "test"

            if "run_type" not in kwargs:
                kwargs["run_type"] = DagRunType.from_run_id(kwargs["run_id"])
            if kwargs.get("execution_date") is None:
                if kwargs["run_type"] == DagRunType.MANUAL:
                    kwargs["execution_date"] = self.start_date
                else:
                    kwargs["execution_date"] = getattr(
                        dag.next_dagrun_info(None), "logical_date", None
                    )
            if "data_interval" not in kwargs:
                logical_date = cast(
                    "DateTime", timezone.coerce_datetime(kwargs["execution_date"])
                )
                if kwargs["run_type"] == DagRunType.MANUAL:
                    data_interval = dag.timetable.infer_manual_data_interval(
                        run_after=logical_date
                    )
                else:
                    data_interval = dag.infer_automated_data_interval(logical_date)
                kwargs["data_interval"] = data_interval

            self.dag_run = dag.create_dagrun(**kwargs)
            task_instances = cast("Iterable[TaskInstance]", self.dag_run.task_instances)
            for ti in task_instances:
                task_id = cast("str", ti.task_id)
                ti.refresh_from_task(dag.get_task(task_id))
            return self.dag_run

        def create_dagrun_after(self, dagrun: DagRun, **kwargs: Any) -> DagRun:
            next_info = self.dag.next_dagrun_info(
                self.dag.get_run_data_interval(dagrun)
            )
            if next_info is None:
                error_msg = f"cannot create run after {dagrun}"
                raise ValueError(error_msg)
            return self.create_dagrun(
                execution_date=next_info.logical_date,
                data_interval=next_info.data_interval,
                **kwargs,
            )

        def __call__(  # noqa: PLR0913
            self,
            *,
            dag_id: str = "test_dag",
            serialized: bool = want_serialized,
            fileloc: str | None = None,
            processor_subdir: str | None = None,
            session: Session | None = None,
            **kwargs: Any,
        ) -> Self:
            if session is None:
                self._own_session = True
                session = settings.Session()

            self.kwargs = kwargs
            self.session = session
            self.start_date = self.kwargs.get("start_date", None)
            default_args: dict[str, Any] | None = kwargs.get("default_args", None)
            if default_args and not self.start_date and "start_date" in default_args:
                self.start_date = default_args.get("start_date")
            if not self.start_date:
                if hasattr(request.module, "DEFAULT_DATE"):
                    self.start_date = getattr(request.module, "DEFAULT_DATE")  # noqa: B009
                else:
                    DEFAULT_DATE = timezone.datetime(2016, 1, 1)  # noqa: N806
                    self.start_date = DEFAULT_DATE
            self.kwargs["start_date"] = self.start_date
            self.dag = DAG(dag_id, **self.kwargs)
            self.dag.fileloc = fileloc or request.module.__file__
            self.want_serialized = serialized
            self.processor_subdir = processor_subdir

            return self

        def cleanup(self) -> None:
            for attempt in run_with_db_retries(logger=self.log):
                with attempt:
                    dag_ids = list(self.dagbag.dag_ids)
                    if not dag_ids:
                        return
                    self.session.rollback()

                    for model in (
                        SerializedDagModel,
                        DagRun,
                        TaskInstance,
                        XCom,
                        DagModel,
                        TaskMap,
                    ):
                        table: sa.Table = model.__table__
                        self.session.execute(
                            sa.delete(model)
                            .where(table.c["dag_id"].in_(dag_ids))
                            .execution_options(synchronize_session=False)
                        )
                    self.session.execute(
                        sa.delete(DatasetEvent)
                        .where(DatasetEvent.source_dag_id.in_(dag_ids))
                        .execution_options(synchronize_session=False)
                    )
                    self.session.commit()
                    if self._own_session:
                        self.session.expunge_all()

    factory = DagFactory()

    try:
        yield factory
    finally:
        factory.cleanup()
        with suppress(AttributeError):
            del factory.session


@pytest.fixture()
def create_dag(create_dag_fixture) -> DagMaker:
    return create_dag_fixture
