from __future__ import annotations

import inspect
import json
import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, ClassVar, cast

import sqlalchemy as sa
from sqlalchemy.orm import declared_attr, registry
from typing_extensions import Self, TypeGuard, override

from airflow_fernet_secrets.core.config.common import ensure_fernet
from airflow_fernet_secrets.core.utils.re import camel_to_snake

if TYPE_CHECKING:
    from airflow.models.connection import Connection as AirflowConnection
    from airflow.models.variable import Variable as AirflowVariable
    from cryptography.fernet import Fernet, MultiFernet
    from sqlalchemy.engine import Connection as SqlalchemyConnection
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.result import Result
    from sqlalchemy.orm import Session, scoped_session, sessionmaker


__all__ = ["Connection", "Variable", "migrate"]

_DATACLASS_ARGS: dict[str, Any]
if sys.version_info >= (3, 10):
    _DATACLASS_ARGS = {"kw_only": True}
else:
    _DATACLASS_ARGS = {}


metadata = sa.MetaData()
mapper_registry = registry(metadata=metadata)


def _get_class(value: Any) -> type[Any]:
    return value if inspect.isclass(value) else type(value)


@dataclass(**_DATACLASS_ARGS)
class Base:
    __sa_dataclass_metadata_key__: ClassVar[str] = "sa"
    __table__: ClassVar[sa.Table]
    __tablename__: ClassVar[str]

    if not TYPE_CHECKING:

        @declared_attr
        def __tablename__(self) -> str:
            cls = _get_class(self)
            return camel_to_snake(cls.__name__)

    id: int = field(
        init=False,
        metadata={"sa": sa.Column(sa.Integer(), primary_key=True, autoincrement=True)},
    )


@dataclass(**_DATACLASS_ARGS)
class Encrypted(Base):
    __abstract__: ClassVar[bool] = True
    encrypted: bytes = field(metadata={"sa": sa.Column(sa.LargeBinary())})

    @staticmethod
    def decrypt(
        value: str | bytes, secret_key: str | bytes | Fernet | MultiFernet
    ) -> bytes:
        secret_key = ensure_fernet(secret_key)
        return secret_key.decrypt(value)

    @staticmethod
    def encrypt(value: Any, secret_key: str | bytes | Fernet | MultiFernet) -> bytes:
        secret_key = ensure_fernet(secret_key)
        as_bytes = _dump(value)
        return secret_key.encrypt(as_bytes)


@mapper_registry.mapped
@dataclass(**_DATACLASS_ARGS)
class Connection(Encrypted):
    conn_id: str = field(
        metadata={"sa": sa.Column(sa.String(2**8), index=True, unique=True)}
    )
    conn_type: str | None = field(
        metadata={"sa": sa.Column(sa.String(2**8), nullable=True)}
    )

    @classmethod
    def get(
        cls, session: Session, conn_id: int | str, conn_type: str | None = None
    ) -> Self | None:
        if isinstance(conn_id, int):
            return cast("Self", session.get(cls, conn_id))
        stmt = sa.select(cls).where(cls.conn_id == conn_id)
        if conn_type:
            stmt = stmt.where(cls.conn_type == conn_type)

        fetch: Result = session.execute(stmt)
        return fetch.scalar_one_or_none()

    @property
    def is_sql_connection(self) -> bool:
        return self.conn_type is not None and self.conn_type.lower().strip() == "sql"


@mapper_registry.mapped
@dataclass(**_DATACLASS_ARGS)
class Variable(Encrypted):
    key: str = field(
        metadata={"sa": sa.Column(sa.String(2**8), index=True, unique=True)}
    )

    @staticmethod
    @override
    def decrypt(
        value: str | bytes, secret_key: str | bytes | Fernet | MultiFernet
    ) -> str:
        value = Encrypted.decrypt(value, secret_key)
        return value.decode("utf-8")

    @classmethod
    def get(cls, session: Session, key: int | str) -> Self | None:
        if isinstance(key, int):
            return cast("Self", session.get(cls, key))
        stmt = sa.select(cls).where(cls.key == key)
        fetch: Result = session.execute(stmt)
        return fetch.scalar_one_or_none()

    @classmethod
    def from_value(
        cls, key: str, value: Any, secret_key: str | bytes | Fernet | MultiFernet
    ) -> Self:
        secret_key = ensure_fernet(secret_key)
        as_bytes = cls.encrypt(value, secret_key)
        return cls(key=key, encrypted=as_bytes)


def migrate(
    connectable: Engine
    | SqlalchemyConnection
    | sessionmaker
    | scoped_session
    | Session,
) -> None:
    engine_or_connection: Engine | SqlalchemyConnection

    finalize: Callable[[], None] | None = None
    if callable(connectable):
        connectable = cast("sessionmaker | scoped_session", connectable)()
        finalize = connectable.close

    try:
        if callable(getattr(connectable, "connect", None)):
            engine_or_connection = cast("Engine | SqlalchemyConnection", connectable)
        elif callable(getattr(connectable, "execute", None)):
            engine_or_connection = cast("Session", connectable).connection()
        else:
            raise NotImplementedError

        commit = getattr(engine_or_connection, "commit", None)
        metadata.create_all(
            engine_or_connection,
            [Connection.__table__, Variable.__table__],
            checkfirst=True,
        )
        if callable(commit):
            commit()

    finally:
        if callable(finalize):
            finalize()


def _check_airflow_connection_instance(value: Any) -> TypeGuard[AirflowConnection]:
    cls = _get_class(value)
    if cls is value or cls.__name__ != "Connection" or not hasattr(cls, "__table__"):
        return False

    return any(
        _fullname(x) == "airflow.models.connection.Connection" for x in cls.mro()
    )


def _check_airflow_variable_instance(value: Any) -> TypeGuard[AirflowVariable]:
    cls = _get_class(value)
    if cls is value or cls.__name__ != "Variable" or not hasattr(cls, "__table__"):
        return False

    return any(_fullname(x) == "airflow.models.variable.Variable" for x in cls.mro())


def _run_as_json(value: AirflowConnection) -> str:
    return value.as_json()


def _get_variable(value: AirflowVariable) -> str:
    return value.val


def _fullname(value: type[Any]) -> str:
    return value.__module__ + "." + value.__qualname__


def _dump(value: Any) -> bytes:
    if _check_airflow_connection_instance(value):
        value = _run_as_json(value)
    elif _check_airflow_variable_instance(value):
        value = _get_variable(value)
    elif isinstance(value, dict):
        value = json.dumps(value)

    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")

    raise NotImplementedError
