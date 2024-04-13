from __future__ import annotations

import inspect
import json
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, ClassVar, cast

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, declared_attr, registry
from typing_extensions import Self, TypeGuard, override

from airflow_fernet_secrets import const
from airflow_fernet_secrets.config.common import ensure_fernet
from airflow_fernet_secrets.utils.re import camel_to_snake

if TYPE_CHECKING:
    from airflow.models.connection import Connection as AirflowConnection
    from airflow.models.variable import Variable as AirflowVariable
    from cryptography.fernet import Fernet, MultiFernet
    from sqlalchemy.engine import Connection as SqlalchemyConnection
    from sqlalchemy.engine import Engine
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.orm import Session, scoped_session, sessionmaker
    from sqlalchemy.sql import Delete, Select, Update


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
class Base(ABC):
    __sa_dataclass_metadata_key__: ClassVar[str] = const.SA_DATACLASS_METADATA_KEY
    __table__: ClassVar[sa.Table]
    __tablename__: ClassVar[str] = ""

    if not TYPE_CHECKING:

        @declared_attr
        def __tablename__(self) -> str:
            cls = _get_class(self)
            return camel_to_snake(cls.__name__)

    id: Mapped[int] = field(
        init=False,
        metadata={
            const.SA_DATACLASS_METADATA_KEY: sa.Column(
                sa.Integer(), primary_key=True, autoincrement=True
            )
        },
    )


@dataclass(**_DATACLASS_ARGS)
class Encrypted(Base):
    __abstract__: ClassVar[bool] = True
    encrypted: Mapped[bytes] = field(
        metadata={const.SA_DATACLASS_METADATA_KEY: sa.Column(sa.LargeBinary())}
    )

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

    def is_exists(self, session: Session) -> bool:
        if not hasattr(self, "id") or self.id is None:
            return False

        stmt = self._exists_stmt()
        fetch = session.execute(stmt)
        count: int = fetch.scalars().one()
        return count >= 1

    async def is_aexists(self, session: AsyncSession) -> bool:
        if not hasattr(self, "id") or self.id is None:
            return False

        stmt = self._exists_stmt()
        fetch = await session.execute(stmt)
        count = fetch.scalars().one()
        return count >= 1

    def upsert(
        self, session: Session, secret_key: str | bytes | Fernet | MultiFernet
    ) -> None:
        secret_key = ensure_fernet(secret_key)
        secret_key.decrypt(self.encrypted)
        if not self.is_exists(session):
            session.add(self)
            return
        stmt = self._upsert_stmt().execution_options(synchronize_session="fetch")
        session.execute(stmt)

    async def aupsert(
        self, session: AsyncSession, secret_key: str | bytes | Fernet | MultiFernet
    ) -> None:
        secret_key = ensure_fernet(secret_key)
        secret_key.decrypt(self.encrypted)
        if not await self.is_aexists(session):
            session.add(self)
            return
        stmt = self._upsert_stmt().execution_options(synchronize_session="fetch")
        await session.execute(stmt)

    def delete(
        self, session: Session, secret_key: str | bytes | Fernet | MultiFernet
    ) -> None:
        secret_key = ensure_fernet(secret_key)
        secret_key.decrypt(self.encrypted)
        if self.is_exists(session):
            session.delete(self)
            return
        stmt = self._delete_stmt().execution_options(synchronize_session="fetch")
        session.execute(stmt)

    async def adelete(
        self, session: AsyncSession, secret_key: str | bytes | Fernet | MultiFernet
    ) -> None:
        secret_key = ensure_fernet(secret_key)
        secret_key.decrypt(self.encrypted)
        if await self.is_aexists(session):
            await session.delete(self)
            return
        stmt = self._delete_stmt().execution_options(synchronize_session="fetch")
        await session.execute(stmt)

    # abc

    @abstractmethod
    def _exists_stmt(self) -> Select: ...

    @abstractmethod
    def _upsert_stmt(self) -> Update: ...

    @abstractmethod
    def _delete_stmt(self) -> Delete: ...


@mapper_registry.mapped
@dataclass(**_DATACLASS_ARGS)
class Connection(Encrypted):
    conn_id: Mapped[str] = field(
        metadata={
            const.SA_DATACLASS_METADATA_KEY: sa.Column(
                sa.String(2**8), index=True, unique=True, nullable=False
            )
        }
    )
    conn_type: Mapped[str] = field(
        metadata={
            const.SA_DATACLASS_METADATA_KEY: sa.Column(sa.String(2**8), nullable=False)
        }
    )

    @classmethod
    def get(cls, session: Session, conn_id: int | str) -> Self | None:
        if isinstance(conn_id, int):
            return cast("Self", session.get(cls, conn_id))

        stmt = cls._get_stmt(conn_id=conn_id)
        fetch = session.execute(stmt)
        return fetch.scalar_one_or_none()

    @classmethod
    async def aget(cls, session: AsyncSession, conn_id: int | str) -> Self | None:
        if isinstance(conn_id, int):
            return await session.get(cls, conn_id)

        stmt = cls._get_stmt(conn_id=conn_id)
        fetch = await session.execute(stmt)
        return fetch.scalar_one_or_none()

    @classmethod
    def _get_stmt(cls, conn_id: int | str) -> Select:
        return sa.select(cls).where(cls.conn_id == conn_id)

    @property
    def is_sql_connection(self) -> bool:
        return (
            self.conn_type is not None
            and self.conn_type.lower().strip() == const.SQL_CONN_TYPE
        )

    # abc

    @override
    def _exists_stmt(self) -> Select:
        model = type(self)
        return sa.select(sa.func.count().label("count")).where(
            model.conn_id == self.conn_id
        )

    @override
    def _upsert_stmt(self) -> Update:
        model = type(self)
        table: sa.Table = self.__table__
        pks: set[str] = set(table.primary_key.columns.keys())
        columns: list[str] = [x for x in table.columns.keys() if x not in pks]  # noqa: SIM118
        return (
            sa.update(model)
            .where(model.conn_id == self.conn_id)
            .values(**{x: getattr(self, x) for x in columns})
        )

    @override
    def _delete_stmt(self) -> Delete:
        model = type(self)
        return sa.delete(model).where(model.conn_id == self.conn_id)


@mapper_registry.mapped
@dataclass(**_DATACLASS_ARGS)
class Variable(Encrypted):
    key: Mapped[str] = field(
        metadata={
            const.SA_DATACLASS_METADATA_KEY: sa.Column(
                sa.String(2**8), index=True, unique=True, nullable=False
            )
        }
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
        fetch = session.execute(stmt)
        return fetch.scalar_one_or_none()

    @classmethod
    def _get_stmt(cls, key: str) -> Select:
        return sa.select(cls).where(cls.key == key)

    @classmethod
    async def aget(cls, session: AsyncSession, key: int | str) -> Self | None:
        if isinstance(key, int):
            return await session.get(cls, key)
        stmt = sa.select(cls).where(cls.key == key)
        fetch = await session.execute(stmt)
        return fetch.scalar_one_or_none()

    @classmethod
    def from_value(
        cls, key: str, value: Any, secret_key: str | bytes | Fernet | MultiFernet
    ) -> Self:
        secret_key = ensure_fernet(secret_key)
        as_bytes = cls.encrypt(value, secret_key)
        return cls(key=key, encrypted=as_bytes)

    # abc

    @override
    def _exists_stmt(self) -> Select:
        model = type(self)
        return sa.select(sa.func.count().label("count")).where(model.key == self.key)

    @override
    def _upsert_stmt(self) -> Update:
        model = type(self)
        table: sa.Table = self.__table__
        pks: set[str] = set(table.primary_key.columns.keys())
        columns: list[str] = [x for x in table.columns.keys() if x not in pks]  # noqa: SIM118
        return (
            sa.update(model)
            .where(model.key == self.key)
            .values(**{x: getattr(self, x) for x in columns})
        )

    @override
    def _delete_stmt(self) -> Delete:
        model = type(self)
        return sa.delete(model).where(model.key == self.key)


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
    return cast(str, value.val)


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
