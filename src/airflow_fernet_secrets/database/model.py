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
from airflow_fernet_secrets import exceptions as fe
from airflow_fernet_secrets.config.common import ensure_fernet
from airflow_fernet_secrets.utils.re import camel_to_snake

if TYPE_CHECKING:
    from cryptography.fernet import Fernet, MultiFernet
    from sqlalchemy.engine import Connection as SqlalchemyConnection
    from sqlalchemy.engine import Engine
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession
    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Delete, Select, Update

    from airflow.models.connection import Connection as AirflowConnection
    from airflow.models.variable import Variable as AirflowVariable

    from airflow_fernet_secrets.database.connect import SessionMaker


__all__ = ["Version", "Connection", "Variable", "migrate"]

_DATACLASS_ARGS: dict[str, Any]
if sys.version_info >= (3, 10):
    _DATACLASS_ARGS = {"kw_only": True}
else:
    _DATACLASS_ARGS = {}  # pyright: ignore[reportConstantRedefinition]


metadata = sa.MetaData()
mapper_registry = registry(metadata=metadata)


def _get_class(value: Any) -> type[Any]:
    return value if inspect.isclass(value) else type(value)


@dataclass(**_DATACLASS_ARGS)
class Base(ABC):
    __sa_dataclass_metadata_key__: ClassVar[str] = const.SA_DATACLASS_METADATA_KEY
    __table__: ClassVar[sa.Table]
    __tablename__: ClassVar[str]

    id: Mapped[int] = field(
        init=False,
        metadata={
            const.SA_DATACLASS_METADATA_KEY: sa.Column(
                sa.Integer(), primary_key=True, autoincrement=True
            )
        },
    )

    if not TYPE_CHECKING:

        @declared_attr
        def __tablename__(cls) -> str:  # noqa: N805
            return camel_to_snake(cls.__name__)


@mapper_registry.mapped
@dataclass(**_DATACLASS_ARGS)
class Version(Base):
    """version table using in migration"""

    revision: Mapped[str] = field(
        metadata={
            const.SA_DATACLASS_METADATA_KEY: sa.Column(sa.String(100), nullable=False)
        }
    )

    @classmethod
    def get(cls, session: Session | SqlalchemyConnection) -> str:
        """get revision id"""
        stmt = sa.select(cls.revision).where(cls.id == 1)
        return session.scalar(stmt)

    @classmethod
    async def aget(cls, session: AsyncSession | AsyncConnection) -> str:
        """get revision id"""
        stmt = sa.select(cls.revision).where(cls.id == 1)
        return await session.scalar(stmt)


@dataclass(**_DATACLASS_ARGS)
class Encrypted(Base):
    """encrypted base table"""

    __abstract__: ClassVar[bool] = True
    encrypted: Mapped[bytes] = field(
        metadata={
            const.SA_DATACLASS_METADATA_KEY: sa.Column(sa.LargeBinary(), nullable=False)
        }
    )
    """encrypted connection or variable"""

    @staticmethod
    def decrypt(
        value: str | bytes, secret_key: str | bytes | Fernet | MultiFernet
    ) -> bytes:
        """decrypt value using fernet key"""
        secret_key = ensure_fernet(secret_key)
        return secret_key.decrypt(value)

    @staticmethod
    def encrypt(value: Any, secret_key: str | bytes | Fernet | MultiFernet) -> bytes:
        """encrypt value using fernet key"""
        secret_key = ensure_fernet(secret_key)
        as_bytes = _dump(value)
        return secret_key.encrypt(as_bytes)

    def is_exists(self, session: Session) -> bool:
        """check is exists in db"""
        if not hasattr(self, "id") or self.id is None:
            return False

        stmt = self._exists_stmt()
        fetch = session.execute(stmt)
        count: int = fetch.scalars().one()
        return count >= 1

    async def is_aexists(self, session: AsyncSession) -> bool:
        """check is exists in db"""
        if not hasattr(self, "id") or self.id is None:
            return False

        stmt = self._exists_stmt()
        fetch = await session.execute(stmt)
        count = fetch.scalars().one()
        return count >= 1

    def upsert(
        self, session: Session, secret_key: str | bytes | Fernet | MultiFernet
    ) -> None:
        """add new one or update"""
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
        """add new one or update"""
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
        """delete in db if exists"""
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
        """delete in db if exists"""
        secret_key = ensure_fernet(secret_key)
        secret_key.decrypt(self.encrypted)
        if await self.is_aexists(session):
            await session.delete(self)
            return
        stmt = self._delete_stmt().execution_options(synchronize_session="fetch")
        await session.execute(stmt)

    # abc

    @abstractmethod
    def _exists_stmt(self) -> Select:
        """using in `.is_exists()` and `.is_aexists()`"""

    @abstractmethod
    def _upsert_stmt(self) -> Update:
        """using in `.upsert()` and `.aupsert()`"""

    @abstractmethod
    def _delete_stmt(self) -> Delete:
        """using in `.delete()` and `.adelete()`"""


@mapper_registry.mapped
@dataclass(**_DATACLASS_ARGS)
class Connection(Encrypted):
    """encrypted connection table"""

    conn_id: Mapped[str] = field(
        metadata={
            const.SA_DATACLASS_METADATA_KEY: sa.Column(
                sa.String(2**8), index=True, unique=True, nullable=False
            )
        }
    )
    """connection id"""
    conn_type: Mapped[str] = field(
        metadata={
            const.SA_DATACLASS_METADATA_KEY: sa.Column(sa.String(2**8), nullable=False)
        }
    )
    """connection type(<> airflow `Connection.conn_type`)"""

    @classmethod
    def get(cls, session: Session, conn_id: int | str) -> Self | None:
        """get connection in db

        Args:
            session: sync sesion
            conn_id: connection id.
                if int, check `Connection.id`.
                if str, check `Connection.conn_id`.

        Returns:
            connection or null
        """
        if isinstance(conn_id, int):
            return cast("Self", session.get(cls, conn_id))

        stmt = cls._get_stmt(conn_id=conn_id)
        fetch = session.execute(stmt)
        return fetch.scalar_one_or_none()

    @classmethod
    async def aget(cls, session: AsyncSession, conn_id: int | str) -> Self | None:
        """get connection in db

        Args:
            session: async sesion
            conn_id: connection id.
                if int, check `Connection.id`.
                if str, check `Connection.conn_id`.

        Returns:
            connection or null
        """
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
        """check is sql connection"""
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
    """encrypted variable table"""

    key: Mapped[str] = field(
        metadata={
            const.SA_DATACLASS_METADATA_KEY: sa.Column(
                sa.String(2**8), index=True, unique=True, nullable=False
            )
        }
    )
    """variable key"""

    @staticmethod
    @override
    def decrypt(  # pyright: ignore[reportIncompatibleMethodOverride]
        value: str | bytes, secret_key: str | bytes | Fernet | MultiFernet
    ) -> str:
        value = Encrypted.decrypt(value, secret_key)
        return value.decode("utf-8")

    @classmethod
    def get(cls, session: Session, key: int | str) -> Self | None:
        """get variable in db

        Args:
            session: sync sesion
            key: variable key.
                if int, check `Variable.id`.
                if str, check `Variable.key`.

        Returns:
            variable or null
        """
        if isinstance(key, int):
            return cast("Self", session.get(cls, key))
        stmt = sa.select(cls).where(cls.key == key)
        fetch = session.execute(stmt)
        return fetch.scalar_one_or_none()

    @classmethod
    async def aget(cls, session: AsyncSession, key: int | str) -> Self | None:
        """get variable in db

        Args:
            session: async sesion
            key: variable key.
                if int, check `Variable.id`.
                if str, check `Variable.key`.

        Returns:
            variable or null
        """
        if isinstance(key, int):
            return await session.get(cls, key)
        stmt = sa.select(cls).where(cls.key == key)
        fetch = await session.execute(stmt)
        return fetch.scalar_one_or_none()

    @classmethod
    def _get_stmt(cls, key: str) -> Select:
        return sa.select(cls).where(cls.key == key)

    @classmethod
    def from_value(
        cls, key: str, value: Any, secret_key: str | bytes | Fernet | MultiFernet
    ) -> Self:
        """constructor"""
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
    connectable: Engine | SqlalchemyConnection | SessionMaker[Session] | Session,
) -> None:
    """migrate database without alembic"""
    finalize: Callable[[], None] | None = None
    if callable(connectable):
        connectable = connectable()
        finalize = connectable.close

    try:
        _migrate_process(connectable)
    finally:
        if callable(finalize):
            finalize()


def _migrate_process(connectable: Engine | SqlalchemyConnection | Session) -> None:
    engine_or_connection: Engine | SqlalchemyConnection
    connection: SqlalchemyConnection

    if callable(getattr(connectable, "connect", None)):
        engine_or_connection = cast("Engine | SqlalchemyConnection", connectable)
    elif callable(getattr(connectable, "execute", None)):
        engine_or_connection = cast("Session", connectable).connection()
    else:
        error_msg = f"invalid sync connectable type: {type(connectable).__name__}"
        raise fe.FernetSecretsTypeError(error_msg)

    if callable(getattr(engine_or_connection, "dispose", None)):
        connection = cast("Engine", engine_or_connection).connect()
    else:
        connection = cast("SqlalchemyConnection", engine_or_connection)

    revision = _check_migrate_version(connection)

    if not revision:
        from airflow_fernet_secrets.database.revision.init import upgrade

        upgrade(connection)
        return


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

    error_msg = f"invalid value type: {type(value).__name__}"
    raise fe.FernetSecretsTypeError(error_msg)


def _check_migrate_version(conn: SqlalchemyConnection) -> str | None:
    insp = sa.inspect(conn)
    tablenames = set(insp.get_table_names())
    version_tablename = Version.__tablename__
    if version_tablename not in tablenames:
        return None

    return Version.get(conn)
