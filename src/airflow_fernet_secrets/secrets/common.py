from __future__ import annotations

import json
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager, contextmanager, suppress
from functools import cached_property
from typing import TYPE_CHECKING, AsyncGenerator, Generator, Generic, Literal, cast

import sqlalchemy as sa
from sqlalchemy.exc import ResourceClosedError
from typing_extensions import TypeVar, override

from airflow_fernet_secrets.config.common import ensure_fernet
from airflow_fernet_secrets.const import CONNECTION_BEGIN_INFO_KEY
from airflow_fernet_secrets.database.connect import (
    create_sqlite_url,
    ensure_sqlite_async_engine,
    ensure_sqlite_sync_engine,
    enter_async_database,
    enter_sync_database,
)
from airflow_fernet_secrets.database.model import Connection as FernetConnection
from airflow_fernet_secrets.database.model import Variable as FernetVariable
from airflow_fernet_secrets.database.model import migrate

if TYPE_CHECKING:
    from logging import Logger

    from cryptography.fernet import Fernet, MultiFernet
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.result import Result
    from sqlalchemy.engine.url import URL
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
    from sqlalchemy.orm import Session

    from airflow.secrets import BaseSecretsBackend

    from airflow_fernet_secrets._typeshed import PathType
    from airflow_fernet_secrets.connection import ConnectionDict
    from airflow_fernet_secrets.log.common import CommonLoggingMixin

    class BaseFernetLocalSecretsBackend(
        BaseSecretsBackend, CommonLoggingMixin, ABC
    ): ...

else:

    class BaseFernetLocalSecretsBackend(ABC): ...  # noqa: B024


__all__ = ["CommonFernetLocalSecretsBackend"]


ConnectionT = TypeVar("ConnectionT", infer_variance=True)


class CommonFernetLocalSecretsBackend(
    BaseFernetLocalSecretsBackend, Generic[ConnectionT]
):
    """base fernet secrets backend"""

    load_backend_file: staticmethod[[Logger], str]
    load_secret_key: staticmethod[[Logger], MultiFernet]

    def __init__(
        self,
        *,
        fernet_secrets_key: str | bytes | Fernet | MultiFernet | None = None,
        fernet_secrets_backend_file_path: PathType | None = None,
    ) -> None:
        super().__init__()
        self.fernet_secrets_backend_file = fernet_secrets_backend_file_path

        self._fernet_secrets_key = (
            None if fernet_secrets_key is None else ensure_fernet(fernet_secrets_key)
        )

    @cached_property
    def _backend_sync_url(self) -> URL:
        if self.fernet_secrets_backend_file is not None:
            return create_sqlite_url(self.fernet_secrets_backend_file, is_async=False)
        file = self.load_backend_file(self.log)
        return create_sqlite_url(file, is_async=False)

    @cached_property
    def _backend_async_url(self) -> URL:
        if self.fernet_secrets_backend_file is not None:
            return create_sqlite_url(self.fernet_secrets_backend_file, is_async=True)
        file = self.load_backend_file(self.log)
        return create_sqlite_url(file, is_async=True)

    @cached_property
    def _backend_sync_engine(self) -> Engine:
        engine = ensure_sqlite_sync_engine(self._backend_sync_url)
        migrate(engine)
        return engine

    @cached_property
    def _backend_async_engine(self) -> AsyncEngine:
        engine = ensure_sqlite_async_engine(self._backend_async_url)
        url = engine.url.set(drivername="sqlite+pysqlite")
        sync_engine = ensure_sqlite_sync_engine(url)
        migrate(sync_engine)
        return engine

    def _secret(self) -> MultiFernet:
        if self._fernet_secrets_key is not None:
            return self._fernet_secrets_key
        return self.load_secret_key(self.log)

    @override
    def get_conn_value(self, conn_id: str) -> str | None:
        with enter_sync_database(self._backend_sync_engine) as session:
            value = FernetConnection.get(session, conn_id)
            return self._get_conn_value_process(conn_id=conn_id, value=value)

    def has_connection(self, conn_id: str) -> bool:
        """check has connection"""
        with enter_sync_database(self._backend_sync_engine) as session:
            count: int = session.scalar(
                sa.select(sa.func.count(FernetConnection.id)).where(
                    FernetConnection.conn_id == conn_id
                )
            )
            return count > 0

    async def ahas_connection(self, conn_id: str) -> bool:
        """check has connection"""
        async with enter_async_database(self._backend_async_engine) as session:
            count: int = await session.scalar(
                sa.select(sa.func.count(FernetConnection.id)).where(
                    FernetConnection.conn_id == conn_id
                )
            )
            return count > 0

    async def aget_conn_value(self, conn_id: str) -> str | None:
        """get conn value from backend"""
        async with enter_async_database(self._backend_async_engine) as session:
            value = await FernetConnection.aget(session, conn_id)
            return self._get_conn_value_process(conn_id=conn_id, value=value)

    def _get_conn_value_process(
        self, conn_id: str, value: FernetConnection | None
    ) -> str | None:
        if value is None:
            return None
        value = self._validate_connection(conn_id=conn_id, connection=value, when="get")
        return value.encrypted.decode("utf-8")

    def set_conn_value(self, conn_id: str, conn_type: str, value: str | bytes) -> None:
        """set conn value to backend"""
        if isinstance(value, str):
            value = value.encode("utf-8")
        secret_key = self._secret()
        with enter_sync_database(self._backend_sync_engine) as session:
            with _sync_transact(session):
                connection = FernetConnection.get(session, conn_id=conn_id)
                connection = self._set_conn_value_process(
                    conn_id=conn_id,
                    conn_type=conn_type,
                    value=value,
                    connection=connection,
                )
                connection.upsert(session, secret_key=secret_key)
                session.commit()

    async def aset_conn_value(
        self, conn_id: str, conn_type: str, value: str | bytes
    ) -> None:
        """set conn value to backend"""
        if isinstance(value, str):
            value = value.encode("utf-8")
        secret_key = self._secret()
        async with enter_async_database(self._backend_async_engine) as session:
            async with _async_transact(session):
                connection = await FernetConnection.aget(session, conn_id=conn_id)
                connection = self._set_conn_value_process(
                    conn_id=conn_id,
                    conn_type=conn_type,
                    value=value,
                    connection=connection,
                )
                await connection.aupsert(session, secret_key=secret_key)
                await session.commit()

    def _set_conn_value_process(
        self,
        conn_id: str,
        conn_type: str,
        value: bytes,
        connection: FernetConnection | None,
    ) -> FernetConnection:
        if connection is None:
            connection = FernetConnection(
                encrypted=value, conn_id=conn_id, conn_type=conn_type
            )
        else:
            secret_key = self._secret()
            FernetConnection.decrypt(connection.encrypted, secret_key)
            connection.encrypted = value
        return self._validate_connection(
            conn_id=conn_id, connection=connection, when="set"
        )

    @override
    def deserialize_connection(self, conn_id: str, value: str | bytes) -> ConnectionT:  # pyright: ignore[reportIncompatibleMethodOverride]
        secret_key = self._secret()
        value = secret_key.decrypt(value)
        as_dict = json.loads(value)
        as_dict = self._validate_connection_dict(
            conn_id=conn_id, connection=as_dict, when="deserialize"
        )
        return self._deserialize_connection(conn_id=conn_id, connection=as_dict)

    def serialize_connection(
        self, conn_id: str, connection: ConnectionT
    ) -> str | bytes:
        """serialize connection as string or bytes"""
        secret_key = self._secret()
        as_dict = self._serialize_connection(conn_id=conn_id, connection=connection)
        as_dict = self._validate_connection_dict(
            conn_id=conn_id, connection=as_dict, when="serialize"
        )
        value = json.dumps(as_dict)
        return secret_key.encrypt(value.encode("utf-8"))

    @override
    def get_connection(self, conn_id: str) -> ConnectionT | None:  # pyright: ignore[reportIncompatibleMethodOverride]
        value = self.get_conn_value(conn_id)
        if value is None:
            return None

        return self.deserialize_connection(conn_id, value)

    async def aget_connection(self, conn_id: str) -> ConnectionT | None:
        """get connection or null from backend"""
        value = await self.aget_conn_value(conn_id)
        if value is None:
            return None

        return self.deserialize_connection(conn_id, value)

    def set_connection(self, conn_id: str, connection: ConnectionT) -> None:
        """set connection to backend"""
        conn_type = self._get_conn_type(connection)
        value = self.serialize_connection(conn_id, connection)
        self.set_conn_value(conn_id=conn_id, conn_type=conn_type, value=value)

    async def aset_connection(self, conn_id: str, connection: ConnectionT) -> None:
        """set connection to backend"""
        conn_type = self._get_conn_type(connection)
        value = self.serialize_connection(conn_id, connection)
        await self.aset_conn_value(conn_id=conn_id, conn_type=conn_type, value=value)

    def delete_connection(self, conn_id: str) -> None:
        """delete connection in backend"""
        secret_key = self._secret()
        with enter_sync_database(self._backend_sync_engine) as session:
            with _sync_transact(session):
                value = FernetConnection.get(session, conn_id)
                if value is None:
                    return
                value.delete(session, secret_key=secret_key)
                session.commit()

    async def adelete_connection(self, conn_id: str) -> None:
        """delete connection in backend"""
        secret_key = self._secret()
        async with enter_async_database(self._backend_async_engine) as session:
            async with _async_transact(session):
                value = await FernetConnection.aget(session, conn_id)
                if value is None:
                    return
                await value.adelete(session, secret_key=secret_key)
                await session.commit()

    @override
    def get_variable(self, key: str) -> str | None:
        with enter_sync_database(self._backend_sync_engine) as session:
            value = FernetVariable.get(session, key)
            if value is None:
                return None
            session.expunge(value)

        fernet = self._secret()
        return FernetVariable.decrypt(value.encrypted, fernet)

    async def aget_variable(self, key: str) -> str | None:
        """get variable from backend"""
        async with enter_async_database(self._backend_async_engine) as session:
            value = await FernetVariable.aget(session, key)
            if value is None:
                return None
            session.expunge(value)

        fernet = self._secret()
        return FernetVariable.decrypt(value.encrypted, fernet)

    def set_variable(self, key: str, value: str) -> None:
        """set variable to backend"""
        secret_key = self._secret()
        with enter_sync_database(self._backend_sync_engine) as session:
            with _sync_transact(session):
                as_bytes = FernetVariable.encrypt(value, secret_key)
                variable = FernetVariable.get(session, key)
                if variable is None:
                    variable = FernetVariable(encrypted=as_bytes, key=key)
                else:
                    variable.encrypted = as_bytes
                variable.upsert(session, secret_key=secret_key)
                session.commit()

    async def aset_variable(self, key: str, value: str) -> None:
        """set variable to backend"""
        secret_key = self._secret()
        async with enter_async_database(self._backend_async_engine) as session:
            async with _async_transact(session):
                as_bytes = FernetVariable.encrypt(value, secret_key)
                variable = await FernetVariable.aget(session, key)
                if variable is None:
                    variable = FernetVariable(encrypted=as_bytes, key=key)
                else:
                    variable.encrypted = as_bytes
                await variable.aupsert(session, secret_key=secret_key)
                await session.commit()

    def delete_variable(self, key: str) -> None:
        """delete variable to backend"""
        secret_key = self._secret()
        with enter_sync_database(self._backend_sync_engine) as session:
            with _sync_transact(session):
                variable = FernetVariable.get(session, key=key)
                if variable is None:
                    return
                variable.delete(session, secret_key=secret_key)
                session.commit()

    async def adelete_variable(self, key: str) -> None:
        """delete variable to backend"""
        secret_key = self._secret()
        async with enter_async_database(self._backend_async_engine) as session:
            async with _async_transact(session):
                variable = await FernetVariable.aget(session, key=key)
                if variable is None:
                    return
                await variable.adelete(session, secret_key=secret_key)
                await session.commit()

    def has_variable(self, key: str) -> bool:
        """check has variable"""
        with enter_sync_database(self._backend_sync_engine) as session:
            count: int = session.scalar(
                sa.select(sa.func.count(FernetVariable.id)).where(
                    FernetVariable.key == key
                )
            )
            return count > 0

    async def ahas_variable(self, key: str) -> bool:
        """check has variable"""
        async with enter_async_database(self._backend_async_engine) as session:
            count: int = await session.scalar(
                sa.select(sa.func.count(FernetVariable.id)).where(
                    FernetVariable.key == key
                )
            )
            return count > 0

    @override
    def get_config(self, key: str) -> str | None:
        return None

    def rotate(self) -> None:
        """convert connections and variables to new fernet key from old."""
        self._rotate_connections()
        self._rotate_variables()

    async def arotate(self) -> None:
        """convert connections and variables to new fernet key from old."""
        await self._arotate_connections()
        await self._arotate_variables()

    def _rotate_connections(self) -> None:
        secret_key = self._secret()
        do_rorate = False
        limit = 100
        offset = 0
        with enter_sync_database(self._backend_sync_engine) as session:
            with _sync_transact(session):
                while True:
                    fetch: Result = session.execute(
                        sa.select(FernetConnection).limit(limit).offset(offset)
                    )
                    connections = cast("list[FernetConnection]", fetch.scalars().all())
                    if not connections:
                        break

                    for connection in connections:
                        connection.encrypted = secret_key.rotate(connection.encrypted)
                        connection.upsert(session, secret_key=secret_key)
                    session.flush()
                    do_rorate = True
                    offset += limit

                if do_rorate:
                    session.commit()

    def _rotate_variables(self) -> None:
        do_rorate = False
        secret_key = self._secret()
        limit = 100
        offset = 0
        with enter_sync_database(self._backend_sync_engine) as session:
            with _sync_transact(session):
                while True:
                    fetch: Result = session.execute(
                        sa.select(FernetVariable).limit(limit).offset(offset)
                    )
                    variables = cast("list[FernetVariable]", fetch.scalars().all())
                    if not variables:
                        break

                    for variable in variables:
                        variable.encrypted = secret_key.rotate(variable.encrypted)
                        variable.upsert(session, secret_key=secret_key)
                    session.flush()
                    do_rorate = True
                    offset += limit

                if do_rorate:
                    session.commit()

    async def _arotate_connections(self) -> None:
        secret_key = self._secret()
        do_rorate = False
        limit = 100
        offset = 0
        async with enter_async_database(self._backend_async_engine) as session:
            async with _async_transact(session):
                while True:
                    fetch: Result = await session.execute(
                        sa.select(FernetConnection).limit(limit).offset(offset)
                    )
                    connections = cast("list[FernetConnection]", fetch.scalars().all())
                    if not connections:
                        break

                    for connection in connections:
                        connection.encrypted = secret_key.rotate(connection.encrypted)
                        await connection.aupsert(session, secret_key=secret_key)
                    await session.flush()
                    do_rorate = True
                    offset += limit

                if do_rorate:
                    await session.commit()

    async def _arotate_variables(self) -> None:
        do_rorate = False
        secret_key = self._secret()
        limit = 100
        offset = 0
        async with enter_async_database(self._backend_async_engine) as session:
            async with _async_transact(session):
                while True:
                    fetch: Result = await session.execute(
                        sa.select(FernetVariable).limit(limit).offset(offset)
                    )
                    variables = cast("list[FernetVariable]", fetch.scalars().all())
                    if not variables:
                        break

                    for variable in variables:
                        variable.encrypted = secret_key.rotate(variable.encrypted)
                        await variable.aupsert(session, secret_key=secret_key)
                    await session.flush()
                    do_rorate = True
                    offset += limit

                if do_rorate:
                    await session.commit()

    # validate
    def _validate_connection(
        self,
        conn_id: str,  # noqa: ARG002
        connection: FernetConnection,
        when: Literal["get", "set"],  # noqa: ARG002
    ) -> FernetConnection:
        return connection

    def _validate_connection_dict(
        self,
        conn_id: str,  # noqa: ARG002
        connection: ConnectionDict,
        when: Literal["serialize", "deserialize"],  # noqa: ARG002
    ) -> ConnectionDict:
        return connection

    # abc
    @abstractmethod
    def _deserialize_connection(
        self, conn_id: str, connection: ConnectionDict
    ) -> ConnectionT: ...

    @abstractmethod
    def _serialize_connection(
        self, conn_id: str, connection: ConnectionT
    ) -> ConnectionDict: ...

    @abstractmethod
    def _get_conn_type(self, connection: ConnectionT) -> str: ...


@contextmanager
def _sync_transact(session: Session) -> Generator[Session, None, None]:
    conn = session.connection()
    is_begin = conn.info.get(CONNECTION_BEGIN_INFO_KEY, False)
    if is_begin is False:
        conn.execute(sa.text("BEGIN IMMEDIATE;"))
        conn.info[CONNECTION_BEGIN_INFO_KEY] = True

    try:
        yield session
    finally:
        with suppress(ResourceClosedError):
            conn.info[CONNECTION_BEGIN_INFO_KEY] = False


@asynccontextmanager
async def _async_transact(session: AsyncSession) -> AsyncGenerator[AsyncSession, None]:
    conn = await session.connection()
    is_begin = conn.info.get(CONNECTION_BEGIN_INFO_KEY, False)
    if is_begin is False:
        await conn.execute(sa.text("BEGIN IMMEDIATE;"))
        conn.info[CONNECTION_BEGIN_INFO_KEY] = True

    try:
        yield session
    finally:
        with suppress(ResourceClosedError):
            conn.info[CONNECTION_BEGIN_INFO_KEY] = False
