from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, AsyncGenerator, Generator, cast

import sqlalchemy as sa
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
from sqlalchemy.engine import Connection, Engine, create_engine
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_scoped_session,
    create_async_engine,
)
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from typing_extensions import TypeAlias

if sa.__version__ >= "2.0.0":
    from sqlalchemy.ext.asyncio import async_sessionmaker  # type: ignore
else:
    async_sessionmaker: TypeAlias = sessionmaker  # noqa: PYI042

if TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import Dialect

    from airflow_fernet_secrets.core.typeshed import PathType

__all__ = [
    "create_sqlite_url",
    "ensure_sqlite_url",
    "ensure_sqlite_sync_engine",
    "ensure_sqlite_async_engine",
    "enter_sync_database",
    "enter_async_database",
]


def create_sqlite_url(file: PathType, *, is_async: bool = False, **kwargs: Any) -> URL:
    url = URL.create(drivername="sqlite+pysqlite")
    if is_async:
        url = url.set(drivername="sqlite+aiosqlite")

    url = url.set(**kwargs).set(database=str(file))
    return ensure_sqlite_url(url, is_async=is_async)


def ensure_sqlite_url(url: str | URL, *, is_async: bool = False) -> URL:
    url = cast("URL", make_url(url))
    dialect: type[Dialect] = url.get_dialect()
    if getattr(dialect, "name", "") != sqlite_dialect.name:
        raise NotImplementedError

    if getattr(dialect, "is_async", False) is not is_async:
        driver = url.get_driver_name()
        if not driver and is_async:
            url = url.set(drivername=f"{sqlite_dialect.name}+aiosqlite")
        else:
            raise NotImplementedError

    return url


def ensure_sqlite_sync_engine(
    connectable_or_url: Engine
    | Connection
    | sessionmaker
    | scoped_session
    | Session
    | URL
    | str,
) -> Engine:
    if isinstance(connectable_or_url, (Engine, Connection)):
        return connectable_or_url.engine
    if isinstance(connectable_or_url, (str, URL)):
        connectable_or_url = ensure_sqlite_url(connectable_or_url, is_async=False)
    if isinstance(connectable_or_url, URL):
        return cast("Engine", create_engine(connectable_or_url))
    if isinstance(connectable_or_url, Session):
        conn = connectable_or_url.connection()
        return ensure_sqlite_sync_engine(conn)
    if isinstance(connectable_or_url, (sessionmaker, scoped_session)):
        session: Session = connectable_or_url()
        try:
            return ensure_sqlite_sync_engine(session)
        finally:
            session.close()
    raise NotImplementedError


async def ensure_sqlite_async_engine(
    connectable_or_url: AsyncEngine
    | AsyncConnection
    | async_sessionmaker  # type: ignore
    | async_scoped_session
    | AsyncSession
    | URL
    | str,
) -> AsyncEngine:
    if isinstance(connectable_or_url, (AsyncEngine, AsyncConnection)):
        return getattr(connectable_or_url, "engine")  # noqa: B009
    if isinstance(connectable_or_url, (str, URL)):
        connectable_or_url = ensure_sqlite_url(connectable_or_url, is_async=True)
    if isinstance(connectable_or_url, URL):
        return create_async_engine(connectable_or_url)
    if isinstance(connectable_or_url, AsyncSession):
        conn = await connectable_or_url.connection()
        return await ensure_sqlite_async_engine(conn)
    if isinstance(connectable_or_url, (async_sessionmaker, async_scoped_session)):
        session: AsyncSession = connectable_or_url()
        try:
            return await ensure_sqlite_async_engine(session)
        finally:
            await session.close()
    raise NotImplementedError


@contextmanager
def enter_sync_database(
    connectable: Engine | Connection | sessionmaker | scoped_session | Session,
) -> Generator[Session, None, None]:
    session: Session
    if isinstance(connectable, Session):
        session = connectable
    elif isinstance(connectable, (Engine, Connection)):
        session = Session(connectable)
    elif isinstance(connectable, (sessionmaker, scoped_session)):
        session = connectable()
    else:
        raise NotImplementedError

    try:
        yield session
    except:
        session.rollback()
        raise
    finally:
        session.close()


@asynccontextmanager
async def enter_async_database(
    connectable: AsyncEngine
    | AsyncConnection
    | async_sessionmaker  # type: ignore
    | async_scoped_session
    | AsyncSession,
) -> AsyncGenerator[AsyncSession, None]:
    session: AsyncSession
    if isinstance(connectable, AsyncSession):
        session = connectable
    elif isinstance(connectable, (AsyncEngine, AsyncConnection)):
        session = AsyncSession(connectable)
    elif isinstance(connectable, (async_sessionmaker, async_scoped_session)):
        session = connectable()
    else:
        raise NotImplementedError

    try:
        yield session
    except:
        await session.rollback()
        raise
    finally:
        await session.close()
