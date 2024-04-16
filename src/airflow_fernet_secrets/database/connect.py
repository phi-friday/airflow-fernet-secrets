from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Callable,
    Generator,
    Protocol,
    overload,
    runtime_checkable,
)

import sqlalchemy as sa
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
from sqlalchemy.engine import Connection, Engine, create_engine
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.event import listen
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    create_async_engine,
)
from sqlalchemy.orm import Session
from typing_extensions import TypeVar

if TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import Dialect, _DBAPIConnection

    from airflow_fernet_secrets._typeshed import PathType

__all__ = [
    "SessionMaker",
    "create_sqlite_url",
    "ensure_sqlite_url",
    "ensure_sqlite_engine",
    "ensure_sqlite_sync_engine",
    "ensure_sqlite_async_engine",
    "enter_sync_database",
    "enter_async_database",
]

SessionT = TypeVar("SessionT", bound="Session | AsyncSession")


@runtime_checkable
class SessionMaker(Protocol[SessionT]):
    __call__: Callable[..., SessionT]


def create_sqlite_url(file: PathType, *, is_async: bool = False, **kwargs: Any) -> URL:
    url = URL.create(drivername="sqlite+pysqlite")
    if is_async:
        url = url.set(drivername="sqlite+aiosqlite")

    url = url.set(**kwargs).set(database=str(file))
    return ensure_sqlite_url(url, is_async=is_async)


def ensure_sqlite_url(url: str | URL, *, is_async: bool = False) -> URL:
    url = make_url(url)
    dialect = url.get_dialect()
    if dialect.name != sqlite_dialect.name:
        raise NotImplementedError

    if _is_async_dialect(dialect) is not is_async:
        driver = url.get_driver_name()
        if not driver and is_async:
            url = url.set(drivername=f"{dialect.name}+aiosqlite")
        else:
            raise NotImplementedError

    return url


@overload
def ensure_sqlite_engine(connectable_or_url: URL | str) -> Engine | AsyncEngine: ...


@overload
def ensure_sqlite_engine(
    connectable_or_url: Engine
    | Connection
    | SessionMaker[Session]
    | Session
    | URL
    | str,
) -> Engine: ...


@overload
def ensure_sqlite_engine(
    connectable_or_url: AsyncEngine
    | AsyncConnection
    | SessionMaker[AsyncSession]
    | AsyncSession
    | URL
    | str,
) -> AsyncEngine: ...


@overload
def ensure_sqlite_engine(
    connectable_or_url: Engine
    | AsyncEngine
    | AsyncConnection
    | SessionMaker[AsyncSession]
    | AsyncSession
    | Connection
    | SessionMaker[Session]
    | Session
    | URL
    | str,
) -> Engine | AsyncEngine: ...


def ensure_sqlite_engine(
    connectable_or_url: Engine
    | AsyncEngine
    | AsyncConnection
    | SessionMaker[AsyncSession]
    | AsyncSession
    | Connection
    | SessionMaker[Session]
    | Session
    | URL
    | str,
) -> Engine | AsyncEngine:
    if isinstance(connectable_or_url, (AsyncEngine, AsyncConnection, AsyncSession)):
        return ensure_sqlite_async_engine(connectable_or_url)
    if isinstance(connectable_or_url, (Engine, Connection, Session)):
        return ensure_sqlite_sync_engine(connectable_or_url)
    if isinstance(connectable_or_url, str):
        connectable_or_url = ensure_sqlite_url(connectable_or_url)
    if isinstance(connectable_or_url, URL):
        dialect = connectable_or_url.get_dialect()
        if _is_async_dialect(dialect):
            return ensure_sqlite_async_engine(connectable_or_url)
        return ensure_sqlite_sync_engine(connectable_or_url)
    if isinstance(connectable_or_url, SessionMaker):
        connectable_or_url = connectable_or_url()
        return ensure_sqlite_engine(connectable_or_url)
    raise NotImplementedError


def ensure_sqlite_sync_engine(
    connectable_or_url: Engine
    | Connection
    | SessionMaker[Session]
    | Session
    | URL
    | str,
) -> Engine:
    if isinstance(connectable_or_url, (Engine, Connection)):
        return _set_listeners(connectable_or_url.engine)
    if isinstance(connectable_or_url, (str, URL)):
        connectable_or_url = ensure_sqlite_url(connectable_or_url, is_async=False)
    if isinstance(connectable_or_url, URL):
        return _set_listeners(create_engine(connectable_or_url))
    if isinstance(connectable_or_url, SessionMaker):
        connectable_or_url = connectable_or_url()
    if isinstance(connectable_or_url, Session):
        bind = connectable_or_url.get_bind()
        return ensure_sqlite_sync_engine(bind)
    raise NotImplementedError


def ensure_sqlite_async_engine(
    connectable_or_url: AsyncEngine
    | AsyncConnection
    | SessionMaker[AsyncSession]
    | AsyncSession
    | URL
    | str,
) -> AsyncEngine:
    if isinstance(connectable_or_url, (AsyncEngine, AsyncConnection)):
        return _set_listeners(connectable_or_url.engine)
    if isinstance(connectable_or_url, (str, URL)):
        connectable_or_url = ensure_sqlite_url(connectable_or_url, is_async=True)
    if isinstance(connectable_or_url, URL):
        return _set_listeners(create_async_engine(connectable_or_url))
    if isinstance(connectable_or_url, SessionMaker):
        connectable_or_url = connectable_or_url()
    if isinstance(connectable_or_url, AsyncSession):
        bind = connectable_or_url.get_bind()
        return ensure_sqlite_async_engine(bind)
    raise NotImplementedError


@contextmanager
def enter_sync_database(
    connectable: Engine | Connection | SessionMaker[Session] | Session,
) -> Generator[Session, None, None]:
    if isinstance(connectable, Session):
        session = connectable
    elif isinstance(connectable, (Engine, Connection)):
        session = Session(connectable)
    elif isinstance(connectable, SessionMaker):
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
    | SessionMaker[AsyncSession]
    | AsyncSession,
) -> AsyncGenerator[AsyncSession, None]:
    if isinstance(connectable, AsyncSession):
        session = connectable
    elif isinstance(connectable, (AsyncEngine, AsyncConnection)):
        session = AsyncSession(connectable)
    elif isinstance(connectable, SessionMaker):
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


def _is_async_dialect(dialect: type[Dialect] | Dialect) -> bool:
    return getattr(dialect, "is_async", False) is True


def _sqlite_isolation_read(
    dbapi_connection: _DBAPIConnection,
    connection_record: Any,  # noqa: ARG001
) -> None:
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA read_uncommitted = true;", ())


def _sqlite_isolation_non_read(conn: Connection) -> None:
    stmt = sa.text("PRAGMA read_uncommitted = false;")
    conn.execute(stmt)


@overload
def _set_listeners(engine: Engine) -> Engine: ...
@overload
def _set_listeners(engine: AsyncEngine) -> AsyncEngine: ...
@overload
def _set_listeners(engine: Engine | AsyncEngine) -> Engine | AsyncEngine: ...
def _set_listeners(engine: Engine | AsyncEngine) -> Engine | AsyncEngine:
    sync_engine = engine.sync_engine if isinstance(engine, AsyncEngine) else engine
    listen(sync_engine, "connect", _sqlite_isolation_read)
    listen(sync_engine, "begin", _sqlite_isolation_non_read)
    return engine
