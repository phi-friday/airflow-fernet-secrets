from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Generator, cast

from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
from sqlalchemy.engine import Connection, Engine, create_engine
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.orm import Session, scoped_session, sessionmaker

if TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import Dialect

    from airflow_fernet_secrets.core.typeshed import PathType

__all__ = [
    "create_sqlite_url",
    "ensure_sqlite_url",
    "ensure_sqlite_engine",
    "enter_database",
]


def create_sqlite_url(file: PathType, **kwargs: Any) -> URL:
    url = URL.create(drivername="sqlite+pysqlite").set(**kwargs).set(database=str(file))
    return ensure_sqlite_url(url)


def ensure_sqlite_url(url: str | URL) -> URL:
    url = cast("URL", make_url(url))
    dialect: type[Dialect] = url.get_dialect()
    if getattr(dialect, "name", "") != sqlite_dialect.name:
        raise NotImplementedError

    return url


def ensure_sqlite_engine(
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
        connectable_or_url = ensure_sqlite_url(connectable_or_url)
    if isinstance(connectable_or_url, URL):
        return cast("Engine", create_engine(connectable_or_url))
    if isinstance(connectable_or_url, Session):
        return connectable_or_url.connection().engine
    if isinstance(connectable_or_url, (sessionmaker, scoped_session)):
        session: Session = connectable_or_url()
        try:
            return session.connection().engine
        finally:
            session.close()
    raise NotImplementedError


@contextmanager
def enter_database(
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
