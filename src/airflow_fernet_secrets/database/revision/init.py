from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import schema

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

metadata = sa.MetaData()
version = sa.Table(
    "version",
    metadata,
    sa.Column("id", sa.Integer(), nullable=False, autoincrement=True),
    sa.Column("revision", sa.String(100), nullable=False),
    sa.PrimaryKeyConstraint("id", name="pk_version"),
)
connection = sa.Table(
    "connection",
    metadata,
    sa.Column("id", sa.Integer(), nullable=False, autoincrement=True),
    sa.Column("encrypted", sa.LargeBinary(), nullable=False),
    sa.Column("conn_id", sa.String(2**8), nullable=False),
    sa.Column("conn_type", sa.String(2**8), nullable=False),
    sa.PrimaryKeyConstraint("id", name="pk_connection"),
    sa.Index("index_connection_conn_id", "conn_id"),
    sa.UniqueConstraint("conn_id", name="unique_connection_conn_id"),
)
variable = sa.Table(
    "variable",
    metadata,
    sa.Column("id", sa.Integer(), nullable=False, autoincrement=True),
    sa.Column("encrypted", sa.LargeBinary(), nullable=False),
    sa.Column("key", sa.String(2**8), nullable=False),
    sa.PrimaryKeyConstraint("id", name="pk_variable"),
    sa.Index("index_variable_key", "key"),
    sa.UniqueConstraint("key", name="unique_variable_key"),
)


def upgrade(conn: Connection) -> None:
    """upgrade null -> init"""
    if conn.in_transaction() or conn.in_nested_transaction():
        func = conn.begin_nested
    else:
        func = conn.begin

    with func() as transact:
        metadata.create_all(
            conn, tables=[version, connection, variable], checkfirst=False
        )
        conn.execute(sa.insert(version).values({"revision": "init"}))
        transact.commit()


def downgrade(conn: Connection) -> None:
    """downgrade init -> null"""
    drop_tables = [
        schema.DropTable(table, if_exists=False)
        for table in (version, connection, variable)
    ]

    if conn.in_transaction() or conn.in_nested_transaction():
        func = conn.begin_nested
    else:
        func = conn.begin

    with func() as transact:
        for stmt in drop_tables:
            conn.execute(stmt)
        transact.commit()
