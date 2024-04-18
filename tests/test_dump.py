from __future__ import annotations

from itertools import product
from typing import Any
from uuid import uuid4

import pytest
from airflow.models.connection import Connection
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.url import URL, make_url

from airflow_fernet_secrets.connection import (
    connection_to_args,
    convert_args_to_jsonable,
)


def _rand() -> str:
    rand = uuid4().hex
    return f"a{rand}Z"


@pytest.mark.parametrize(
    "data",
    (
        {
            "conn_type": conn_type,
            "host": _rand() if param[0] else None,
            "login": _rand() if param[1] else None,
            "password": _rand() if param[2] else None,
            "schema": _rand() if param[3] else None,
            "port": 12345 if param[4] else None,
            "extra": {_rand(): _rand()} if param[5] else None,
        }
        # conn_type, host, login, password, schema, param, port, extra
        for conn_type, param in product(
            ("sqlite", "postgresql", "odbc", "mssql"),
            product(
                (True, False),
                (True, False),
                (True, False),
                (True, False),
                (True, False),
                (True, False),
            ),
        )
    ),
)
def test_dumps(data: dict[str, Any]):
    if data["conn_type"] == "mssql" and not data["host"]:
        pytest.skip("pymssql needs host")

    connection = Connection(**data)
    args = connection_to_args(connection)

    assert "url" in args
    url = args["url"]
    assert isinstance(url, (str, URL))
    make_url(url)

    assert "connect_args" in args
    assert isinstance(args["connect_args"], dict)

    assert "engine_kwargs" in args
    assert isinstance(args["engine_kwargs"], dict)

    convert_args_to_jsonable(args)
    # TODO: engine_kwargs
    create_engine(args["url"], connect_args=args["connect_args"])
