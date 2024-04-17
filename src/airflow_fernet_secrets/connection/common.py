from __future__ import annotations

import json
from contextlib import suppress
from typing import TYPE_CHECKING, Any

from sqlalchemy.engine.url import make_url
from typing_extensions import Required, TypedDict

from airflow_fernet_secrets.connection.json.class_to_tuple import (
    dump_as_jsonable,
    load_from_jsonable,
)

if TYPE_CHECKING:
    from airflow_fernet_secrets.connection import ConnectionArgs

__all__ = ["ConnectionDict", "convert_args_to_jsonable", "convert_args_from_jsonable"]


class ConnectionDict(TypedDict, total=False):
    """serialized connection dict"""

    conn_type: str
    """conn type(== airflow `Connection.conn_type`)"""
    host: str
    """`Connection.host`"""
    login: str
    """`Connection.login`"""
    password: str
    """`Connection.password`"""
    schema: str
    """`Connection.schema`"""
    port: int
    """`Connection.port`"""
    extra: Required[dict[str, Any]]
    """`Connection.extra`"""
    args: Required[ConnectionArgs | None]
    """`ConnectionArgs`"""


def convert_args_to_jsonable(args: ConnectionArgs) -> ConnectionArgs:
    """filter is jsonable"""
    url, connect_args, engine_kwargs = (
        args["url"],
        args["connect_args"],
        args["engine_kwargs"],
    )
    if not isinstance(url, str):
        url = url.render_as_string(hide_password=False)

    sr_connect_args: dict[str, Any] = {}
    sr_engine_kwargs: dict[str, Any] = {}
    for sr_dict_value, dict_value in zip(
        (sr_connect_args, sr_engine_kwargs), (connect_args, engine_kwargs)
    ):
        for key, value in dict_value.items():
            with suppress(Exception):
                json.dumps(value)
                sr_dict_value[key] = value
                continue
            flag, new_value = dump_as_jsonable(value)
            if flag:
                sr_dict_value[key] = new_value

    return {
        "url": url,
        "connect_args": sr_connect_args,
        "engine_kwargs": sr_engine_kwargs,
    }


def convert_args_from_jsonable(args: ConnectionArgs) -> ConnectionArgs:
    """filter is jsonable"""
    url, sr_connect_args, sr_engine_kwargs = (
        args["url"],
        args["connect_args"],
        args["engine_kwargs"],
    )
    url = make_url(url)

    connect_args: dict[str, Any] = {}
    engine_kwargs: dict[str, Any] = {}
    for sr_dict_value, dict_value in zip(
        (sr_connect_args, sr_engine_kwargs), (connect_args, engine_kwargs)
    ):
        for key, value in sr_dict_value.items():
            _, new_value = load_from_jsonable(value)
            dict_value[key] = new_value

    return {"url": url, "connect_args": connect_args, "engine_kwargs": engine_kwargs}
