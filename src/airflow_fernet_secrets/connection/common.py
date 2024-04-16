from __future__ import annotations

import json
from contextlib import suppress
from typing import TYPE_CHECKING, Any

from typing_extensions import Required, TypedDict

if TYPE_CHECKING:
    from airflow_fernet_secrets.connection.dump.main import ConnectionArgs

__all__ = ["ConnectionDict", "convert_args_to_jsonable"]


class ConnectionDict(TypedDict, total=False):
    conn_type: str
    host: str
    login: str
    password: str
    schema: str
    port: int
    extra: Required[dict[str, Any]]
    args: Required[ConnectionArgs | None]


def convert_args_to_jsonable(args: ConnectionArgs) -> ConnectionArgs:
    url, connect_args, engine_kwargs = (
        args["url"],
        args["connect_args"],
        args["engine_kwargs"],
    )
    if not isinstance(url, str):
        url = url.render_as_string(hide_password=False)

    sr_connect_args: dict[str, Any] = {}
    for key, value in connect_args.items():
        with suppress(Exception):
            json.dumps(value)
            sr_connect_args[key] = value

    sr_engine_kwargs: dict[str, Any] = {}
    for key, value in engine_kwargs.items():
        with suppress(Exception):
            json.dumps(value)
            sr_engine_kwargs[key] = value

    return {
        "url": url,
        "connect_args": sr_connect_args,
        "engine_kwargs": sr_engine_kwargs,
    }
