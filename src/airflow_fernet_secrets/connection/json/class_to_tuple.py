from __future__ import annotations

import importlib
import pickle
from contextlib import suppress
from inspect import isclass
from typing import Any, Literal

from typing_extensions import TypedDict, TypeGuard

from airflow_fernet_secrets.const import JSONABLE_CLASS_TYPE_FLAG

__all__ = ["JsonableClassType", "dump_as_jsonable", "load_from_jsonable"]


class JsonableClassType(TypedDict, total=True):
    """class to jsonable dict"""

    _jsonable_class_type_flag_: Literal[True]  # JSONABLE_CLASS_TYPE_FLAG
    """just flag"""
    module: str
    """class module"""
    name: str
    """class name"""


def dump_as_jsonable(value: Any) -> tuple[bool, Any]:
    """class to jsonable dict"""
    if isclass(value):
        with suppress(Exception):
            pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
            module, name = value.__module__, value.__qualname__
            return True, {
                JSONABLE_CLASS_TYPE_FLAG: True,
                "module": module,
                "name": name,
            }

    return False, value


def load_from_jsonable(value: Any) -> tuple[bool, Any]:
    """jsonable dict to class"""
    if not is_jsonable_class_type(value):
        return False, value

    module, name = value["module"], value["name"]
    class_module = importlib.import_module(module)
    return True, getattr(class_module, name)


def is_jsonable_class_type(value: Any) -> TypeGuard[JsonableClassType]:
    """check is jsonable dict"""
    if not isinstance(value, dict):
        return False

    if set(map(str, value)) != {JSONABLE_CLASS_TYPE_FLAG, "module", "name"}:
        return False

    jsonable_class_type_flag, module, name = (
        value[JSONABLE_CLASS_TYPE_FLAG],
        value["module"],
        value["name"],
    )
    return bool(
        jsonable_class_type_flag is True
        and isinstance(module, str)
        and module
        and isinstance(name, str)
        and name
    )
