from __future__ import annotations

import inspect
import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

import sqlalchemy as sa
from cryptography.fernet import Fernet
from sqlalchemy.orm import declared_attr, registry
from typing_extensions import TypeGuard, override

from airflow_fernet_secrets.common.utils.re import camel_to_snake

if TYPE_CHECKING:
    from airflow.models.connection import Connection as AirflowConnection
    from airflow.models.variable import Variable as AirflowVariable

metadata = sa.MetaData()
mapper_registry = registry(metadata=metadata)


def _get_class(value: Any) -> type[Any]:
    return value if inspect.isclass(value) else type(value)


@dataclass
class Base:
    __sa_dataclass_metadata_key__: ClassVar[str] = "sa"
    __table__: ClassVar[sa.Table]
    __tablename__: ClassVar[str]

    if not TYPE_CHECKING:

        @declared_attr
        def __tablename__(self) -> str:
            cls = _get_class(self)
            return camel_to_snake(cls.__name__)

    id: int = field(
        init=False,
        metadata={"sa": sa.Column(sa.Integer(), primary_key=True, autoincrement=True)},
    )


@dataclass
class Encrypted(Base):
    __abstract__: ClassVar[bool] = True
    encrypted: bytes = field(metadata={"sa": sa.Column(sa.LargeBinary())})

    def decrypt(self, secret_key: str | bytes | Fernet) -> bytes:
        secret_key = _ensure_fernet(secret_key)
        return secret_key.decrypt(self.encrypted)

    @staticmethod
    def encrypt(value: Any, secret_key: str | bytes | Fernet) -> bytes:
        secret_key = _ensure_fernet(secret_key)
        as_bytes = _dump(value)
        return secret_key.encrypt(as_bytes)


@mapper_registry.mapped
@dataclass
class Connection(Encrypted):
    conn_id: str = field(metadata={"sa": sa.Column(sa.String(2**8))})


@mapper_registry.mapped
@dataclass
class Variable(Encrypted):
    variable_id: str = field(metadata={"sa": sa.Column(sa.String(2**8))})

    @override
    def decrypt(self, secret_key: str | bytes | Fernet) -> str:
        as_bytes = super().decrypt(secret_key)
        return as_bytes.decode("utf-8")


def _ensure_fernet(secret_key: str | bytes | Fernet) -> Fernet:
    if isinstance(secret_key, Fernet):
        return secret_key
    return Fernet(secret_key)


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
    return value.val


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

    raise NotImplementedError
