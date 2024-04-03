from __future__ import annotations

import json
from typing import Any, Dict

from typing_extensions import override

from airflow_fernet_secrets.backend.common import (
    CommonFernetLocalSecretsBackend as _CommonFernetLocalSecretsBackend,
)

__all__ = ["FernetLocalSecretsBackend"]

ConnectionType = Dict[str, Any]


class FernetLocalSecretsBackend(_CommonFernetLocalSecretsBackend[ConnectionType]):
    @override
    def _deserialize_connection(self, conn_id: str, value: bytes) -> ConnectionType:
        as_dict = json.loads(value)
        as_dict["conn_id"] = conn_id
        return as_dict

    @override
    def serialize_connection(self, conn_id: str, connection: ConnectionType) -> bytes:
        as_str = json.dumps(connection)
        return as_str.encode("utf-8")
