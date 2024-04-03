from __future__ import annotations

import json
from typing import Any

from typing_extensions import override

from airflow_fernet_secrets.backend.common import (
    CommonFernetLocalSecretsBackend as _CommonFernetLocalSecretsBackend,
)

__all__ = ["FernetLocalSecretsBackend"]


class FernetLocalSecretsBackend(_CommonFernetLocalSecretsBackend):
    @override
    def deserialize_connection(self, conn_id: str, value: str) -> dict[str, Any]:
        data = super().deserialize_connection(conn_id=conn_id, value=value)
        as_dict = json.loads(data)
        as_dict["conn_id"] = conn_id
        return as_dict

    @override
    def serialize_connection(self, conn_id: str, connection: dict[str, Any]) -> bytes:
        as_str = json.dumps(connection)
        return as_str.encode("utf-8")
