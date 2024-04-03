from __future__ import annotations

import json
from importlib.metadata import distribution
from importlib.util import find_spec
from pathlib import Path
from typing import Any

__all__ = ["validate_provider_info"]


def get_provider_info() -> dict[str, Any]:
    dist = distribution(__package__ or "airflow-fernet-secrets")
    meta = dist.metadata

    return {
        "package-name": __package__,
        "name": meta.get("name"),
        "description": meta.get("summary"),
        "version": dist.version,
        "secrets-backends": [
            "airflow.providers.fernet_secrets.secrets.secret_manager.FernetLocalSecretsBackend"
        ],
        "config": {
            "fernet_secrets": {
                "description": meta.get("summary"),
                "options": {
                    "secret_key": {
                        "default": None,
                        "description": "fernet secret key",
                        "type": "string",
                        "version_added": "2.8.0",
                        "example": "96qLkedvLv-7mvHsj5xPOPFbJYUxS3jBp9Q_eLeJOmQ=",
                    },
                    "secret_key_cmd": {
                        "default": None,
                        "description": "fernet secret key command",
                        "type": "string",
                        "version_added": "2.8.0",
                        "example": "cat /dev/run/fernet_key",
                    },
                    "variables_file": {
                        "default": None,
                        "description": "variables_file path",
                        "type": "string",
                        "version_added": "2.8.0",
                        "example": "/tmp/variables.sqlite3",  # noqa: S108
                    },
                    "variables_file_cmd": {
                        "default": None,
                        "description": "variables_file path command",
                        "type": "string",
                        "version_added": "2.8.0",
                        "example": "cat /dev/run/variables",
                    },
                    "connections_file": {
                        "default": None,
                        "description": "connections_file path",
                        "type": "string",
                        "version_added": "2.8.0",
                        "example": "/tmp/connections.sqlite3",  # noqa: S108
                    },
                    "connections_file_cmd": {
                        "default": None,
                        "description": "connections_file path command",
                        "type": "string",
                        "version_added": "2.8.0",
                        "example": "cat /dev/run/connections",
                    },
                },
            }
        },
    }


def validate_provider_info(info: dict[str, Any]) -> None:
    import jsonschema

    from airflow_fernet_secrets.common.log.common import get_logger

    logger = get_logger()

    spec = find_spec("airflow")
    if spec is None or spec.origin is None:
        logger.warning("install airflow first. skip validation.")
        return

    origin = Path(spec.origin)
    schema = origin.with_name("provider_info.schema.json")
    if not schema.exists() or not schema.is_file():
        logger.warning("there is no provider info schema file: %s", schema)
        return

    with schema.open("r") as file:
        json_data: dict[str, Any] = json.load(file)

    jsonschema.validate(info, json_data)
