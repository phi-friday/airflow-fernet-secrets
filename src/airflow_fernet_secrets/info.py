from __future__ import annotations

import json
from importlib.metadata import distribution
from importlib.util import find_spec
from pathlib import Path
from typing import Any

from airflow_fernet_secrets.common.log import logger

__all__ = ["validate_provider_info"]


def get_provider_info() -> dict[str, Any]:
    dist = distribution(__package__)
    meta = dist.metadata

    return {
        "package-name": __package__,
        "name": meta.get("name"),
        "description": meta.get("summary"),
        "version": dist.version,
    }


def validate_provider_info(info: dict[str, Any]) -> None:
    import jsonschema

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
