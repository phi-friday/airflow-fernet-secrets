from __future__ import annotations

import json
import re
import shutil
from importlib.metadata import distribution
from importlib.util import find_spec
from pathlib import Path
from typing import Any

import yaml

__all__ = ["get_provider_info"]

_RE_VARIABLE = re.compile(r"{{ ([a-zA-Z0-9_-]+) }}")


def get_provider_info() -> dict[str, Any]:
    root = Path(__file__).parent
    package = __package__ or root.name
    dist = distribution(package)
    path = root / "info.yaml"
    with path.open("r") as file:
        text = file.read()

    registry = {x: "" for x in _RE_VARIABLE.findall(text)}
    meta = dist.metadata

    registry["package"] = registry["name"] = meta.get("name") or package
    registry["version"] = dist.version
    registry["description"] = meta.get("summary") or ""

    for key, value in registry.items():
        old = f"{{{{ {key} }}}}"
        text = text.replace(old, value)

    info = yaml.safe_load(text)
    validate_provider_info(info)
    return info


def validate_provider_info(info: dict[str, Any]) -> None:
    import jsonschema

    schema = _get_provider_schema_path()
    if schema is None:
        return

    schema = copy_provider_info_schema(schema)
    if schema is None:
        return

    with schema.open("r") as file:
        json_data: dict[str, Any] = json.load(file)

    jsonschema.validate(info, json_data)


def copy_provider_info_schema(file: Path | None = None) -> Path | None:
    local_schema = Path(__file__).with_name("info.schema.json")
    if local_schema.exists() and not local_schema.is_file():
        raise FileExistsError(local_schema)

    if file is None:
        schema = _get_provider_schema_path()
        if schema is None:
            return None
    else:
        schema = file

    shutil.copyfile(schema, local_schema)

    return local_schema


def _get_provider_schema_path() -> Path | None:
    from airflow_fernet_secrets.core.log.common import get_logger

    logger = get_logger()

    spec = find_spec("airflow")
    if spec is None or spec.origin is None:
        logger.warning("install airflow first. skip validation.", stacklevel=2)
        return None

    origin = Path(spec.origin)
    schema = origin.with_name("provider_info.schema.json")
    if not schema.exists() or not schema.is_file():
        logger.warning(
            "there is no provider info schema file: %s", schema, stacklevel=2
        )
        return None

    return schema
