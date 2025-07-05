from __future__ import annotations

__version__: str


def __getattr__(name: str):  # noqa: ANN202
    if name == "__version__":
        from importlib.metadata import version  # noqa: PLC0415

        _version = version("airflow-fernet-secrets")
        globals()["__version__"] = _version
        return _version

    raise AttributeError(name)
