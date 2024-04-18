from __future__ import annotations

__version__: str


def __getattr__(name: str):  # noqa: ANN202
    if name == "__version__":
        from importlib.metadata import version

        return version("airflow-fernet-secrets")

    raise AttributeError(name)
