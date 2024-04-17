from __future__ import annotations

__all__ = [
    "FernetSecretsError",
    "FernetSecretsKeyError",
    "FernetSecretsValueError",
    "FernetSecretsTypeError",
    "FernetSecretsNotImplementedError",
]


class FernetSecretsError(Exception):
    """airflow-fernet-secrets base error"""


class FernetSecretsKeyError(KeyError, FernetSecretsError):
    """airflow-fernet-secrets key error"""


class FernetSecretsValueError(ValueError, FernetSecretsError):
    """airflow-fernet-secrets value error"""


class FernetSecretsTypeError(TypeError, FernetSecretsError):
    """airflow-fernet-secrets type error"""


class FernetSecretsNotImplementedError(NotImplementedError, FernetSecretsError):
    """airflow-fernet-secrets implement error"""
