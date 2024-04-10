from __future__ import annotations

from airflow_fernet_secrets.config import IS_SERVER_FLAG as _IS_SERVER_FLAG

if _IS_SERVER_FLAG:
    from airflow_fernet_secrets.log.server import LoggingMixin
else:
    from airflow_fernet_secrets.log.client import LoggingMixin

__all__ = ["LoggingMixin"]
