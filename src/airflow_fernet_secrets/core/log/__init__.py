from __future__ import annotations

from airflow_fernet_secrets.core.config import IS_SERVER_FLAG as _IS_SERVER_FLAG

if _IS_SERVER_FLAG:
    from airflow_fernet_secrets.core.log.server import LoggingMixin
else:
    from airflow_fernet_secrets.core.log.client import LoggingMixin

__all__ = ["LoggingMixin"]
