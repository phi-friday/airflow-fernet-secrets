from __future__ import annotations

from airflow_fernet_secrets.log.common import CommonLoggingMixin

__all__ = ["LoggingMixin"]


class LoggingMixin(CommonLoggingMixin): ...  # noqa: D101
