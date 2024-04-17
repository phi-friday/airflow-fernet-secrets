from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override

from airflow_fernet_secrets.log.common import CommonLoggingMixin

if TYPE_CHECKING:
    import logging


__all__ = ["LoggingMixin"]


class LoggingMixin(CommonLoggingMixin):
    """using airflow logging mixin"""

    def __init__(self) -> None:
        from airflow.utils.log.logging_mixin import LoggingMixin as AirflowLoggingMixin

        super().__init__()
        self._mixin = AirflowLoggingMixin()

    @property
    @override
    def logger_name(self) -> str:
        return self._mixin.log.name

    @property
    @override
    def logger(self) -> logging.Logger:
        return self._mixin.log

    @property
    @override
    def log(self) -> logging.Logger:
        return self._mixin.log
