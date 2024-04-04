from __future__ import annotations

from typing import TYPE_CHECKING

from airflow_fernet_secrets.core.log.common import get_logger

if TYPE_CHECKING:
    from logging import Logger

__all__ = ["LoggingMixin"]


class LoggingMixin:
    def __init__(self) -> None:
        self._logger = get_logger()

    @property
    def logger_name(self) -> str:
        return self._logger.name

    @property
    def logger(self) -> Logger:
        return self._logger

    @property
    def log(self) -> Logger:
        return self._logger
