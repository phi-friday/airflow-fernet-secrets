from __future__ import annotations

import logging

from airflow_fernet_secrets.const import LOGGER_NAME

__all__ = ["CommonLoggingMixin", "get_logger"]


class CommonLoggingMixin:
    """has properties `logger`, `log`, and `logger_name`"""

    def __init__(self) -> None:
        self._logger = get_logger()

    @property
    def logger_name(self) -> str:
        """logger name"""
        return self._logger.name

    @property
    def logger(self) -> logging.Logger:
        """logger instance"""
        return self._logger

    @property
    def log(self) -> logging.Logger:
        """logger instance"""
        return self._logger


def get_logger() -> logging.Logger:
    """get airflow-fernet-secrets logger"""
    return logging.getLogger(LOGGER_NAME)
