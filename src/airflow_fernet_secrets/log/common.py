from __future__ import annotations

import logging

from airflow_fernet_secrets.config.const import LOGGER_NAME

__all__ = ["CommonLoggingMixin", "get_logger"]


class CommonLoggingMixin:
    def __init__(self) -> None:
        self._logger = get_logger()

    @property
    def logger_name(self) -> str:
        return self._logger.name

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @property
    def log(self) -> logging.Logger:
        return self._logger


def get_logger() -> logging.Logger:
    return logging.getLogger(LOGGER_NAME)
