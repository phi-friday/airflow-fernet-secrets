from __future__ import annotations

import logging

from airflow_fernet_secrets.common.config.const import LOGGER_NAME

__all__ = ["get_logger"]


def get_logger() -> logging.Logger:
    return logging.getLogger(LOGGER_NAME)
