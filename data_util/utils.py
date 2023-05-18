import logging

from dataclasses import dataclass, field


@dataclass
class DataTemplate:
    amount: int = 0
    columns: dict = field(default_factory=lambda: {})


@dataclass
class DataColumn:
    column_name: str
    column_category: str
    column_args: dict = None


def get_logger(log_level="info"):
    if log_level == "warning":
        log_level = logging.WARNING
    if log_level == "debug":
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logging.basicConfig(level=log_level, format="%(message)s")
    return logging.getLogger()
