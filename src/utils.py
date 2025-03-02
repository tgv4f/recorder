from logging.handlers import RotatingFileHandler
from pathlib import Path
from time import time

import logging
import typing


LOGGING_CONSOLE_FORMAT: typing.Final = "%(message)s"
LOGGING_FILE_FORMAT: typing.Final = "%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"


T = typing.TypeVar("T")


class LoggerStrippingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        record.msg = (
            record.msg.strip()
            if isinstance(record.msg, str)
            else
            record.msg
        )

        return super().format(record)


def get_logger(name: str, filepath: Path, console_log_level: int=logging.INFO, file_log_level: int=logging.DEBUG) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(min(console_log_level, file_log_level))

    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_log_level)
    console_handler.setFormatter(logging.Formatter(LOGGING_CONSOLE_FORMAT))
    logger.addHandler(console_handler)

    file_handler = RotatingFileHandler(
        filename = filepath.resolve().as_posix(),
        mode = "a",
        maxBytes = 1028 * 1024,  # = 1 MB
        backupCount = 1_000,
        encoding = "utf-8"
    )

    file_handler.setLevel(file_log_level)
    file_handler.setFormatter(LoggerStrippingFormatter(LOGGING_FILE_FORMAT))
    logger.addHandler(file_handler)

    return logger


def is_int(value: str) -> bool:
    return value.rstrip("-").isdigit()


def get_timestamp_int() -> int:
    return int(time())


async def async_wrapper_logger(logger: logging.Logger, coro: typing.Awaitable[T]) -> T | None:
    try:
        return await coro

    except Exception as ex:
        logger.exception("Error in async function", exc_info=ex)

    return None
