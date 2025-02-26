from logging.handlers import RotatingFileHandler
from pathlib import Path

import logging


class StrippingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        record.msg = record.msg.strip() if isinstance(record.msg, str) else record.msg

        return super().format(record)


def get_logger(name: str, filepath: Path) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(console_handler)

    file_handler = RotatingFileHandler(
        filename = filepath.resolve().as_posix(),
        mode = "a",
        # maxBytes = 1028 * 1024,  # 1 MB
        # backupCount = 1_000,
        encoding = "utf-8"
    )

    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(StrippingFormatter("%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"))
    logger.addHandler(file_handler)

    return logger


def is_int(value: str) -> bool:
    return value.rstrip("-").isdigit()


async def async_wrapper_logger(logger: logging.Logger, coro) -> None:
    try:
        await coro

    except Exception as ex:
        logger.exception("Error in async function", exc_info=ex)
