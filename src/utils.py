from pyrogram.client import Client
from pyrogram.raw.base.input_peer import InputPeer
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


def extract_id_from_peer(peer: InputPeer) -> int | None:
    return getattr(peer, "chat_id", None) or getattr(peer, "channel_id", None) or getattr(peer, "user_id", None)


@typing.overload
async def resolve_chat_id(
    app: Client,
    value: int | str,
    as_peer: typing.Literal[False] = ...
) -> int: ...

@typing.overload
async def resolve_chat_id(
    app: Client,
    value: int | str,
    as_peer: typing.Literal[True]
) -> InputPeer: ...

async def resolve_chat_id(
    app: Client,
    value: int | str,
    as_peer: bool = False
) -> int | InputPeer:
    if isinstance(value, str) and is_int(value):
        value = int(value)

    try:
        chat_peer: InputPeer = await app.resolve_peer(value)  # type: ignore

    except Exception:
        raise ValueError("A valid peer must be specified")

    if as_peer:
        return chat_peer

    chat_id = extract_id_from_peer(chat_peer)

    if not chat_id:
        raise ValueError("A valid ID must be specified")

    return chat_id


def fix_chat_id(chat_id: int) -> int:
    if chat_id > 0:
        return -1_000_000_000_000 - chat_id

    return chat_id
