from pyrogram.client import Client
from pyrogram import filters
from pyrogram.types import Message
from pyrogram.raw.base.input_peer import InputPeer
from pytgcalls import PyTgCalls, filters as calls_filters, idle
from pytgcalls.types import Direction, StreamFrames, UpdatedGroupCallParticipant, AudioQuality

import re
import typing

from src import constants, utils
from src.recorder import RecorderPy
from src.config import config


COMMANDS_PREFIXES = "!"
RECORD_COMMAND = "record"
RECORD_COMMAND_PATTERN = re.compile(r"^(?:\s+(?P<listen_chat_id>-|@?[a-zA-Z0-9_]{4,})(?:\s+(?P<join_as_id>@?[a-zA-Z0-9_]{4,}))?)?(?:\s*\n\s*(?P<to_listen_user_ids>(?:@?[a-zA-Z0-9_]{4,}\s*)*))?$")


logger = utils.get_logger(
    name = "tests",
    filepath = constants.LOG_FILEPATH,
    console_log_level = config.console_log_level,
    file_log_level = config.file_log_level
)


app = Client(
    name = config.session.name,
    api_id = config.session.api_id,
    api_hash = config.session.api_hash,
    phone_number = config.session.phone_number,
    workdir = constants.WORK_DIRPATH.resolve().as_posix()
)

call_py = PyTgCalls(app)

recorder_py = RecorderPy(
    logger = logger,
    app = app,
    call_py = call_py,
    quality = AudioQuality.HIGH,
    write_log_debug_progress = True
)


send_to_chat_peer: InputPeer | None = None


async def _chat_id_filter(_: typing.Any, __: typing.Any, message: Message) -> bool:
    if isinstance(config.control_chat_id, int):
        return message.chat.id == config.control_chat_id

    return message.chat.username.lower() == config.control_chat_id

chat_id_filter = filters.create(_chat_id_filter)


def _extract_id_from_peer(peer: InputPeer) -> int | None:
    return getattr(peer, "chat_id", None) or getattr(peer, "channel_id", None) or getattr(peer, "user_id", None)

@typing.overload
async def _resolve_chat_id(value: int | str, as_peer: typing.Literal[False]=...) -> int: ...

@typing.overload
async def _resolve_chat_id(value: int | str, as_peer: typing.Literal[True]) -> InputPeer: ...

async def _resolve_chat_id(value: int | str, as_peer: bool=False) -> int | InputPeer:
    if isinstance(value, str) and utils.is_int(value):
        value = int(value)

    try:
        chat_peer: InputPeer = await app.resolve_peer(value)  # type: ignore

    except Exception:
        raise ValueError("A valid peer must be specified")

    if as_peer:
        return chat_peer

    chat_id = _extract_id_from_peer(chat_peer)

    if not chat_id:
        raise ValueError("A valid ID must be specified")

    return chat_id

def _fix_chat_id(chat_id: int) -> int:
    if chat_id > 0:
        return -1_000_000_000_000 - chat_id

    return chat_id


@app.on_message(chat_id_filter & filters.command(RECORD_COMMAND, COMMANDS_PREFIXES))
async def record_handler(_, message: Message):
    """
    Start recording voice chat.

    The regex starts with `^!record` and optionally captures
    `listen_chat_id` (which can be `-` or a username-like string of
    at least 4 characters), followed by an optional `join_as_id`
    (also at least 4 characters). If a newline is present, it
    captures a space-separated list of `to_listen_user_ids`, ensuring
    each entry is at least 4 characters long, and allows
    optional leading/trailing spaces.

    Examples:

    `!record <listen_chat_id>\n<listen_user_id_1> <listen_user_id_2>`

    `!record <listen_chat_id> <join_as_id>\n<listen_user_id_1> <listen_user_id_2>`

    `!record - <join_as_id>`

    `!record`
    """
    global send_to_chat_peer

    command_match = RECORD_COMMAND_PATTERN.match(typing.cast(str, message.text)[1 + len(RECORD_COMMAND):])  # skip prefix and command

    if not command_match:
        await message.reply_text("Invalid command format")

        return

    command_match_data: dict[str, str] = command_match.groupdict()

    listen_chat_id_str = command_match_data.get("listen_chat_id")
    join_as_id_str = command_match_data.get("join_as_id")
    to_listen_user_ids_str = command_match_data.get("to_listen_user_ids")

    processing_message = await message.reply_text("Processing...")

    if not listen_chat_id_str or listen_chat_id_str == "-":
        listen_chat_id = config.default_listen_chat_id or message.chat.id

        if isinstance(listen_chat_id, int):
            listen_chat_id = _fix_chat_id(listen_chat_id)

        else:
            try:
                listen_chat_id = _fix_chat_id(await _resolve_chat_id(listen_chat_id))

            except ValueError as ex:
                await processing_message.delete()
                await message.reply_text(f"Listen chat ID (config) = {listen_chat_id!r}" + "\n" + ex.args[0])

        listen_chat_id = typing.cast(int, listen_chat_id)

    else:
        try:
            listen_chat_id = _fix_chat_id(await _resolve_chat_id(listen_chat_id_str))

        except ValueError as ex:
            await processing_message.delete()
            await message.reply_text(f"Chat ID = {listen_chat_id_str!r}" + "\n" + ex.args[0])

            return

    join_as_peer: InputPeer | None = None

    if join_as_id_str:
        try:
            join_as_peer = await _resolve_chat_id(join_as_id_str, as_peer=True)

        except ValueError as ex:
            await processing_message.delete()
            await message.reply_text(f"Join as ID = {join_as_id_str!r}" + "\n" + ex.args[0])

            return

    to_listen_user_ids: list[int] = []

    if to_listen_user_ids_str:
        to_listen_user_ids_str_list = to_listen_user_ids_str.split()

        for listen_user_id_str in to_listen_user_ids_str_list:
            try:
                listen_user_id = await _resolve_chat_id(listen_user_id_str)

            except ValueError as ex:
                await processing_message.delete()
                await message.reply_text(f"Listen User ID = {listen_user_id_str!r}" + "\n" + ex.args[0])

                return

            to_listen_user_ids.append(listen_user_id)

    if recorder_py.is_running:
        if recorder_py.listen_chat_id != listen_chat_id:
            await processing_message.delete()
            await message.reply_text(f"Already recording in chat {listen_chat_id}")

            return

        await recorder_py.stop()

    if not send_to_chat_peer:
        send_to_chat_id = config.send_to_chat_id or message.chat.id

        try:
            send_to_chat_peer = await _resolve_chat_id(send_to_chat_id, as_peer=True)

        except ValueError as ex:
            await processing_message.delete()
            await message.reply_text(f"Send to chat ID (config) = {send_to_chat_id!r}" + "\n" + ex.args[0])

            return

    await recorder_py.start(
        listen_chat_id = listen_chat_id,
        send_to_chat_peer = send_to_chat_peer,
        join_as_peer = join_as_peer,
        to_listen_user_ids = to_listen_user_ids
    )

    await processing_message.delete()

    await message.reply_text((
        f"Started listening voice chat of <code>{listen_chat_id}</code>\n"
        f"""Joined as: {f"<code>{_fix_chat_id(typing.cast(int, _extract_id_from_peer(join_as_peer)))}</code>" if join_as_peer else "<b>self</b>"}\n"""
        f"""Listen user IDs: {f"<code>{', '.join(map(str, to_listen_user_ids))}</code>" if to_listen_user_ids else "<b>all</b>"}"""
    ))


@app.on_message(chat_id_filter & filters.command(["stop", "leave"], COMMANDS_PREFIXES))
async def leave_handler(_, message: Message):
    stopping_message = await message.reply_text("Stopping recording...")

    await recorder_py.stop()

    await stopping_message.delete()

    await message.reply_text("Recording stopped")


@call_py.on_update(calls_filters.stream_frame(
    directions = Direction.INCOMING
))
async def stream_audio_frame_handler(_, update: StreamFrames):
    for frame in update.frames:
        await recorder_py.process_pcm_frame(frame)


@call_py.on_update(calls_filters.call_participant())
async def joined_handler(_, update: UpdatedGroupCallParticipant):
    await recorder_py.process_participant_update(update)


call_py.start()  # type: ignore
idle()
