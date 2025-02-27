from pyrogram.client import Client
from pyrogram import filters
from pyrogram.types import Message
from pyrogram.raw.base.input_peer import InputPeer
from pytgcalls import PyTgCalls, filters as calls_filters, idle
from pytgcalls.types import Direction, StreamFrames, UpdatedGroupCallParticipant, AudioQuality

import typing

from .recorder import RecorderPy

from src import constants, utils, config


logger = utils.get_logger(
    name = "tests",
    filepath = constants.LOG_FILEPATH,
    console_log_level = config.CONSOLE_LOG_LEVEL,
    file_log_level = config.FILE_LOG_LEVEL
)


app = Client(
    config.SESSION_NAME,
    api_id = config.API_ID,
    api_hash = config.API_HASH,
    phone_number = config.PHONE_NUMBER,
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


SEND_TO_CHAT_PEER: InputPeer | None = None
TO_LISTEN_USER_IDS: typing.Collection[int] | None = None


PARTICIPANTS_SSRC_TO_TG_ID: dict[int, int] = {}
RECORDER_MODELS: dict[int, RecorderPy] = {}


async def _chat_id_filter(_, __, message: Message) -> bool:
    return message.chat.id == config.SEND_TO_CHAT_ID

chat_id_filter = filters.create(_chat_id_filter)


@app.on_message(chat_id_filter & filters.regex('!record'))
async def play_handler(_: Client, message: Message):
    global SEND_TO_CHAT_PEER

    args = message.text.split(' ')[1:]

    if len(args) == 0:
        chat_id = config.DEFAULT_LISTEN_CHAT_ID

    elif len(args) != 1 or not utils.is_int(args[0]):
        await message.reply_text("A valid Chat ID must be specified")

        return

    else:
        chat_id = int(args[0])

    if recorder_py.is_running:
        if recorder_py.worker and recorder_py.worker.listen_chat_id == chat_id:
            await message.reply_text(f"Already recording in chat {chat_id}")

            return

        await recorder_py.stop()

    if not SEND_TO_CHAT_PEER:
        SEND_TO_CHAT_PEER = await app.resolve_peer(config.SEND_TO_CHAT_ID)  # type: ignore

    await recorder_py.start(
        listen_chat_id = chat_id,
        send_to_chat_peer = typing.cast(InputPeer, SEND_TO_CHAT_PEER),
        to_listen_user_ids = TO_LISTEN_USER_IDS
    )

    await message.reply_text(f"Switched to listening to voice chat in chat {chat_id}")


@app.on_message(chat_id_filter & filters.regex('!leave'))
async def leave_handler(_: Client, message: Message):
    await message.reply_text("Stopping recording...")

    await recorder_py.stop()


@call_py.on_update(calls_filters.stream_frame(
    # devices = Device.SPEAKER
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
