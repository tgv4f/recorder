from pyrogram import Client, filters
from pyrogram.types import Message
from pytgcalls import PyTgCalls, filters as fl, idle
from pytgcalls.types import Direction, StreamFrames, UpdatedGroupCallParticipant, AudioQuality

from .recorder import RecorderPy

from src import constants, utils, config


logger = utils.get_logger(
    name = "tests",
    filepath = constants.LOG_FILEPATH
)


app = Client(
    config.SESSION_NAME,
    api_id = config.API_ID,
    api_hash = config.API_HASH,
    phone_number = config.PHONE_NUMBER,
    workdir = constants.WORK_DIRPATH
)

call_py = PyTgCalls(app)

recorder_py = RecorderPy(
    logger = logger,
    app = app,
    call_py = call_py,
    quality = AudioQuality.HIGH
)


SEND_TO_CHAT_PEER = None

TO_LISTEN_USER_IDS = None


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
        await message.reply("A valid Chat ID must be specified")

        return

    else:
        chat_id = int(args[0])

    if recorder_py.is_running:
        if recorder_py.worker.listen_chat_id == chat_id:
            await message.reply(f"Already recording in chat {chat_id}")

            return

        await recorder_py.stop()

    if not SEND_TO_CHAT_PEER:
        SEND_TO_CHAT_PEER = await app.resolve_peer(config.SEND_TO_CHAT_ID)

    await recorder_py.start(chat_id, SEND_TO_CHAT_PEER)

    await message.reply(f"Switched to listening to voice chat in chat {chat_id}")


@app.on_message(chat_id_filter & filters.regex('!leave'))
async def leave_handler(_: Client, message: Message):
    await message.reply("Stopping recording...")

    await recorder_py.stop()


@call_py.on_update(fl.stream_frame(
    # devices = Device.SPEAKER
    directions = Direction.INCOMING
))
async def stream_audio_frame_handler(_, update: StreamFrames):
    for frame in update.frames:
        await recorder_py.process_pcm_frame(frame)


@call_py.on_update(fl.call_participant())
async def joined_handler(_, update: UpdatedGroupCallParticipant):
    await recorder_py.process_participant_update(update)


call_py.start()
idle()
