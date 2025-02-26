from pyrogram import Client, raw
from pyrogram.session import Session
from pytgcalls import PyTgCalls, exceptions as calls_excs
from pytgcalls.types import Direction, RecordStream, StreamFrames, GroupCallParticipant, UpdatedGroupCallParticipant, AudioQuality, Frame

from bidict import bidict
from io import BytesIO
from logging import Logger

import asyncio

# from .. import utils
# from .worker import RecorderWorker


class RecorderPy:
    # PEERS_CACHE: dict[int, raw.base.InputPeer] = {}
    SSRC_AND_TG_ID: bidict[int, int] = bidict()

    def __init__(
        self,
        logger: Logger,
        app: Client,
        call_py: PyTgCalls,
        quality: AudioQuality,
        max_duration: float = 15.,
        silence_threshold: float = 3.,
        upload_files_workers_count: int = 1
    ):
        from .worker import RecorderWorker

        self._logger = logger
        self._app = app
        self._call_py = call_py
        self._call_py_binding = call_py._binding
        self._quality = quality

        self.worker: RecorderWorker | None = None

        self._is_running = False

        self._sample_rate = quality.value[0]
        self._channels = quality.value[1]
        self._channel_second_rate = self._sample_rate * self._channels * 2
        self._max_duration = max_duration
        self._silence_threshold = silence_threshold
        self._upload_files_workers_count = upload_files_workers_count

    @property
    def is_running(self) -> bool:
        return self._is_running

    async def _process_participant_joined(self, participant: GroupCallParticipant) -> None:
        pass

    async def _process_participant_left_me(self) -> None:
        await self.stop()

    async def _process_participant_left(self, participant: GroupCallParticipant) -> None:
        if participant.user_id == self._app.me.id:
            await self._process_participant_left_me()

            return

    async def process_participant_update(self, update: UpdatedGroupCallParticipant) -> None:
        if not self._is_running or not self.worker or not self.worker.is_running:
            return

        participant = update.participant

        match participant.action:
            case GroupCallParticipant.Action.LEFT:
                await self._process_participant_left(participant)
            case GroupCallParticipant.Action.JOINED:
                await self._process_participant_joined(participant)

    async def process_pcm_frame(self, frame: Frame) -> None:
        if not self._is_running or not self.worker or not self.worker.is_running:
            return

        await self.worker.process_pcm_frame(frame)

    async def start(self, listen_chat_id: int, send_to_chat_peer: raw.base.InputPeer) -> None:
        if self._is_running:
            return

        self._logger.info("Starting recorder...")

        self._is_running = True

        from .worker import RecorderWorker

        self.worker = RecorderWorker(
            parent = self,
            listen_chat_id = listen_chat_id,
            send_to_chat_peer = send_to_chat_peer
        )

        # await binding.calls()   =>   {-1001183345400: <ntgcalls.MediaStatus object at 0x7fcb5d994570>}
        for chat_id in (await self._call_py_binding.calls()).keys():
            if chat_id != listen_chat_id:
                try:
                    await self._call_py.leave_call(chat_id)
                except Exception as ex:
                    self._logger.exception(f"Error while leaving call with chat ID {chat_id}", exc_info=ex)

        await self._call_py.record(
            chat_id = listen_chat_id,
            stream = RecordStream(
                audio = True,
                audio_parameters = self._quality,
                camera = False,
                screen = False
            )
        )

        await self.worker.start()

        self._logger.info("Recorder started")

    async def stop(self) -> None:
        if not self._is_running:
            return

        self._logger.info("Stopping recorder...")

        self._is_running = False

        if self.worker:
            try:
                await self._call_py.leave_call(self.worker.listen_chat_id)
            except calls_excs.NoActiveGroupCall:
                pass

            if self.worker.is_running:
                await self.worker.stop()

            self.worker = None

        self._logger.info("Recorder stopped")
