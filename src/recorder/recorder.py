from pyrogram.client import Client
from pyrogram.raw.base.input_peer import InputPeer
from pytgcalls import PyTgCalls, exceptions as calls_exceptions
from pytgcalls.types import RecordStream, GroupCallParticipant, UpdatedGroupCallParticipant, AudioQuality, Frame, GroupCallConfig

from logging import Logger

import typing


class RecorderPy:
    def __init__(
        self,
        logger: Logger,
        app: Client,
        call_py: PyTgCalls,
        quality: AudioQuality,
        max_duration: float = 15.,
        silence_duration: float = 3.,
        silence_threshold: float = 0.01,
        latest_frame_detector_duration: float = 5.,
        upload_files_workers_count: int = 1,
        write_log_debug_progress: bool = False
    ):
        self._logger = logger
        self._app = app
        self._call_py = call_py
        self._call_py_binding = call_py._binding
        self._quality = quality

        from .worker import RecorderWorker

        self._is_running = False
        self.worker: RecorderWorker | None = None

        self._app_user_id: int | None = None
        self._sample_rate = quality.value[0]
        self._channels = quality.value[1]
        self._channel_second_rate = self._sample_rate * self._channels * 2
        self._max_duration = max_duration
        self._pcm_max_duration_in_size = int(self._channel_second_rate * max_duration)
        self._pcm_silence_duration_in_size = int(self._channel_second_rate * silence_duration)
        self._silence_threshold = silence_threshold
        self._latest_frame_detector_duration = latest_frame_detector_duration
        self._upload_files_workers_count = upload_files_workers_count
        self._write_log_debug_progress = write_log_debug_progress

    @property
    def is_running(self) -> bool:
        return self._is_running

    async def _process_participant_joined(self, participant: GroupCallParticipant) -> None:
        if not self.worker or not self.worker.is_running or (self.worker.to_listen_user_ids and participant.user_id not in self.worker.to_listen_user_ids):
            return

        self.worker.ssrc_and_tg_id.inverse[participant.user_id] = participant.source

    async def _process_participant_left_me(self) -> None:
        await self.stop()

    async def _process_participant_left(self, participant: GroupCallParticipant) -> None:
        if participant.user_id == self._app_user_id:
            await self._process_participant_left_me()

            return

    async def process_participant_update(self, update: UpdatedGroupCallParticipant) -> None:
        if not self._is_running or not self.worker or not self.worker.is_running:
            return

        participant = update.participant

        if participant.action == GroupCallParticipant.Action.JOINED:
            await self._process_participant_joined(participant)
        elif participant.action == GroupCallParticipant.Action.LEFT:
            await self._process_participant_left(participant)

    async def process_pcm_frame(self, frame: Frame) -> None:
        if not self._is_running or not self.worker or not self.worker.is_running:
            return

        await self.worker.process_pcm_frame(frame)

    async def start(
        self,
        listen_chat_id: int,
        send_to_chat_peer: InputPeer,
        join_as_peer: InputPeer | None = None,
        to_listen_user_ids: typing.Collection[int] | None = None
    ) -> None:
        if self._is_running:
            return

        self._logger.info("Starting recorder...")

        self._is_running = True
        self._app_user_id = self._app.me.id  # type: ignore

        from .worker import RecorderWorker

        self.worker = RecorderWorker(
            parent = self,
            listen_chat_id = listen_chat_id,
            send_to_chat_peer = send_to_chat_peer,
            to_listen_user_ids = to_listen_user_ids
        )

        for chat_id in (await self._call_py_binding.calls()).keys():  # type: ignore
            chat_id = typing.cast(int, chat_id)

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
            ),
            config = GroupCallConfig(
                join_as = join_as_peer
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
            except calls_exceptions.NoActiveGroupCall:
                pass

            if self.worker.is_running:
                await self.worker.stop()

            self.worker = None

        self._logger.info("Recorder stopped")
