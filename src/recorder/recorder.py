from pyrogram.client import Client
from pyrogram.raw.base.input_peer import InputPeer
from pytgcalls import PyTgCalls, exceptions as calls_exceptions
from pytgcalls.types import GroupCallParticipant, UpdatedGroupCallParticipant, AudioQuality, StreamFrames

from logging import Logger

import typing

from .enums import FrameDeviceEnum, generate_devices_enum_dict


class RecorderPy:
    def __init__(
        self,
        logger: Logger,
        app: Client,
        call_py: PyTgCalls,
        audio_quality: AudioQuality,
        max_durations: dict[FrameDeviceEnum, float] = generate_devices_enum_dict(15.),
        silence_duration: float = 3.,
        silence_threshold: float = 0.01,
        latest_frame_detector_duration: float = 5.,
        upload_files_workers_count: int = 1,
        write_log_debug_progress: bool = False
    ):
        if not max_durations:
            raise ValueError("max_durations must not be empty")

        self._logger = logger
        self._app = app
        self._call_py = call_py
        self._call_py_binding = call_py._binding
        self._audio_quality = audio_quality

        from .worker import RecorderWorker

        self._is_running = False
        self.worker: RecorderWorker | None = None

        self._app_user_id: int | None = None
        self._audio_sample_rate = audio_quality.value[0]
        self._audio_channels = audio_quality.value[1]
        self._audio_channel_second_rate = self._audio_sample_rate * self._audio_channels * 2
        self._max_durations = max_durations
        self._audio_max_duration_in_size: int = int(self._audio_channel_second_rate * max_durations[FrameDeviceEnum.MICROPHONE])
        self._audio_silence_duration_in_size = int(self._audio_channel_second_rate * silence_duration)
        self._silence_threshold = silence_threshold
        self._latest_frame_detector_duration = latest_frame_detector_duration
        self._upload_files_workers_count = upload_files_workers_count
        self._write_log_debug_progress = write_log_debug_progress

    @property
    def is_running(self) -> bool:
        return self._is_running and bool(self.worker) and self.worker.is_running

    @property
    def join_chat_id(self) -> int | None:
        if self.worker:
            return self.worker.join_chat_id

        return None

    async def _process_participant_joined(self, participant: GroupCallParticipant) -> None:
        if not self.worker or not self.worker.is_running or (self.worker.to_listen_user_ids and participant.user_id not in self.worker.to_listen_user_ids):
            return

        self.worker._process_participant_joined(participant)

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

    async def process_stream_frames_update(self, update: StreamFrames) -> None:
        if not self._is_running or not self.worker or not self.worker.is_running:
            return

        await self.worker.process_stream_frames_update(update)

    async def start(
        self,
        join_chat_id: int,
        send_to_chat_peer: InputPeer,
        join_as_id: int | None = None,
        join_as_peer: InputPeer | None = None,
        to_listen_user_ids: typing.Collection[int] | None = None
    ) -> None:
        if self._is_running:
            return

        self._logger.info("Starting recorder...")

        self._call_py_binding.enable_h264_encoder(True)

        self._is_running = True
        self._app_user_id = self._app.me.id  # type: ignore

        from .worker import RecorderWorker

        self.worker = RecorderWorker(
            parent = self,
            join_chat_id = join_chat_id,
            audio_quality = self._audio_quality,
            send_to_chat_peer = send_to_chat_peer,
            join_as_id = join_as_id or self._app_user_id,
            join_as_peer = join_as_peer,
            to_listen_user_ids = to_listen_user_ids
        )

        for chat_id in (await self._call_py_binding.calls()).keys():  # type: ignore
            chat_id = typing.cast(int, chat_id)

            if chat_id != join_chat_id:
                try:
                    await self._call_py.leave_call(chat_id)
                except Exception as ex:
                    self._logger.exception(f"Error while leaving call with chat ID {chat_id}", exc_info=ex)

        await self.worker.start()

        self._logger.info("Recorder started")

    async def stop(self) -> None:
        if not self._is_running:
            return

        self._logger.info("Stopping recorder...")

        self._is_running = False

        if self.worker:
            try:
                await self._call_py.leave_call(self.worker.join_chat_id)
            except calls_exceptions.NoActiveGroupCall:
                pass

            if self.worker.is_running:
                await self.worker.stop()

            self.worker = None

        self._logger.info("Recorder stopped")
