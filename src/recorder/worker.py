from pyrogram.raw.base.input_peer import InputPeer
from pyrogram.session.session import Session
from pyrogram.raw.types.input_media_uploaded_document import InputMediaUploadedDocument
from pyrogram.raw.types.document_attribute_audio import DocumentAttributeAudio
from pyrogram.raw.functions.messages.send_media import SendMedia
from pyrogram.raw.functions.upload.save_file_part import SaveFilePart
from pyrogram.raw.types.input_file import InputFile
from pyrogram.raw.functions.upload.save_big_file_part import SaveBigFilePart
from pyrogram.raw.types.input_file_big import InputFileBig
from pytgcalls.types import AudioQuality, GroupCallParticipant, StreamFrames, RecordStream, GroupCallConfig

from io import BytesIO
from bidict import bidict
from hashlib import md5
from time import time

import numpy as np
import soundfile as sf
import asyncio
import typing

from src import utils
from .recorder import RecorderPy
from .enums import FrameDeviceEnum, CALLS_DEVICE_TO_FRAME_DEVICE, generate_devices_enum_dict


NOT_BIG_MAX_FILE_SIZE = 10 * 1024 * 1024


T = typing.TypeVar("T")


class RecorderWorker:
    def _convert_audio_pcm_to_ogg(self, data: bytes) -> bytes:
        """
        Convert raw audio PCM bytes to OGG/OPUS.
        """
        if not data:
            return b""

        audio_data = np.frombuffer(data, dtype=np.int16)

        if self._audio_channels > 1:
            audio_data = audio_data.reshape(-1, self._audio_channels)

        ogg_buffer = BytesIO()

        sf.write(
            file = ogg_buffer,
            data = audio_data,
            samplerate = self._audio_sample_rate,
            format = "OGG",
            subtype = "OPUS"
        )

        return ogg_buffer.getvalue()

    # TODO: fix type hinting for DEVICES_DATA_CONVERTERS and DEVICES_DURATION_PROCESSORS in callbacks

    DEVICES_DATA_CONVERTERS: dict[FrameDeviceEnum, typing.Callable[[bytes], bytes]] = {
        FrameDeviceEnum.MICROPHONE: _convert_audio_pcm_to_ogg,  # type: ignore
        # TODO: camera and screen
    }

    def _get_audio_pcm_duration(self, data: bytes) -> float:
        return len(data) / self._audio_channel_second_rate

    DEVICES_DURATION_PROCESSORS: dict[FrameDeviceEnum, typing.Callable[[bytes], float]] = {
        FrameDeviceEnum.MICROPHONE: _get_audio_pcm_duration,  # type: ignore
        # TODO: camera and screen
    }

    def __init__(
        self,
        parent: RecorderPy,
        join_chat_id: int,
        audio_quality: AudioQuality,
        send_to_chat_peer: InputPeer,
        join_as_id: int,
        join_as_peer: InputPeer | None = None,
        to_listen_user_ids: typing.Collection[int] | None = None,
        latest_frame_detector_interval: float = 1.,
        participants_monitor_interval: float = 3.,
        none_participants_timeout: float = 30.
    ):
        self.join_chat_id = join_chat_id
        self._audio_quality = audio_quality
        self._send_to_chat_peer = send_to_chat_peer
        self._join_as_id = join_as_id
        self._join_as_peer = join_as_peer
        self.to_listen_user_ids = to_listen_user_ids
        self._latest_frame_detector_interval = latest_frame_detector_interval
        self._participants_monitor_interval = participants_monitor_interval
        self._none_participants_timeout = none_participants_timeout

        self._logger = parent._logger
        self._app = parent._app
        self._call_py = parent._call_py
        self._audio_channels = parent._audio_channels
        self._audio_sample_rate = parent._audio_sample_rate
        self._audio_channel_second_rate = parent._audio_channel_second_rate
        self._audio_max_duration_in_size = parent._audio_max_duration_in_size
        self._audio_silence_duration_in_size = parent._audio_silence_duration_in_size
        self._silence_threshold = parent._silence_threshold
        self._latest_frame_detector_duration = parent._latest_frame_detector_duration
        self._upload_files_workers_count = parent._upload_files_workers_count
        self._write_log_debug_progress = parent._write_log_debug_progress

        self._is_running = False
        self.ssrc_and_tg_id: dict[FrameDeviceEnum, bidict[int, int]] = generate_devices_enum_dict(None, bidict)
        self._sender_task: asyncio.Task[None] | None = None
        self._latest_frame_detector_task: asyncio.Task[None] | None = None
        self._participants_monitor_task: asyncio.Task[None] | None = None
        self._none_participants_first_time = 0
        self._upload_files_workers: list[asyncio.Task[None]] | None = None
        self._upload_files_workers_rpc_queue: asyncio.Queue[tuple[SaveFilePart | SaveBigFilePart, InputFile | InputFileBig, float, FrameDeviceEnum, int]] = asyncio.Queue()
        self._app_file_session: Session | None = None
        self._data_frame_queue: asyncio.Queue[tuple[FrameDeviceEnum, int, bytes]] = asyncio.Queue()
        self._data_buffers: dict[FrameDeviceEnum, dict[int, BytesIO]] = generate_devices_enum_dict(None, dict)
        self._audio_buffers_sizes: dict[int, int] = {}
        self._data_buffers_locks: dict[FrameDeviceEnum, dict[int, asyncio.Lock]] = generate_devices_enum_dict(None, dict)
        self._audio_silent_frames_size: dict[int, int] = {}
        self._data_latest_frame_receive_time: dict[FrameDeviceEnum, dict[int, float]] = generate_devices_enum_dict(None, dict)

    @property
    def is_running(self) -> bool:
        return self._is_running

    def _get_log_pre_str(self, user_id: int | None) -> str:
        return f"[{self.join_chat_id}:{user_id or ''}]"

    def _log_debug(self, user_id: int | None, msg: typing.Any, **kwargs: typing.Any) -> None:
        self._logger.debug(f"{self._get_log_pre_str(user_id)} {msg}", **kwargs)

    def _log_info(self, user_id: int | None, msg: typing.Any, **kwargs: typing.Any) -> None:
        self._logger.info(f"{self._get_log_pre_str(user_id)} {msg}", **kwargs)

    def _log_exception(self, user_id: int | None, msg: typing.Any, ex: Exception, **kwargs: typing.Any) -> None:
        self._logger.exception(f"{self._get_log_pre_str(user_id)} {msg}", exc_info=ex, **kwargs)

    async def _upload_files_worker(self) -> None:
        while self._is_running:
            try:
                rpc, file_subrpc, duration, device, user_id = await asyncio.wait_for(
                    self._upload_files_workers_rpc_queue.get(),
                    timeout = 0.1
                )

            except asyncio.TimeoutError:
                continue

            if not self._app_file_session:
                return

            try:
                await self._app_file_session.invoke(rpc)

            except Exception as ex:
                self._log_exception(None, f"Error while uploading voice file | Upload RPC: {rpc}", ex)

                return

            media_subrpc = InputMediaUploadedDocument(
                mime_type = device.value[1],
                file = file_subrpc,  # type: ignore
                attributes = [
                    DocumentAttributeAudio(  # type: ignore
                        voice = True,
                        duration = int(duration)
                    )
                ]
            )

            try:
                await self._app.invoke(
                    SendMedia(
                        peer = self._send_to_chat_peer,
                        media = media_subrpc,  # type: ignore
                        message = f"{self.join_chat_id} | {user_id}",
                        random_id = self._app.rnd_id()
                    )
                )

                self._log_info(user_id, "Voicw file sent")

            except Exception as ex:
                self._log_exception(user_id, f"Error while sending voice file | Media sub-RPC: {media_subrpc}", ex)

                return

    async def _upload_file(self, content: bytes, duration: float, device: FrameDeviceEnum, user_id: int) -> None:
        file_id = self._app.rnd_id()

        content_len = len(content)

        is_big = content_len > NOT_BIG_MAX_FILE_SIZE
        filename = f"file-{file_id}.{device.value[0]}"

        if not is_big:
            rpc = SaveFilePart(
                file_id = file_id,
                file_part = 0,
                bytes = content
            )

            file_subrpc = InputFile(
                id = file_id,
                parts = 1,
                name = filename,
                md5_checksum = md5(content).hexdigest()
            )

        else:
            rpc = SaveBigFilePart(
                file_id = file_id,
                file_part = 0,
                file_total_parts = 1,
                bytes = content
            )

            file_subrpc = InputFileBig(
                id = file_id,
                parts = 1,
                name = filename
            )

        await self._upload_files_workers_rpc_queue.put((rpc, file_subrpc, duration, device, user_id))

        self._log_info(user_id, f"RPC to file upload inserted into queue with:   File ID = {file_id} | Is big = {is_big}")

    async def _process_data_buffer(self, device: FrameDeviceEnum, user_id: int, read_max_size: int | None=None) -> None:
        async with self._data_buffers_locks[device][user_id]:
            data_buffer = self._data_buffers[device][user_id]

            if read_max_size:
                if read_max_size < 0:
                    read_max_size = data_buffer.tell() + read_max_size + 1

                data_bytes = data_buffer.read(read_max_size)

            else:
                data_bytes = data_buffer.getvalue()

            data_bytes_len = len(data_bytes)
            data_buffer.seek(0)
            data_buffer.truncate()

            if data_bytes_len == 0:
                return

            processed_data_bytes = self.DEVICES_DATA_CONVERTERS[device](data_bytes)
            processed_data_bytes_len = len(processed_data_bytes)

            self._log_info(user_id, f"Data converted to {device.value[0]} | DATA {data_bytes_len} => {device.value[0]} {processed_data_bytes_len}")

            await self._upload_file(
                processed_data_bytes,
                self.DEVICES_DURATION_PROCESSORS[device](processed_data_bytes),
                device,
                user_id
            )

    def _is_pcm_silent(self, pcm_data: bytes) -> bool:
        buffer = np.frombuffer(pcm_data, dtype=np.int16)

        if self._audio_channels > 1:
            buffer = buffer.reshape(-1, self._audio_channels).mean(axis=1)

        buffer = buffer.astype(np.float32) / np.iinfo(np.int16).max
        rms = np.sqrt(np.mean(np.square(buffer)))

        return rms < self._silence_threshold

    async def _sender(self) -> None:
        while self._is_running:
            try:
                device, user_id, chunk = await asyncio.wait_for(
                    self._data_frame_queue.get(),
                    timeout = 0.1
                )

            except asyncio.TimeoutError:
                continue

            if user_id not in self._data_buffers[device]:
                self._data_buffers[device][user_id] = BytesIO()
                self._data_buffers_locks[device][user_id] = asyncio.Lock()
                self._data_latest_frame_receive_time[device][user_id] = 0

                if device is FrameDeviceEnum.MICROPHONE:
                    self._audio_buffers_sizes[user_id] = 0
                    self._audio_silent_frames_size[user_id] = 0

            chunk_len = len(chunk)

            if device is FrameDeviceEnum.MICROPHONE:
                is_pcm_silent = self._is_pcm_silent(chunk)

                if is_pcm_silent:
                    self._audio_silent_frames_size[user_id] += chunk_len
                else:
                    self._audio_silent_frames_size[user_id] = 0

            self._data_latest_frame_receive_time[device][user_id] = time()

            if self._write_log_debug_progress:
                if device is FrameDeviceEnum.MICROPHONE:
                    progress = self._audio_buffers_sizes[user_id] / self._audio_max_duration_in_size * 100
                    self._log_debug(user_id, f"Current PCM buffer status: {self._audio_buffers_sizes[user_id]} bytes | {progress:.5f} %")

                else:
                    self._log_debug(user_id, f"Current PCM buffer status: {self._audio_buffers_sizes[user_id]} bytes | duration {self._audio_buffers_sizes[user_id] / self._audio_channel_second_rate:.5f} seconds")

            is_duration_enough = (
                self._audio_buffers_sizes[user_id] + chunk_len <= self._audio_max_duration_in_size
                if device is FrameDeviceEnum.MICROPHONE
                else
                True  # TODO: calculate for camera and screen
            )

            is_silence_enough = (
                self._audio_silent_frames_size[user_id] >= self._audio_silence_duration_in_size
                if device is FrameDeviceEnum.MICROPHONE
                else
                False
            )

            if is_duration_enough:
                self._data_buffers[device][user_id].write(chunk)

                if device is FrameDeviceEnum.MICROPHONE:
                    self._audio_buffers_sizes[user_id] += chunk_len

            if not is_duration_enough or is_silence_enough:
                if not is_duration_enough:
                    self._log_info(user_id, "Buffer is full, processing buffer")

                else:
                    self._log_info(user_id, "Silence detected, processing buffer")

                if device is FrameDeviceEnum.MICROPHONE:
                    if self._audio_buffers_sizes[user_id] == 0 and not is_pcm_silent:  # type: ignore
                        raise ValueError("Received audio chunk size is too big, so buffer is empty")

                    self._log_debug(user_id, f"Silent frames size: {self._audio_silent_frames_size[user_id]}")

                await self._process_data_buffer(device, user_id)

                if not is_duration_enough:
                    self._data_buffers[device][user_id] = BytesIO(chunk)
                    self._data_buffers[device][user_id].seek(chunk_len)

                    if device is FrameDeviceEnum.MICROPHONE:
                        self._audio_buffers_sizes[user_id] = chunk_len

                else:
                    self._data_buffers[device][user_id] = BytesIO()

                    if device is FrameDeviceEnum.MICROPHONE:
                        self._audio_buffers_sizes[user_id] = 0

                if device is FrameDeviceEnum.MICROPHONE:
                    self._audio_silent_frames_size[user_id] = 0

        for device, pcm_buffers_subdict in self._data_buffers.items():
            for user_id in pcm_buffers_subdict.keys():
                await self._process_data_buffer(device, user_id)

        self._log_debug(None, "Background sender task finished")

    async def process_stream_frames_update(self, update: StreamFrames) -> None:
        """
        Process incoming StreamFrames update.
        """
        device = CALLS_DEVICE_TO_FRAME_DEVICE.get(update.device, None)

        if not device:
            self._log_debug(None, f"Unknown device: {update.device}")
            return

        for frame in update.frames:
            ssrc = frame.ssrc
            user_id = self.ssrc_and_tg_id[device].get(ssrc, None)

            if not user_id:
                self._log_debug(user_id, f"Unknown user ID for SSRC: {ssrc}")
                return

            await self._data_frame_queue.put((device, user_id, frame.frame))

    async def _latest_frame_detector(self) -> None:
        while self._is_running:
            await asyncio.sleep(self._latest_frame_detector_interval)

            for device, pcm_buffers_subdict in self._data_buffers.items():
                for user_id in list(pcm_buffers_subdict.keys()):
                    if time() - self._data_latest_frame_receive_time[device][user_id] > self._latest_frame_detector_duration:
                        self._log_info(user_id, "Latest frame detector triggered")

                        await self._process_data_buffer(device, user_id)

                        del self._data_buffers[device][user_id]
                        del self._data_buffers_locks[device][user_id]
                        del self._data_latest_frame_receive_time[device][user_id]

                        if device is FrameDeviceEnum.MICROPHONE:
                            self._audio_buffers_sizes[user_id] = 0
                            self._audio_silent_frames_size[user_id] = 0

        self._log_debug(None, "Latest frame detector task finished")

    def _guess_participant_devices_and_ssrcs(self, participant: GroupCallParticipant) -> list[tuple[FrameDeviceEnum, int]]:
        ssrc = participant.source

        results = [
            (FrameDeviceEnum.MICROPHONE, ssrc)
        ]

        if participant.video_camera and participant.video_info:
            # self._log_debug(None, f"Guessing participant: {participant.user_id} - {participant.source} - camera sources {participant.video_info.sources} ({len(participant.video_info.sources)})")  # type: ignore

            # for i, source in enumerate(participant.video_info.sources):  # type: ignore
            #     self._log_debug(None, f"Source {i}: {source.semantics} {source.ssrcs}")

            for source in participant.video_info.sources:  # type: ignore
                if source.semantics == "SIM":
                    results.append((FrameDeviceEnum.CAMERA, source.ssrcs[0]))  # type: ignore

                    break

        # elif participant.screen_sharing:  # TODO: screen sharing

        return results

    def _update_participant_ssrcs(self, participant: GroupCallParticipant) -> None:
        for device, ssrc in self._guess_participant_devices_and_ssrcs(participant):
            self.ssrc_and_tg_id[device].inverse[participant.user_id] = ssrc

    async def _participants_monitor(self) -> None:
        while self._is_running:
            await asyncio.sleep(self._participants_monitor_interval)

            try:
                participants = typing.cast(
                    list[GroupCallParticipant],
                    await self._call_py.get_participants(self.join_chat_id)
                )

            except Exception as ex:
                self._log_exception(None, "Error while getting participants", ex)

                continue

            participants_count = len(participants)

            for participant in participants:
                # self._log_info(None, f"Participant: {participant.user_id} - {participant.source} - video {participant.video} - screen sharing {participant.screen_sharing} - video camera {participant.video_camera} - screen sharing {participant.screen_sharing}")  # TODO: remove
                user_id = participant.user_id

                if user_id == self._join_as_id:
                    participants_count -= 1

                    continue

                if self.to_listen_user_ids and user_id not in self.to_listen_user_ids:
                    self._log_info(user_id, "User ID is not in to listen user IDs")  # TODO: remove
                    continue

                self._update_participant_ssrcs(participant)

            self._log_debug(None, f"""Participants in chat: {participants_count} (until shutdown: {(self._none_participants_timeout - (utils.get_timestamp_int() - self._none_participants_first_time)) if self._none_participants_first_time else f">{self._none_participants_timeout}"} seconds)""")

            if participants_count != 0 or self._none_participants_first_time == 0:
                self._none_participants_first_time = utils.get_timestamp_int()

            elif utils.get_timestamp_int() - self._none_participants_first_time > self._none_participants_timeout:
                self._log_debug(None, f"No participants in chat for a long time ({self._none_participants_timeout} seconds) - stopping worker")

                self._participants_monitor_task = None

                await self.stop()

                break

        self._log_debug(None, "Participants monitor task finished")

    def _process_participant_joined(self, participant: GroupCallParticipant) -> None:
        self._log_info(participant.user_id, "Participant joined")

        self._update_participant_ssrcs(participant)

    async def _wrapper_logger(self, coro: typing.Awaitable[T]) -> T | None:
        try:
            return await coro
        except Exception as ex:
            self._log_exception(None, "Error in coroutine", ex)

    async def start(self) -> None:
        """
        Start the worker session to record voice chat.
        """

        if self._is_running:
            raise ValueError("Worker is already running")

        self._is_running = True

        self._app_file_session = Session(
            client = self._app,
            dc_id = await self._app.storage.dc_id(),  # type: ignore
            auth_key = await self._app.storage.auth_key(),  # type: ignore
            test_mode = await self._app.storage.test_mode(),  # type: ignore
            is_media = True
        )

        await self._app_file_session.start()

        self._sender_task = asyncio.create_task(self._wrapper_logger(self._sender()))
        self._latest_frame_detector_task = asyncio.create_task(self._wrapper_logger(self._latest_frame_detector()))
        self._participants_monitor_task = asyncio.create_task(self._wrapper_logger(self._participants_monitor()))

        self._current_file_workers = [
            asyncio.create_task(self._upload_files_worker())
            for _ in range(self._upload_files_workers_count)
        ]

        await self._call_py.record(
            chat_id = self.join_chat_id,
            stream = RecordStream(
                audio = True,
                audio_parameters = self._audio_quality,
                camera = True,
                screen = False  # TODO: screen sharing
            ),
            config = GroupCallConfig(
                join_as = self._join_as_peer
            )
        )

        self._log_info(None, "Worker session started")

    async def stop(self) -> None:
        """
        Stop the worker session.
        """

        if self._is_running is False:
            return

        self._is_running = False

        if self._sender_task:
            await self._sender_task

        if self._latest_frame_detector_task:
            await self._latest_frame_detector_task

        if self._participants_monitor_task:
            await self._participants_monitor_task

        if self._app_file_session:
            await self._app_file_session.stop()

        if self._current_file_workers:
            for current_file_worker in self._current_file_workers:
                await current_file_worker
