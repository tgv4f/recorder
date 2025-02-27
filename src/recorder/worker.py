from pyrogram.raw.base.input_peer import InputPeer
from pyrogram.session.session import Session
from pyrogram.raw.types.input_media_uploaded_document import InputMediaUploadedDocument
from pyrogram.raw.types.document_attribute_audio import DocumentAttributeAudio
from pyrogram.raw.functions.messages.send_media import SendMedia
from pyrogram.raw.functions.upload.save_file_part import SaveFilePart
from pyrogram.raw.types.input_file import InputFile
from pyrogram.raw.functions.upload.save_big_file_part import SaveBigFilePart
from pyrogram.raw.types.input_file_big import InputFileBig
from pytgcalls.types import GroupCallParticipant, Frame

from io import BytesIO
from bidict import bidict
from hashlib import md5

import numpy as np
import soundfile as sf
import asyncio
import typing

from .recorder import RecorderPy


FILE_EXT = "ogg"
FILE_MIME_TYPE = "audio/ogg"

NOT_BIG_MAX_FILE_SIZE = 10 * 1024 * 1024


T = typing.TypeVar("T")


class RecorderWorker:
    def __init__(
        self,
        parent: RecorderPy,
        listen_chat_id: int,
        send_to_chat_peer: InputPeer,
        to_listen_user_ids: typing.Collection[int] | None = None,
        max_duration: float = 15.,
        silence_threshold: float = 3.
    ):
        self.listen_chat_id = listen_chat_id
        self._send_to_chat_peer = send_to_chat_peer
        self.to_listen_user_ids = to_listen_user_ids

        self._logger = parent._logger
        self._app = parent._app
        self._call_py = parent._call_py
        self._app_user_id = parent._app_user_id
        self._channels = parent._channels
        self._sample_rate = parent._sample_rate
        self._channel_second_rate = parent._channel_second_rate
        self._upload_files_workers_count = parent._upload_files_workers_count
        self._write_log_debug_progress = parent._write_log_debug_progress

        self._pcm_max_duration_in_size = int(self._channel_second_rate * max_duration)
        self._pcm_silence_threshold_in_size = int(self._channel_second_rate * silence_threshold)

        self._is_running = False
        self.ssrc_and_tg_id: bidict[int, int] = bidict()
        self._sender_task: asyncio.Task[None] | None = None
        self._participants_monitor_task: asyncio.Task[None] | None = None
        self._process_pcm_locks: dict[int, asyncio.Lock] = {}
        self._upload_files_workers: list[asyncio.Task[None]] | None = None
        self._upload_files_workers_rpc_queue: asyncio.Queue[tuple[SaveFilePart | SaveBigFilePart, InputFile | InputFileBig, float, int]] = asyncio.Queue()
        self._app_file_session: Session | None = None
        self._pcm_frame_queue: asyncio.Queue[tuple[int, bytes]] = asyncio.Queue()

    @property
    def is_running(self) -> bool:
        return self._is_running

    def _get_log_pre_str(self, user_id: int | None) -> str:
        return f"[{self.listen_chat_id}:{user_id or ''}]"

    def _log_debug(self, user_id: int | None, msg: typing.Any, **kwargs: typing.Any) -> None:
        self._logger.debug(f"{self._get_log_pre_str(user_id)} {msg}", **kwargs)

    def _log_info(self, user_id: int | None, msg: typing.Any, **kwargs: typing.Any) -> None:
        self._logger.info(f"{self._get_log_pre_str(user_id)} {msg}", **kwargs)

    def _log_exception(self, user_id: int | None, msg: typing.Any, ex: Exception, **kwargs: typing.Any) -> None:
        self._logger.exception(f"{self._get_log_pre_str(user_id)} {msg}", exc_info=ex, **kwargs)

    def _pcm_to_ogg(self, data: bytes) -> bytes:
        """
        Convert raw PCM bytes to OGG/OPUS.
        """
        if not data:
            return b""

        audio_data = np.frombuffer(data, dtype=np.int16)

        if self._channels > 1:
            audio_data = audio_data.reshape(-1, self._channels)

        ogg_buffer = BytesIO()

        sf.write(
            file = ogg_buffer,
            data = audio_data,
            samplerate = self._sample_rate,
            format = "OGG",
            subtype = "OPUS"
        )

        return ogg_buffer.getvalue()

    async def _upload_files_worker(self) -> None:
        while self._is_running:
            try:
                rpc, file_subrpc, duration, user_id = await asyncio.wait_for(
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
                mime_type = FILE_MIME_TYPE,
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
                        message = f"{self.listen_chat_id} | {user_id}",
                        random_id = self._app.rnd_id()
                    )
                )

                self._log_info(user_id, "Voicw file sent")

            except Exception as ex:
                self._log_exception(user_id, f"Error while sending voice file | Media sub-RPC: {media_subrpc}", ex)

                return

    async def _upload_file(self, content: bytes, duration: float, user_id: int) -> None:
        file_id = self._app.rnd_id()

        content_len = len(content)

        is_big = content_len > NOT_BIG_MAX_FILE_SIZE
        filename = f"file-{file_id}.{FILE_EXT}"

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

        await self._upload_files_workers_rpc_queue.put((rpc, file_subrpc, duration, user_id))

        self._log_info(user_id, f"RPC to file upload inserted into queue with:   File ID = {file_id} | Is big = {is_big}")

    async def _process_pcm_buffer(self, user_id: int) -> None:
        async with self._pcm_buffers_locks[user_id]:
            pcm_buffer = self._pcm_buffers[user_id]
            pcm_data = pcm_buffer.getvalue()
            pcm_data_len = len(pcm_data)
            pcm_buffer.seek(0)
            pcm_buffer.truncate()

            if pcm_data_len == 0:
                return

            ogg_data = self._pcm_to_ogg(pcm_data)
            ogg_data_len = len(ogg_data)

            self._log_info(user_id, f"PCM converted to OGG | PCM {pcm_data_len} => OGG {ogg_data_len}")

            await self._upload_file(
                ogg_data,
                pcm_data_len / self._channel_second_rate,
                user_id
            )

    async def _background_sender(self) -> None:
        while self._is_running:
            self._pcm_buffers: dict[int, BytesIO] = {}
            self._pcm_buffers_sizes: dict[int, int] = {}
            self._pcm_buffers_locks: dict[int, asyncio.Lock] = {}

            while self._is_running:
                try:
                    user_id, chunk = await asyncio.wait_for(
                        self._pcm_frame_queue.get(),
                        timeout = 0.1
                    )

                except asyncio.TimeoutError:
                    continue

                if user_id not in self._pcm_buffers:
                    self._pcm_buffers[user_id] = BytesIO()
                    self._pcm_buffers_sizes[user_id] = 0
                    self._pcm_buffers_locks[user_id] = asyncio.Lock()

                chunk_len = len(chunk)

                if self._write_log_debug_progress:
                    progress = self._pcm_buffers_sizes[user_id] / self._pcm_max_duration_in_size * 100
                    self._log_debug(user_id, f"Current PCM buffer status: {self._pcm_buffers_sizes[user_id]} bytes | {progress:.5f} %")

                if self._pcm_buffers_sizes[user_id] + chunk_len <= self._pcm_max_duration_in_size:
                    self._pcm_buffers[user_id].write(chunk)
                    self._pcm_buffers_sizes[user_id] += chunk_len

                else:
                    if self._pcm_buffers_sizes[user_id] == 0:
                        raise ValueError("Received chunk size is too big, so buffer is empty")

                    await self._process_pcm_buffer(user_id)

                    self._pcm_buffers[user_id] = BytesIO(chunk)
                    self._pcm_buffers[user_id].seek(chunk_len)
                    self._pcm_buffers_sizes[user_id] = chunk_len

            for user_id in self._pcm_buffers.keys():
                await self._process_pcm_buffer(user_id)

        self._log_info(None, "Background sender task finished.")

    async def process_pcm_frame(self, frame: Frame) -> None:
        """
        Process incoming PCM frame.
        """
        ssrc = frame.ssrc
        user_id = self.ssrc_and_tg_id.get(ssrc, None)

        if not user_id:
            return

        await self._pcm_frame_queue.put((user_id, frame.frame))

    async def _participants_monitor(self) -> None:
        while self._is_running:
            await asyncio.sleep(3)

            try:
                participants = typing.cast(
                    list[GroupCallParticipant],
                    await self._call_py.get_participants(self.listen_chat_id)
                )

            except Exception as ex:
                self._log_exception(None, "Error while getting participants", ex)

                continue

            for participant in participants:
                user_id = participant.user_id

                if user_id == self._app_user_id or (self.to_listen_user_ids and user_id not in self.to_listen_user_ids):
                    continue

                self.ssrc_and_tg_id.inverse[user_id] = participant.source

            self._log_debug(None, f"Participants in chat {self.listen_chat_id}: {len(participants)}")

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
            raise ValueError("Model is already running")

        self._is_running = True

        self._app_file_session = Session(
            self._app,
            await self._app.storage.dc_id(),  # type: ignore
            await self._app.storage.auth_key(),  # type: ignore
            await self._app.storage.test_mode(),  # type: ignore
            is_media = True
        )

        await self._app_file_session.start()

        self._sender_task = asyncio.create_task(self._wrapper_logger(self._background_sender()))
        self._participants_monitor_task = asyncio.create_task(self._wrapper_logger(self._participants_monitor()))

        self._current_file_workers = [
            asyncio.create_task(self._upload_files_worker())
            for _ in range(self._upload_files_workers_count)
        ]

        self._log_info(None, "Session started")

    async def stop(self) -> None:
        """
        Stop the worker session.
        """

        if self._is_running is False:
            return

        self._is_running = False

        if self._sender_task:
            await self._sender_task

        if self._participants_monitor_task:
            await self._participants_monitor_task

        if self._app_file_session:
            await self._app_file_session.stop()

        if self._current_file_workers:
            for current_file_worker in self._current_file_workers:
                await current_file_worker
