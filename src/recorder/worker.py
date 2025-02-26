from io import BytesIO

from pyrogram import raw
from pyrogram.session import Session
from pytgcalls.types import Frame

from hashlib import md5

import numpy as np
import soundfile as sf
import asyncio
import typing

from .recorder import RecorderPy


FILE_EXT = "ogg"
FILE_MIME_TYPE = "audio/ogg"

NOT_BIG_MAX_FILE_SIZE = 10 * 1024 * 1024


class RecorderWorker:
    def __init__(
        self,
        parent: "RecorderPy",
        listen_chat_id: int,
        send_to_chat_peer: raw.base.InputPeer,
        max_duration: float = 15.,
        silence_threshold: float = 3.,
        upload_files_workers_count: int = 1
    ):
        self._parent = parent
        self.listen_chat_id = listen_chat_id
        self._send_to_chat_peer = send_to_chat_peer

        self._logger = parent._logger
        self._app = parent._app
        self._channels = parent._channels
        self._sample_rate = parent._sample_rate
        self._channel_second_rate = parent._channel_second_rate

        self._pcm_max_duration_in_size = int(self._channel_second_rate * max_duration)
        self._pcm_silence_threshold_in_size = int(self._channel_second_rate * silence_threshold)
        self._upload_files_workers_count = upload_files_workers_count

        self._is_running = False
        self._sender_task = None
        self._participants_monitor_task = None
        self._process_pcm_locks: dict[int, asyncio.Lock] = {}
        self._upload_files_workers: list[asyncio.Task[None]] | None = None
        self._upload_files_workers_rpc_queue = asyncio.Queue()
        self._app_file_session: Session | None = None
        self._pcm_frame_queue: asyncio.Queue[Frame] = asyncio.Queue()

    @property
    def is_running(self) -> bool:
        return self._is_running

    def _log_info(self, user_id: int | None, msg, **kwargs) -> None:
        self._logger.info(f"[{self.listen_chat_id}:{user_id or ''}] {msg}", **kwargs)

    def _log_exception(self, user_id: int | None, msg, ex, **kwargs) -> None:
        self._logger.exception(f"[{self.listen_chat_id}:{user_id or ''}] {msg}", exc_info=ex, **kwargs)

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

            try:
                await self._app_file_session.invoke(rpc)

            except Exception as ex:
                self._log_exception(None, f"Error while uploading voice file | Upload RPC: {rpc}", ex)

                return

            media_subrpc = raw.types.InputMediaUploadedDocument(
                mime_type = FILE_MIME_TYPE,
                file = file_subrpc,
                attributes = [
                    raw.types.DocumentAttributeAudio(
                        voice = True,
                        duration = int(duration)
                    )
                ]
            )

            try:
                await self._app.invoke(
                    raw.functions.messages.SendMedia(
                        peer = self._send_to_chat_peer,
                        media = media_subrpc,
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

        # with open(f"temp-{file_id}.{self.FILE_EXT}", "wb") as f:
        #     f.write(content)

        content_len = len(content)

        is_big = content_len > NOT_BIG_MAX_FILE_SIZE
        filename = f"file-{file_id}.{FILE_EXT}"

        if not is_big:
            rpc = raw.functions.upload.SaveFilePart(
                file_id = file_id,
                file_part = 0,
                bytes = content
            )

            file_subrpc = raw.types.InputFile(
                id = file_id,
                parts = 1,
                name = filename,
                md5_checksum = md5(content).hexdigest()
            )

        else:
            rpc = raw.functions.upload.SaveBigFilePart(
                file_id = file_id,
                file_part = 0,
                file_total_parts = 1,
                bytes = content
            )

            file_subrpc = raw.types.InputFileBig(
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

                progress = self._pcm_buffers_sizes[user_id] / self._pcm_max_duration_in_size * 100
                print(f"[{user_id}] Current PCM buffer status: {self._pcm_buffers_sizes[user_id]} bytes | {progress} %")

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

            await self._process_pcm_buffer(user_id)

        self._log_info(None, "Background sender task finished.")

    async def process_pcm_frame(self, frame: Frame) -> None:
        """
        Puts a PCM frame into the queue to be processed by the background sender.
        """
        ssrc = frame.ssrc
        user_id = self._parent.SSRC_AND_TG_ID.get(ssrc, None)

        if not user_id:
            return

        await self._pcm_frame_queue.put((user_id, frame.frame))

    async def _participants_monitor(self) -> None:
        while self._is_running:
            await asyncio.sleep(3)

            try:
                participants = await self._parent._call_py.get_participants(self.listen_chat_id)

            except Exception as ex:
                self._log_exception(None, "Error while getting participants", ex)

                continue

            for participant in participants:
                user_id = participant.user_id

                if user_id == self._app.me.id:
                    continue

                self._parent.SSRC_AND_TG_ID[participant.source] = user_id

            self._log_info(None, f"Participants in chat {self.listen_chat_id}: {len(participants)}")

    async def wrapper_logger(self, coro) -> typing.Any:
        """
        Wrapper for coroutines that logs exceptions.
        """
        try:
            return await coro
        except Exception as ex:
            self._log_exception(None, "Error in coroutine", ex)

    async def start(self) -> None:
        """
        Start the model.
        """
        if self._is_running:
            raise ValueError("Model is already running")

        self._is_running = True

        self._app_file_session = Session(
            self._parent._app,
            await self._parent._app.storage.dc_id(),
            await self._parent._app.storage.auth_key(),
            await self._parent._app.storage.test_mode(),
            is_media = True
        )

        await self._app_file_session.start()

        self._sender_task = asyncio.create_task(self.wrapper_logger(self._background_sender()))
        self._participants_monitor_task = asyncio.create_task(self.wrapper_logger(self._participants_monitor()))

        self._current_file_workers = [
            asyncio.create_task(self._upload_files_worker())
            for _ in range(self._parent._upload_files_workers_count)
        ]

        self._log_info(None, "Session started")

    async def stop(self) -> None:
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
