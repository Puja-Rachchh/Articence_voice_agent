import logging
import queue
import shutil
import threading
from typing import Generator

import numpy as np
import sounddevice as sd
import whisper


LOGGER = logging.getLogger(__name__)


class SpeechToTextStreamer:
    """Capture microphone audio continuously and yield Whisper transcriptions."""

    def __init__(
        self,
        model_name: str = "base.en",
        sample_rate: int = 16000,
        block_duration: float = 0.5,
        phrase_duration: float = 3.0,
        silence_threshold: float = 0.008,
    ) -> None:
        self.sample_rate = sample_rate
        self.block_size = int(sample_rate * block_duration)
        self.phrase_samples = int(sample_rate * phrase_duration)
        self.silence_threshold = silence_threshold
        self.model_name = model_name

        self._audio_queue: "queue.Queue[np.ndarray]" = queue.Queue()
        self._stop_event = threading.Event()
        self._model = None

    def _get_model(self):
        if self._model is None:
            self._model = whisper.load_model(self.model_name)
        return self._model

    @staticmethod
    def ffmpeg_available() -> bool:
        return shutil.which("ffmpeg") is not None

    def _audio_callback(self, indata: np.ndarray, frames: int, time_info, status) -> None:
        if status:
            LOGGER.warning("Input stream status: %s", status)
        self._audio_queue.put(indata.copy().reshape(-1))

    def _transcribe(self, audio: np.ndarray) -> str:
        if audio.size == 0:
            return ""

        # Skip mostly silent chunks to reduce unnecessary model work.
        if float(np.sqrt(np.mean(np.square(audio)))) < self.silence_threshold:
            return ""

        result = self._get_model().transcribe(
            audio,
            fp16=False,
            language="en",
            task="transcribe",
            verbose=False,
        )
        return result.get("text", "").strip()

    def transcribe_file(self, file_path: str) -> str:
        if not self.ffmpeg_available():
            raise RuntimeError(
                "FFmpeg is required for voice transcription but was not found in PATH. "
                "Install FFmpeg and restart the server."
            )

        result = self._get_model().transcribe(
            file_path,
            fp16=False,
            language="en",
            task="transcribe",
            verbose=False,
        )
        return result.get("text", "").strip()

    def listen(self) -> Generator[str, None, None]:
        """Yield transcribed phrases until stop() is called."""
        buffer: list[np.ndarray] = []
        collected = 0

        with sd.InputStream(
            samplerate=self.sample_rate,
            channels=1,
            dtype="float32",
            blocksize=self.block_size,
            callback=self._audio_callback,
        ):
            LOGGER.info("Microphone stream started")

            while not self._stop_event.is_set():
                try:
                    chunk = self._audio_queue.get(timeout=0.25)
                except queue.Empty:
                    continue

                buffer.append(chunk)
                collected += len(chunk)

                if collected < self.phrase_samples:
                    continue

                audio = np.concatenate(buffer, dtype=np.float32)
                buffer.clear()
                collected = 0

                transcript = self._transcribe(audio)
                if transcript:
                    yield transcript

    def stop(self) -> None:
        self._stop_event.set()
