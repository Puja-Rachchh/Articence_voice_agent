import threading

import pyttsx3


class TextToSpeechEngine:
    def __init__(self, rate: int = 180, volume: float = 1.0) -> None:
        self._engine = pyttsx3.init()
        self._engine.setProperty("rate", rate)
        self._engine.setProperty("volume", volume)
        self._lock = threading.Lock()

    def speak(self, text: str) -> None:
        with self._lock:
            self._engine.say(text)
            self._engine.runAndWait()

    def speak_async(self, text: str) -> None:
        thread = threading.Thread(target=self.speak, args=(text,), daemon=True)
        thread.start()
