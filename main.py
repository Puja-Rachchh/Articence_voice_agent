import logging
import os
import tempfile
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from database import SnowflakeClient
from query_processor import ConversationSession, QueryProcessor
from speech_to_text import SpeechToTextStreamer
from text_to_speech import TextToSpeechEngine


LOGGER = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
conversation_sessions: dict[str, ConversationSession] = {}


def server_tts_enabled() -> bool:
    return os.getenv("ENABLE_SERVER_TTS", "false").strip().lower() in {"1", "true", "yes", "on"}


def load_dotenv(dotenv_path: str = ".env") -> None:
    path = BASE_DIR / dotenv_path
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def get_or_create_session(session_id: str | None) -> tuple[str, ConversationSession]:
    if session_id and session_id in conversation_sessions:
        return session_id, conversation_sessions[session_id]

    new_session_id = session_id or str(uuid.uuid4())
    session = ConversationSession()
    conversation_sessions[new_session_id] = session
    return new_session_id, session


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    load_dotenv()

    db = SnowflakeClient.from_env()
    db.connect()

    app.state.db = db
    app.state.processor = QueryProcessor(db)
    app.state.tts = TextToSpeechEngine()
    app.state.stt = SpeechToTextStreamer(
        model_name=os.getenv("WHISPER_MODEL", "base.en"),
        sample_rate=int(os.getenv("STT_SAMPLE_RATE", "16000")),
        phrase_duration=float(os.getenv("STT_PHRASE_DURATION", "3.0")),
    )

    try:
        yield
    finally:
        db.close()


app = FastAPI(title="Articence Voice Support Agent", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "request": request,
            "app_name": "Articence Voice Support Agent",
        },
    )


@app.get("/api/health")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/session")
async def create_session() -> dict[str, str | bool]:
    session_id, session = get_or_create_session(None)
    return {
        "session_id": session_id,
        "verified": session.verified,
        "customer_name": session.customer_name or "",
    }


@app.post("/api/verify")
async def verify_customer_identity(
    request: Request,
    phone: str = Form(...),
    email: str = Form(...),
    session_id: str | None = Form(default=None),
) -> dict[str, str | bool]:
    session_id, session = get_or_create_session(session_id)
    verified, message, session = request.app.state.processor.verify_identity(phone, email, session)
    conversation_sessions[session_id] = session

    return {
        "session_id": session_id,
        "verified": verified,
        "response": message,
        "customer_name": session.customer_name or "",
    }


@app.post("/api/text-query")
async def process_text_query(
    request: Request,
    query: str = Form(...),
    session_id: str | None = Form(default=None),
) -> dict[str, str | bool | float]:
    start_time = time.perf_counter()
    session_id, session = get_or_create_session(session_id)
    if not session.verified:
        raise HTTPException(status_code=403, detail="Please verify your phone number and email before asking queries.")

    intent_result, session = request.app.state.processor.process_query_with_intent(query, session)
    conversation_sessions[session_id] = session
    if server_tts_enabled():
        request.app.state.tts.speak_async(intent_result.response)

    latency_ms = round((time.perf_counter() - start_time) * 1000, 2)

    return {
        "session_id": session_id,
        "transcript": query,
        "response": intent_result.response,
        "intent": intent_result.intent,
        "confidence": intent_result.confidence,
        "latency_ms": latency_ms,
        "verified": session.verified,
        "customer_name": session.customer_name or "",
    }


@app.post("/api/voice-query")
async def process_voice_query(
    request: Request,
    audio: UploadFile = File(...),
    session_id: str | None = Form(default=None),
) -> dict[str, str | bool | float]:
    start_time = time.perf_counter()
    suffix = Path(audio.filename or "audio.webm").suffix or ".webm"
    session_id, session = get_or_create_session(session_id)
    if not session.verified:
        raise HTTPException(status_code=403, detail="Please verify your phone number and email before voice queries.")

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
        temp_file.write(await audio.read())
        temp_path = temp_file.name

    try:
        transcript = request.app.state.stt.transcribe_file(temp_path)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        LOGGER.exception("Whisper transcription failed")
        raise HTTPException(status_code=500, detail=f"Transcription failed: {exc}") from exc
    finally:
        Path(temp_path).unlink(missing_ok=True)

    if not transcript:
        latency_ms = round((time.perf_counter() - start_time) * 1000, 2)
        return {
            "session_id": session_id,
            "transcript": "",
            "response": "I did not catch that. Please try again.",
            "latency_ms": latency_ms,
            "verified": session.verified,
            "customer_name": session.customer_name or "",
        }

    intent_result, session = request.app.state.processor.process_query_with_intent(transcript, session)
    conversation_sessions[session_id] = session
    if server_tts_enabled():
        request.app.state.tts.speak_async(intent_result.response)

    latency_ms = round((time.perf_counter() - start_time) * 1000, 2)

    return {
        "session_id": session_id,
        "transcript": transcript,
        "response": intent_result.response,
        "intent": intent_result.intent,
        "confidence": intent_result.confidence,
        "latency_ms": latency_ms,
        "verified": session.verified,
        "customer_name": session.customer_name or "",
    }


def run() -> None:
    uvicorn.run(
        "main:app",
        host=os.getenv("APP_HOST", "127.0.0.1"),
        port=int(os.getenv("APP_PORT", "8000")),
        reload=False,
    )


if __name__ == "__main__":
    run()
