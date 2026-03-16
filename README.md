# Articence Voice Support Agent

Production-style customer support assistant for electronics orders with:

- FastAPI backend + HTML/CSS/JS chat UI
- Snowflake-backed customer and order lookup
- Verify-first workflow (phone + email)
- Hybrid intent routing (semantic + regex patterns + guardrails)
- Voice support in browser and optional server-side transcription/TTS
- Latency display per response in conversation UI

## 1. Overview

This project provides a customer-support assistant focused on electronics orders. It verifies customers using phone and email, then answers order-related questions such as return eligibility, warranty, AppleCare, replacement, recent order lookup, price, and purchase date.

The assistant supports text input and voice interaction. Responses are shown in the conversation panel and can also be spoken in the browser.

## 2. Features

- Verify-first support flow before account-specific queries.
- Snowflake integration for customer verification and order history.
- Intent detection with confidence score.
- Hybrid intent strategy:
  - semantic retrieval (`sentence-transformers` + `faiss-cpu`)
  - regex pattern fallback
  - explicit guardrails to prevent unrelated query misrouting
- Latency measurement (`latency_ms`) returned by backend and displayed in UI.
- Voice query support:
  - browser Web Speech API path for real-time transcription
  


## 3. Tech Stack

- Backend: FastAPI, Uvicorn
- Database: Snowflake (`snowflake-connector-python`)
- Intent Semantics: `sentence-transformers`, `faiss-cpu`

- Frontend Voice: Web Speech API + `speechSynthesis`
- TTS (optional server-side): `pyttsx3`

## 4. Project Structure

- `main.py`: FastAPI app, lifecycle, endpoints, session map, latency reporting
- `query_processor.py`: intent detection, guardrails, intent handlers, response generation
- `semantic_intent_router.py`: semantic intent retrieval using embeddings + FAISS
- `database.py`: Snowflake connection, verification, order fetch
- `business_rules.py`: policy logic (returns, warranty, AppleCare, order limits)
- `speech_to_text.py`: Whisper transcription utilities
- `text_to_speech.py`: server-side TTS wrapper
- `templates/index.html`: web UI template
- `static/app.js`: verify flow, chat rendering, voice input/output, latency display
- `static/styles.css`: UI styling
- `.env.example`: required environment settings
- `requirements.txt`: Python dependencies
- `entrypoint.py`: compatibility runner

## 5. Prerequisites

- Python 3.10+
- Access to a Snowflake account with required tables/data
- Windows PowerShell (or equivalent shell)
- For server audio transcription path (`/api/voice-query`): `ffmpeg` in `PATH`

## 6. Setup

1. Clone and enter project.

```powershell
git clone <your-repo-url>
cd articence
```

2. Create virtual environment.

```powershell
python -m venv .venv
```

3. Activate virtual environment.

```powershell
.\.venv\Scripts\Activate.ps1
```

4. Install dependencies.

```powershell
pip install -r requirements.txt
```

5. Create `.env` from template and fill credentials.

```powershell
Copy-Item .env.example .env
```

## 7. Configuration

Set the following values in `.env`:

```env
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_ROLE=
SNOWFLAKE_DATABASE=ARTICENCE_ORDERS
SNOWFLAKE_SCHEMA=CUSTOMER_DATA

WHISPER_MODEL=base.en
STT_SAMPLE_RATE=16000
STT_PHRASE_DURATION=3.0

APP_HOST=127.0.0.1
APP_PORT=8000

ENABLE_SERVER_TTS=false

USE_SEMANTIC_INTENT_ROUTER=true
SEMANTIC_MODEL_NAME=all-MiniLM-L6-v2
SEMANTIC_INTENT_THRESHOLD=0.55
```

Notes:

- Keep `ENABLE_SERVER_TTS=false` when browser TTS is active to avoid double audio.
- If semantic dependencies are unavailable, app falls back to regex intent routing.

## 8. Running the App

```powershell
python main.py
```

Open:

`http://127.0.0.1:8000`

## 9. User Flow

1. Open app and establish session (`/api/session`).
2. Verify using phone + email (`/api/verify`).
3. Ask text query (`/api/text-query`) or voice query.
4. Assistant returns:
   - response text
   - detected intent
   - confidence
   - latency in milliseconds
5. UI shows conversation entries and `Agent • <latency> ms` label.

## 10. API Endpoints

- `GET /`: UI page
- `GET /api/health`: health check
- `POST /api/session`: create/retrieve frontend session context
- `POST /api/verify`: verify customer (`phone`, `email`, optional `session_id`)
- `POST /api/text-query`: process text query (`query`, optional `session_id`)
- `POST /api/voice-query`: process uploaded audio (`audio`, optional `session_id`)

Sample `POST /api/text-query` response:

```json
{
  "session_id": "...",
  "transcript": "can i return my ipad",
  "response": "Yes, your ipad purchased on ... is eligible for return until ...",
  "intent": "return",
  "confidence": 0.86,
  "latency_ms": 155.21,
  "verified": true,
  "customer_name": "..."
}
```

## 11. Architecture

```text
Browser UI (index.html + app.js)
  -> FastAPI (main.py)
      -> QueryProcessor (intent + rules)
          -> SemanticIntentRouter (optional)
          -> Business Rules
          -> SnowflakeClient (database.py)
      -> Optional STT/TTS services
  <- JSON response (intent, confidence, latency)
```

Core runtime components:

- In-memory conversation sessions keyed by `session_id`
- Verification gate for account-specific intents
- Deterministic business rule enforcement

## 12. Intent Routing Design

Routing uses layered decisioning:

1. Semantic intent detection (optional, threshold-controlled).
2. Regex pattern scoring fallback.
3. Guardrails:
   - block personal order lookup without personal order hints
   - block purchase-date intent without personal date signals
   - block unrelated out-of-domain queries

This prevents wrong answers for general questions like sports/news queries.

## 13. Business Rules

- Return window: 30 days from order date
- Warranty coverage: 12 months from order date
- New order creation limit: below $10,000
- AppleCare policy (device specific):
  - iPhone: replacement coverage available when active
  - iPad: minimum AppleCare charge $49
  - MacBook: minimum AppleCare charge $99

## 14. Troubleshooting

1. App starts but old UI behavior persists:
   - Hard refresh browser (`Ctrl+F5`) to reload static JS/CSS.
2. Query responses seem unchanged after code edit:
   - Restart `python main.py` (app currently runs without reload mode).
3. Voice upload transcription fails:
   - Install FFmpeg and ensure `ffmpeg -version` works in a new terminal.
4. Snowflake login/connection failure:
   - Recheck `SNOWFLAKE_ACCOUNT`, user, role, warehouse, db/schema values.
5. Semantic model load slow/fails:
   - Verify `sentence-transformers` and `faiss-cpu` are installed.
   - Set `USE_SEMANTIC_INTENT_ROUTER=false` to run regex-only mode.

FFmpeg install (Windows):

```powershell
winget install --id Gyan.FFmpeg -e
ffmpeg -version
```

## 15. Security Notes

- Do not commit `.env` with real credentials.
- Rotate credentials immediately if they were ever shared or pushed.
- Use least-privilege Snowflake role permissions.
- Consider replacing in-memory sessions with persistent/session-store for production.
