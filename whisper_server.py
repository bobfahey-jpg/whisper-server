#!/usr/bin/env python3
"""
whisper_server.py — Remote Whisper transcription server (Windows or RunPod).

Two transcription modes:
  POST /transcribe      — upload an MP3 file (Windows/local network)
  POST /transcribe-url  — pass a URL, pod downloads directly (RunPod)

Optional API key auth: set WHISPER_API_KEY env var.
Leave unset for local network use. Always set for RunPod (public internet).

════════════════════════════════════════════════════════
WINDOWS SETUP (one-time, on each machine)
════════════════════════════════════════════════════════

1. Install Python 3.11 from python.org (check "Add to PATH")
2. Install CUDA Toolkit 12.x from nvidia.com/cuda-downloads
3. pip install faster-whisper fastapi uvicorn python-multipart requests
4. Copy this file to the Windows machine
5. Allow inbound TCP port 8765 in Windows Firewall
6. Run: python whisper_server.py
   Model (~1.5GB) downloads automatically on first run.
7. Find IP: ipconfig → IPv4 Address under WiFi adapter

════════════════════════════════════════════════════════
NIGHTLY USE
════════════════════════════════════════════════════════
Run: python whisper_server.py
Leave terminal open overnight. Close in the morning.
════════════════════════════════════════════════════════
"""

import logging
import os
import tempfile
import traceback

import requests as _requests
import uvicorn
from fastapi import FastAPI, HTTPException, Request, UploadFile
from faster_whisper import WhisperModel
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("whisper_server.log"),
    ],
)
log = logging.getLogger(__name__)

MODEL_NAME = "large-v3-turbo"
PORT       = int(os.environ.get("WHISPER_PORT", 8765))
API_KEY    = os.environ.get("WHISPER_API_KEY", "")

DOWNLOAD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}

print(f"Loading {MODEL_NAME} on CUDA...")
model = WhisperModel(MODEL_NAME, device="cuda", compute_type="float16")
print(f"Model ready. Listening on 0.0.0.0:{PORT}")
print(f"API key auth: {'ENABLED' if API_KEY else 'disabled (local mode)'}")

app = FastAPI()


class UrlRequest(BaseModel):
    mp3_url: str
    slug: str = ""


def _check_auth(request: Request):
    if not API_KEY:
        return
    if request.headers.get("Authorization", "") != f"Bearer {API_KEY}":
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


def _run_whisper(audio_path: str) -> dict:
    log.info(f"Transcribing {audio_path}")
    try:
        segments, info = model.transcribe(audio_path, language="en")
        text = " ".join(s.text.strip() for s in segments)
        log.info(f"Done: {round(info.duration, 1)}s audio, {len(text)} chars")
        return {"text": text, "duration_secs": round(info.duration, 1)}
    except Exception as e:
        log.error(f"Transcription error: {e}\n{traceback.format_exc()}")
        raise


@app.get("/health")
def health(request: Request):
    _check_auth(request)
    return {"status": "ok", "model": MODEL_NAME}


@app.post("/transcribe")
async def transcribe_upload(request: Request, file: UploadFile):
    """Accept MP3 upload from client (Windows local network mode)."""
    _check_auth(request)
    if not file.filename.lower().endswith(".mp3"):
        raise HTTPException(status_code=400, detail="MP3 files only")
    with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name
    try:
        return _run_whisper(tmp_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        os.unlink(tmp_path)


@app.post("/transcribe-url")
async def transcribe_url(request: Request, body: UrlRequest):
    """Download MP3 directly from URL and transcribe (RunPod mode)."""
    _check_auth(request)
    log.info(f"transcribe-url: {body.slug} {body.mp3_url}")
    try:
        r = _requests.get(body.mp3_url, headers=DOWNLOAD_HEADERS, timeout=120)
        r.raise_for_status()
        log.info(f"Downloaded {len(r.content)} bytes")
    except Exception as e:
        log.error(f"Download failed: {e}")
        raise HTTPException(status_code=400, detail=f"Failed to download MP3: {e}")
    with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as tmp:
        tmp.write(r.content)
        tmp_path = tmp.name
    try:
        return _run_whisper(tmp_path)
    except Exception as e:
        log.error(f"500 error: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        os.unlink(tmp_path)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
