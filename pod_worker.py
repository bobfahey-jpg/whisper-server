#!/usr/bin/env python3
"""
pod_worker.py — Autonomous, ephemeral sermon transcription worker.

Runs inside the Docker container on RunPod, Windows, or any machine with a GPU.
Pulls work from Azure SQL, downloads MP3 from UCG S3, transcribes with Whisper,
writes transcript to Azure Blob Storage, updates SQL status.

Exits cleanly when:
  - No more sermons to transcribe
  - MAX_RUNTIME_HOURS has elapsed (0 = run until done)

Configuration (env vars):
  AZURE_SQL_SERVER            sermon-db-fb-1.database.windows.net
  AZURE_SQL_DB                sermons
  AZURE_SQL_USER              sermonadmin
  AZURE_SQL_PASSWORD          ...
  AZURE_STORAGE_CONNECTION_STRING  DefaultEndpointsProtocol=https;...
  MAX_RUNTIME_HOURS           2  (0 = unlimited, e.g. for Windows home machines)
  WORKER_ID                   optional label for logging

Usage:
  python3 pod_worker.py
  docker run --gpus all -e AZURE_SQL_SERVER=... -e AZURE_SQL_PASSWORD=... \\
             -e AZURE_STORAGE_CONNECTION_STRING=... \\
             bobfahey6709/whisper-worker:latest
"""

import logging
import os
import sys
import tempfile
import time
import traceback
from datetime import datetime, timezone

import pyodbc
import requests
from azure.storage.blob import BlobServiceClient
from faster_whisper import WhisperModel

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SQL_SERVER   = os.environ["AZURE_SQL_SERVER"]
SQL_DB       = os.environ.get("AZURE_SQL_DB", "sermons")
SQL_USER     = os.environ["AZURE_SQL_USER"]
SQL_PASSWORD = os.environ["AZURE_SQL_PASSWORD"]
BLOB_CONN    = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
BLOB_CONTAINER = os.environ.get("AZURE_BLOB_TRANSCRIPTS", "transcripts")

MAX_HOURS    = float(os.environ.get("MAX_RUNTIME_HOURS", "2"))  # 0 = unlimited
WORKER_ID    = os.environ.get("WORKER_ID", "worker")
MODEL_NAME   = "large-v3-turbo"

DOWNLOAD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(worker)s] %(message)s",
)
log = logging.getLogger(__name__)

class WorkerFilter(logging.Filter):
    def filter(self, record):
        record.worker = WORKER_ID
        return True

log.addFilter(WorkerFilter())


def ts():
    return datetime.now().strftime("[%H:%M:%S]")


# ---------------------------------------------------------------------------
# Azure SQL helpers
# ---------------------------------------------------------------------------

def get_sql_conn():
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DB};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)


def claim_sermon(conn):
    """
    Atomically claim the next available sermon.
    Uses UPDLOCK + READPAST for safe concurrent claiming across multiple pods.
    Returns (slug, mp3_url) or (None, None) if no work available.
    """
    cur = conn.cursor()
    try:
        cur.execute("BEGIN TRANSACTION")
        cur.execute("""
            SELECT TOP 1 slug, mp3_url
            FROM sermons WITH (UPDLOCK, READPAST)
            WHERE status = 'metadata_scraped'
              AND mp3_url LIKE '%.mp3%'
            ORDER BY priority DESC, date DESC
        """)
        row = cur.fetchone()
        if not row:
            cur.execute("ROLLBACK")
            return None, None
        slug, mp3_url = row[0], row[1]
        cur.execute(
            "UPDATE sermons SET status='queued', updated_at=? WHERE slug=?",
            (now_utc(), slug)
        )
        cur.execute("COMMIT")
        conn.commit()
        return slug, mp3_url
    except Exception as e:
        try:
            cur.execute("ROLLBACK")
        except Exception:
            pass
        log.error(f"claim_sermon failed: {e}")
        return None, None


def mark_transcribed(conn, slug, transcript_url):
    conn.cursor().execute(
        "UPDATE sermons SET status='transcribed', transcript_url=?, updated_at=? WHERE slug=?",
        (transcript_url, now_utc(), slug)
    )
    conn.commit()


def mark_failed(conn, slug):
    conn.cursor().execute(
        "UPDATE sermons SET status='metadata_scraped', updated_at=? WHERE slug=?",
        (now_utc(), slug)
    )
    conn.commit()


def mark_not_found(conn, slug):
    """Permanent failure — bad/deleted file. Don't requeue."""
    conn.cursor().execute(
        "UPDATE sermons SET status='not_found', updated_at=? WHERE slug=?",
        (now_utc(), slug)
    )
    conn.commit()


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Blob Storage helpers
# ---------------------------------------------------------------------------

def upload_transcript(blob_client, slug, text):
    """Upload transcript text to Azure Blob, return public URL."""
    blob_name = f"{slug}.txt"
    container = blob_client.get_container_client(BLOB_CONTAINER)
    container.upload_blob(blob_name, text.encode("utf-8"), overwrite=True)
    account = blob_client.account_name
    return f"https://{account}.blob.core.windows.net/{BLOB_CONTAINER}/{blob_name}"


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def download_mp3(mp3_url):
    """Download MP3 to a temp file. Returns path or raises."""
    log.info(f"Downloading {mp3_url}")
    r = requests.get(mp3_url, headers=DOWNLOAD_HEADERS, timeout=120)
    r.raise_for_status()
    tmp = tempfile.NamedTemporaryFile(suffix=".mp3", delete=False)
    tmp.write(r.content)
    tmp.close()
    log.info(f"Downloaded {len(r.content)/1e6:.1f} MB → {tmp.name}")
    return tmp.name


# ---------------------------------------------------------------------------
# Main worker loop
# ---------------------------------------------------------------------------

def run():
    t_start = time.time()
    max_secs = MAX_HOURS * 3600 if MAX_HOURS > 0 else None

    print(f"{ts()} pod_worker starting — worker_id={WORKER_ID}")
    print(f"{ts()} Max runtime: {f'{MAX_HOURS:.0f} hours' if max_secs else 'unlimited'}")

    # Load Whisper model (pre-cached in Docker image — fast)
    print(f"{ts()} Loading {MODEL_NAME}...")
    t_model = time.time()
    model = WhisperModel(MODEL_NAME, device="cuda", compute_type="float16")
    print(f"{ts()} Model ready in {time.time()-t_model:.1f}s")

    # Connect to Azure
    print(f"{ts()} Connecting to Azure SQL...")
    conn = get_sql_conn()
    print(f"{ts()} Connected.")

    print(f"{ts()} Connecting to Azure Blob Storage...")
    blob_client = BlobServiceClient.from_connection_string(BLOB_CONN)
    print(f"{ts()} Connected.")

    processed = 0
    consecutive_failures = 0

    while True:
        # Check runtime limit
        elapsed = time.time() - t_start
        if max_secs and elapsed >= max_secs:
            print(f"{ts()} {MAX_HOURS:.0f}-hour limit reached — exiting cleanly after {processed} sermons.")
            break

        # Claim next sermon
        slug, mp3_url = claim_sermon(conn)
        if not slug:
            if processed == 0:
                print(f"{ts()} No work available — exiting.")
            else:
                print(f"{ts()} Queue empty — exiting after {processed} sermons.")
            break

        print(f"{ts()} START {slug[:60]}")
        t0 = time.time()
        tmp_path = None

        try:
            # Download
            tmp_path = download_mp3(mp3_url)

            # Transcribe
            segments, info = model.transcribe(tmp_path, language="en")
            text = " ".join(s.text.strip() for s in segments)
            elapsed_transcribe = time.time() - t0
            print(f"{ts()} TRANSCRIBED {slug[:50]}  audio={info.duration:.0f}s  transcribe={elapsed_transcribe:.0f}s  chars={len(text)}")

            # Upload to Blob
            transcript_url = upload_transcript(blob_client, slug, text)

            # Update SQL
            mark_transcribed(conn, slug, transcript_url)

            processed += 1
            consecutive_failures = 0
            total_elapsed = time.time() - t_start
            print(f"{ts()} DONE  {slug[:50]}  ({processed} total, {total_elapsed/3600:.1f}h elapsed)")

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else 0
            if status_code in (403, 404):
                log.warning(f"PERMANENT {status_code} for {slug} — marking not_found")
                try:
                    mark_not_found(conn, slug)
                except Exception:
                    pass
                # Don't count toward consecutive_failures — move on immediately
            else:
                log.error(f"FAILED {slug}: {e}\n{traceback.format_exc()}")
                try:
                    mark_failed(conn, slug)
                except Exception:
                    pass
                consecutive_failures += 1
                if consecutive_failures >= 5:
                    print(f"{ts()} 5 consecutive failures — exiting to avoid spinning.")
                    break
                time.sleep(15)

        except Exception as e:
            log.error(f"FAILED {slug}: {e}\n{traceback.format_exc()}")
            try:
                mark_failed(conn, slug)
            except Exception:
                pass
            consecutive_failures += 1
            if consecutive_failures >= 5:
                print(f"{ts()} 5 consecutive failures — exiting to avoid spinning.")
                break
            time.sleep(15)

        finally:
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass

    conn.close()
    print(f"{ts()} pod_worker done. Processed {processed} sermons in {(time.time()-t_start)/3600:.1f}h.")


if __name__ == "__main__":
    run()
