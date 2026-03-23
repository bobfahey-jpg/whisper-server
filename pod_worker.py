#!/usr/bin/env python3
"""
pod_worker.py — Autonomous, multi-threaded sermon transcription worker.

Runs inside the Docker container on RunPod, Windows, or any machine with a GPU.
Pulls work from Azure SQL, downloads MP3 from UCG S3, transcribes with Whisper,
writes transcript to Azure Blob Storage, updates SQL status.

A WorkerManager spawns NUM_WORKERS threads and monitors them — auto-restarting
any thread that dies. No manual process management needed.

Exits cleanly when:
  - No more sermons to transcribe (all threads idle)
  - MAX_RUNTIME_HOURS has elapsed (0 = run until done)
  - SIGTERM received (graceful drain)

Configuration (env vars):
  AZURE_SQL_SERVER            sermon-db-fb-1.database.windows.net
  AZURE_SQL_DB                sermons
  AZURE_SQL_USER              sermonadmin
  AZURE_SQL_PASSWORD          ...
  AZURE_STORAGE_CONNECTION_STRING  DefaultEndpointsProtocol=https;...
  NUM_WORKERS                 3  (default — tune for GPU VRAM)
  MAX_RUNTIME_HOURS           2  (0 = unlimited, e.g. for Windows home machines)
  WORKER_ID                   optional label for logging

Usage:
  python3 pod_worker.py
  docker run --gpus all -e AZURE_SQL_SERVER=... -e AZURE_SQL_PASSWORD=... \\
             -e AZURE_STORAGE_CONNECTION_STRING=... \\
             bobfahey6709/whisper-server:latest
"""

import logging
import os
import signal
import subprocess
import sys
import tempfile
import threading
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

SQL_SERVER     = os.environ["AZURE_SQL_SERVER"]
SQL_DB         = os.environ.get("AZURE_SQL_DB", "sermons")
SQL_USER       = os.environ["AZURE_SQL_USER"]
SQL_PASSWORD   = os.environ["AZURE_SQL_PASSWORD"]
BLOB_CONN      = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
BLOB_CONTAINER = os.environ.get("AZURE_BLOB_TRANSCRIPTS", "transcripts")

NUM_WORKERS   = int(os.environ.get("NUM_WORKERS", "3"))
MAX_HOURS     = float(os.environ.get("MAX_RUNTIME_HOURS", "2"))   # 0 = unlimited
WORKER_ID     = os.environ.get("WORKER_ID", "pod")
MODEL_NAME    = "large-v3-turbo"
APPINSIGHTS   = os.environ.get("APPINSIGHTS_CONNECTION_STRING", "")

DOWNLOAD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}

# ---------------------------------------------------------------------------
# Logging — each thread tags its own worker ID
# ---------------------------------------------------------------------------

class _DefaultTagFilter(logging.Filter):
    """Ensures %(worker_tag)s is always present — third-party loggers don't set it."""
    def filter(self, record):
        if not hasattr(record, "worker_tag"):
            record.worker_tag = "system"
        return True

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(worker_tag)s] %(message)s",
)
for _h in logging.root.handlers:
    _h.addFilter(_DefaultTagFilter())

# Silence verbose Azure SDK and HTTP client loggers
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

# Ship all logs to Azure Application Insights if connection string is set
if APPINSIGHTS:
    try:
        from opencensus.ext.azure.log_exporter import AzureLogHandler
        _ai_handler = AzureLogHandler(connection_string=APPINSIGHTS)
        _ai_handler.addFilter(_DefaultTagFilter())
        logging.root.addHandler(_ai_handler)
    except ImportError:
        pass  # opencensus not installed — skip silently


class TaggedLogger(logging.LoggerAdapter):
    def __init__(self, tag):
        super().__init__(logging.getLogger(__name__), {"worker_tag": tag})

def make_log(tag):
    return TaggedLogger(tag)

# ---------------------------------------------------------------------------
# Azure SQL helpers  (each thread gets its own connection)
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


def claim_sermon(conn, log):
    """
    Atomically claim the next available sermon.
    UPDLOCK + READPAST = safe for concurrent threads/pods.
    Returns (slug, mp3_url) or (None, None) if queue is empty.
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
    """Permanent failure — bad/deleted file. Never requeue."""
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

def upload_transcript(blob_svc, slug, text):
    """Upload transcript text, return blob URL."""
    blob_name = f"{slug}.txt"
    container = blob_svc.get_container_client(BLOB_CONTAINER)
    container.upload_blob(blob_name, text.encode("utf-8"), overwrite=True)
    account = blob_svc.account_name
    return f"https://{account}.blob.core.windows.net/{BLOB_CONTAINER}/{blob_name}"


# ---------------------------------------------------------------------------
# Download helper
# ---------------------------------------------------------------------------

def download_mp3(mp3_url, log):
    log.info(f"Downloading {mp3_url}")
    r = requests.get(mp3_url, headers=DOWNLOAD_HEADERS, timeout=120)
    r.raise_for_status()
    tmp = tempfile.NamedTemporaryFile(suffix=".mp3", delete=False)
    tmp.write(r.content)
    tmp.close()
    log.info(f"Downloaded {len(r.content)/1e6:.1f} MB → {tmp.name}")
    return tmp.name


# ---------------------------------------------------------------------------
# Single worker thread
# ---------------------------------------------------------------------------

def gpu_monitor(stop_event):
    """Sample GPU stats every 30s, log as structured event to App Insights."""
    log = make_log("gpu")
    while not stop_event.is_set():
        try:
            result = subprocess.run(
                ["nvidia-smi",
                 "--query-gpu=utilization.gpu,utilization.memory,memory.used,memory.free,temperature.gpu",
                 "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0:
                parts = [p.strip() for p in result.stdout.strip().split(",")]
                gpu_pct, mem_pct, mem_used, mem_free, temp = (int(p) for p in parts)
                log.info(
                    f"GPU {gpu_pct}%  VRAM {mem_used}MB/{mem_used+mem_free}MB  Temp {temp}C",
                    extra={"custom_dimensions": {
                        "event_type":    "gpu_metrics",
                        "pod_id":        WORKER_ID,
                        "gpu_util_pct":  gpu_pct,
                        "mem_util_pct":  mem_pct,
                        "mem_used_mb":   mem_used,
                        "mem_free_mb":   mem_free,
                        "gpu_temp_c":    temp,
                    }}
                )
        except Exception:
            pass
        stop_event.wait(30)


def worker_thread(thread_num, stop_event, idle_event, t_start, max_secs):
    """One worker thread: claim → download → transcribe → upload → repeat.
    Each thread loads its own WhisperModel for independent GPU context.
    """
    tag = f"{WORKER_ID}-t{thread_num}"
    log = make_log(tag)

    log.info("Thread starting — loading model...")
    model = WhisperModel(MODEL_NAME, device="cuda", compute_type="float16")
    log.info("Model ready.")

    blob_svc = BlobServiceClient.from_connection_string(BLOB_CONN)
    conn = get_sql_conn()
    log.info("SQL + Blob connected.")

    processed = 0
    consecutive_failures = 0

    while not stop_event.is_set():
        # Runtime limit check
        if max_secs and (time.time() - t_start) >= max_secs:
            log.info(f"Runtime limit reached — thread exiting after {processed} sermons.")
            break

        slug, mp3_url = claim_sermon(conn, log)
        if not slug:
            log.info("Queue empty — thread exiting.")
            break

        idle_event.clear()
        log.info(f"START {slug[:60]}")
        t0 = time.time()
        tmp_path = None

        try:
            tmp_path = download_mp3(mp3_url, log)

            segments, info = model.transcribe(tmp_path, language="en")
            text = " ".join(s.text.strip() for s in segments)
            elapsed_t = time.time() - t0
            log.info(f"TRANSCRIBED {slug[:50]}  audio={info.duration:.0f}s  t={elapsed_t:.0f}s  chars={len(text)}")

            transcript_url = upload_transcript(blob_svc, slug, text)
            mark_transcribed(conn, slug, transcript_url)

            processed += 1
            consecutive_failures = 0
            log.info(f"DONE {slug[:50]}  ({processed} total this thread)")

        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if e.response is not None else 0
            if code in (403, 404):
                log.warning(f"PERMANENT {code} — marking not_found: {slug}")
                try:
                    mark_not_found(conn, slug)
                except Exception:
                    pass
                # Not a failure — keep going immediately
            else:
                log.error(f"HTTP {code} FAILED {slug}: {e}")
                try:
                    mark_failed(conn, slug)
                except Exception:
                    pass
                consecutive_failures += 1
                if consecutive_failures >= 5:
                    log.error("5 consecutive failures — thread exiting.")
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
                log.error("5 consecutive failures — thread exiting.")
                break
            time.sleep(15)

        finally:
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass

    conn.close()
    log.info(f"Thread done. Processed {processed} sermons.")


# ---------------------------------------------------------------------------
# WorkerManager — starts, monitors, restarts worker threads
# ---------------------------------------------------------------------------

class WorkerManager:
    def __init__(self, num_workers, stop_event, t_start, max_secs):
        self.num_workers = num_workers
        self.stop_event  = stop_event
        self.t_start     = t_start
        self.max_secs    = max_secs
        self.threads     = {}   # thread_num → Thread
        self.idle_event  = threading.Event()

    def _spawn(self, n):
        t = threading.Thread(
            target=worker_thread,
            args=(n, self.stop_event, self.idle_event, self.t_start, self.max_secs),
            name=f"worker-{n}",
            daemon=True,
        )
        t.start()
        self.threads[n] = t
        return t

    def start(self):
        log = make_log("manager")
        log.info(f"Starting {self.num_workers} worker threads (staggered 20s apart).")
        for n in range(self.num_workers):
            self._spawn(n)
            if n < self.num_workers - 1:
                time.sleep(20)

    def monitor(self):
        """
        Main monitoring loop — runs on the main thread.
        Auto-restarts dead workers. Exits when all threads finish naturally.
        """
        log = make_log("manager")
        while not self.stop_event.is_set():
            time.sleep(10)

            all_done = True
            for n in range(self.num_workers):
                t = self.threads.get(n)
                if t and t.is_alive():
                    all_done = False
                elif not self.stop_event.is_set():
                    # Thread exited — check if it was a natural end or a crash
                    # Restart only if stop not requested
                    log.info(f"Thread {n} exited — checking for more work.")
                    new_t = self._spawn(n)
                    if new_t.is_alive():
                        all_done = False

            if all_done:
                log.info("All worker threads completed — manager exiting.")
                break

        elapsed = time.time() - self.t_start
        log.info(f"Total runtime: {elapsed/3600:.2f}h")

    def stop(self):
        make_log("manager").info("SIGTERM received — signalling threads to stop.")
        self.stop_event.set()
        for t in self.threads.values():
            t.join(timeout=60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    log = make_log("main")

    t_start  = time.time()
    max_secs = MAX_HOURS * 3600 if MAX_HOURS > 0 else None

    log.info(f"pod_worker starting — {NUM_WORKERS} threads (each loads own model), worker_id={WORKER_ID}")
    log.info(f"Max runtime: {f'{MAX_HOURS:.0f}h' if max_secs else 'unlimited'}")

    stop_event = threading.Event()

    # GPU metrics monitor — runs every 30s, ships to App Insights
    threading.Thread(target=gpu_monitor, args=(stop_event,), daemon=True, name="gpu-monitor").start()

    manager = WorkerManager(NUM_WORKERS, stop_event, t_start, max_secs)

    # Graceful shutdown on SIGTERM
    def handle_sigterm(sig, frame):
        manager.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)

    manager.start()
    manager.monitor()   # blocks until all threads done

    log.info(f"All done. Total elapsed: {(time.time()-t_start)/3600:.2f}h")


if __name__ == "__main__":
    main()
