#!/usr/bin/env python3
"""
pod_worker.py — Autonomous sermon transcription + enrichment worker.

Runs inside the Docker container on RunPod, Windows, or any machine with a GPU.
Pulls work from Azure SQL, downloads MP3 from UCG S3, transcribes with Whisper,
then runs the full per-sermon enrichment pipeline on a CPU background thread:
  Grok processing → sermon_nlp → sermon_scripture → sermon_occasion → sermon_topic_classifier

Speaker-level rollups (speaker_profile, speaker_eval_claude, speaker_eval_docx) are
triggered by the Mac orchestrator (pipeline_grok.py) when a speaker's queue drains.

Each pod runs as NUM_WORKERS independent processes (via start.sh), each with its own
GPU context — avoids CUDA shared-memory issues with multi-threaded models.

Status flow per sermon:
  metadata_scraped → queued → transcribed → enriching → processed

Exits cleanly when:
  - No more sermons to transcribe (queue empty)
  - MAX_RUNTIME_HOURS has elapsed (0 = run until done)
  - SIGTERM received (graceful drain)

Configuration (env vars):
  AZURE_SQL_SERVER            sermon-db-fb-1.database.windows.net
  AZURE_SQL_DB                sermons
  AZURE_SQL_USER              sermonadmin
  AZURE_SQL_PASSWORD          ...
  AZURE_STORAGE_CONNECTION_STRING  DefaultEndpointsProtocol=https;...
  OPENAI_API_KEY              xAI API key (used for Grok processing)
  GROK_MODEL                  grok-4-1-fast-non-reasoning (default)
  NUM_WORKERS                 1  (per-process; start.sh launches multiple processes)
  NUM_CPU_WORKERS             2  (enrichment threads per process)
  MAX_RUNTIME_HOURS           2  (0 = unlimited)
  WORKER_ID                   optional label for logging

Usage:
  python3 pod_worker.py
  docker run --gpus all -e AZURE_SQL_SERVER=... bobfahey6709/whisper-server:latest
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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import pyodbc
import requests
from azure.storage.blob import BlobServiceClient
from faster_whisper import WhisperModel

# ── Enrichment scripts path (mirrors local tools/sermons/ layout) ──────────────
_SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_SCRIPT_DIR))

try:
    from sermon_processor import process_transcript
    _HAS_ENRICHMENT = True
except ImportError:
    _HAS_ENRICHMENT = False

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SQL_SERVER     = os.environ["AZURE_SQL_SERVER"]
SQL_DB         = os.environ.get("AZURE_SQL_DB", "sermons")
SQL_USER       = os.environ["AZURE_SQL_USER"]
SQL_PASSWORD   = os.environ["AZURE_SQL_PASSWORD"]
BLOB_CONN      = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
BLOB_CONTAINER   = os.environ.get("AZURE_BLOB_TRANSCRIPTS", "transcripts")
BLOB_PROCESSED   = os.environ.get("AZURE_BLOB_PROCESSED",   "processed")

NUM_WORKERS     = int(os.environ.get("NUM_WORKERS", "1"))
NUM_CPU_WORKERS = int(os.environ.get("NUM_CPU_WORKERS", "2"))
MAX_HOURS       = float(os.environ.get("MAX_RUNTIME_HOURS", "2"))   # 0 = unlimited
MODEL_NAME      = "large-v3-turbo"
APPINSIGHTS     = os.environ.get("APPINSIGHTS_CONNECTION_STRING", "")

# Pod identity — prefer explicit WORKER_ID, fall back to RunPod-injected pod ID, then hostname
import socket as _socket
RUNPOD_POD_ID = os.environ.get("RUNPOD_POD_ID", "")
HOSTNAME      = _socket.gethostname()
WORKER_ID     = os.environ.get("WORKER_ID") or RUNPOD_POD_ID or HOSTNAME

DOWNLOAD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

class _DefaultTagFilter(logging.Filter):
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

logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

if APPINSIGHTS:
    try:
        from opencensus.ext.azure.log_exporter import AzureLogHandler
        _ai_handler = AzureLogHandler(connection_string=APPINSIGHTS)
        _ai_handler.addFilter(_DefaultTagFilter())
        logging.root.addHandler(_ai_handler)
    except ImportError:
        pass


class TaggedLogger(logging.LoggerAdapter):
    def __init__(self, tag):
        super().__init__(logging.getLogger(__name__), {"worker_tag": tag})

    def process(self, msg, kwargs):
        caller_extra = kwargs.get("extra", {})
        merged = {**self.extra, **caller_extra}
        if "custom_dimensions" in merged:
            merged["custom_dimensions"].setdefault("worker_tag", self.extra.get("worker_tag", ""))
        kwargs["extra"] = merged
        return msg, kwargs

def make_log(tag):
    return TaggedLogger(tag)

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


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _update_status(slug: str, status: str):
    conn = get_sql_conn()
    try:
        conn.execute("UPDATE sermons SET status=?, updated_at=? WHERE slug=?",
                     (status, now_utc(), slug))
        conn.commit()
    finally:
        conn.close()


def claim_sermon(conn, log):
    """
    Atomically claim the next available sermon. Returns (slug, mp3_url, metadata) or
    (None, None, None) if queue is empty.
    """
    cur = conn.cursor()
    try:
        cur.execute("BEGIN TRANSACTION")
        cur.execute("""
            SELECT TOP 1 slug, mp3_url, title, speaker, congregation, date, duration, page_url
            FROM sermons WITH (UPDLOCK, READPAST)
            WHERE status = 'metadata_scraped'
              AND mp3_url LIKE '%.mp3%'
            ORDER BY priority DESC, date DESC
        """)
        row = cur.fetchone()
        if not row:
            cur.execute("ROLLBACK")
            return None, None, None
        slug, mp3_url = row[0], row[1]
        metadata = {
            "title":        row[2] or slug,
            "speaker":      row[3] or "",
            "congregation": row[4] or "",
            "date":         str(row[5]) if row[5] else "",
            "duration":     str(row[6]) if row[6] else "",
            "page_url":     row[7] or "",
        }
        cur.execute(
            "UPDATE sermons SET status='queued', updated_at=? WHERE slug=? AND status='metadata_scraped'",
            (now_utc(), slug)
        )
        cur.execute("COMMIT")
        conn.commit()
        return slug, mp3_url, metadata
    except Exception as e:
        try:
            cur.execute("ROLLBACK")
        except Exception:
            pass
        log.error(f"claim_sermon failed: {e}")
        return None, None, None


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
    conn.cursor().execute(
        "UPDATE sermons SET status='not_found', updated_at=? WHERE slug=?",
        (now_utc(), slug)
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Blob Storage helpers
# ---------------------------------------------------------------------------

def upload_transcript(blob_svc, slug, text):
    blob_name = f"{slug}.txt"
    container = blob_svc.get_container_client(BLOB_CONTAINER)
    container.upload_blob(blob_name, text.encode("utf-8"), overwrite=True)
    account = blob_svc.account_name
    return f"https://{account}.blob.core.windows.net/{BLOB_CONTAINER}/{blob_name}"


def upload_processed(blob_svc, slug, md_content):
    container = blob_svc.get_container_client(BLOB_PROCESSED)
    container.upload_blob(f"{slug}.md", md_content.encode("utf-8"), overwrite=True)


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
# Per-sermon enrichment pipeline (CPU — runs in background thread)
# ---------------------------------------------------------------------------

def enrich_sermon(slug: str, transcript_text: str, metadata: dict, blob_svc):
    """
    Full per-sermon enrichment pipeline. Called from CPU thread pool after transcription.
    Steps: claim enriching → Grok → upload .md → NLP → scripture → occasion → topic
    """
    log = make_log(f"enrich-{WORKER_ID}")

    if not _HAS_ENRICHMENT:
        log.warning(f"Enrichment skipped — sermon_processor not available: {slug}")
        return

    # Atomically claim for enrichment (guards against pipeline_grok.py race)
    conn = get_sql_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE sermons SET status='enriching', updated_at=? WHERE slug=? AND status='transcribed'",
            (now_utc(), slug)
        )
        if cur.rowcount == 0:
            log.info(f"Enrichment claim missed (already claimed): {slug}")
            conn.close()
            return
        conn.commit()
    except Exception as e:
        log.error(f"Enrichment claim failed for {slug}: {e}")
        conn.close()
        return
    finally:
        conn.close()

    # Step 1: Grok — transcript text → .md content
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", encoding="utf-8", delete=False) as f:
            f.write(transcript_text)
            tmp_path = f.name

        md_content = process_transcript(tmp_path, slug, metadata=metadata)
        if not md_content:
            raise Exception("Grok returned no content")

        upload_processed(blob_svc, slug, md_content)
        log.info(f"Grok done + .md uploaded: {slug}")

    except Exception as e:
        log.error(f"Grok failed for {slug}: {e}")
        _update_status(slug, "transcribed")   # revert — pipeline_grok.py fallback
        return
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

    # Mark processed before running enrichment scripts (topic classifier needs .md in blob)
    _update_status(slug, "processed")

    # Steps 2–5: per-sermon enrichment scripts as subprocesses
    enrichment_cmds = [
        ["python3", str(_SCRIPT_DIR / "sermon_nlp.py"),              "--slug", slug],
        ["python3", str(_SCRIPT_DIR / "sermon_scripture.py"),         "--slug", slug],
        ["python3", str(_SCRIPT_DIR / "sermon_occasion.py"),          "--slug", slug],
        ["python3", str(_SCRIPT_DIR / "sermon_topic_classifier.py"),  "--slug", slug],
    ]

    for cmd in enrichment_cmds:
        script_name = Path(cmd[1]).stem
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=180,
                                    env=os.environ.copy())
            if result.returncode != 0:
                log.warning(f"{script_name} failed for {slug}: {result.stderr[-300:]}")
            else:
                log.info(f"{script_name} done: {slug}")
        except subprocess.TimeoutExpired:
            log.warning(f"{script_name} timed out for {slug}")
        except Exception as e:
            log.warning(f"{script_name} error for {slug}: {e}")

    log.info(f"Full enrichment complete: {slug}", extra={"custom_dimensions": {
        "event_type":    "enrichment_complete",
        "pod_id":        WORKER_ID,
        "runpod_pod_id": RUNPOD_POD_ID,
        "slug":          slug,
    }})


# ---------------------------------------------------------------------------
# GPU metrics monitor
# ---------------------------------------------------------------------------

def gpu_monitor(stop_event):
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
                    f"GPU {gpu_pct}%  VRAM {mem_used}MB/{mem_used+mem_free}MB  Temp {temp}C  pod={WORKER_ID}",
                    extra={"custom_dimensions": {
                        "event_type":      "gpu_metrics",
                        "pod_id":          WORKER_ID,
                        "runpod_pod_id":   RUNPOD_POD_ID,
                        "hostname":        HOSTNAME,
                        "gpu_util_pct":    gpu_pct,
                        "mem_util_pct":    mem_pct,
                        "mem_used_mb":     mem_used,
                        "mem_free_mb":     mem_free,
                        "gpu_temp_c":      temp,
                    }}
                )
        except Exception:
            pass
        stop_event.wait(30)


# ---------------------------------------------------------------------------
# Single worker thread (GPU transcription)
# ---------------------------------------------------------------------------

def worker_thread(thread_num, stop_event, idle_event, t_start, max_secs, cpu_pool):
    """
    One worker thread: claim → download → transcribe → upload → submit enrichment → repeat.
    Loads its own WhisperModel for independent GPU context.
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
        if max_secs and (time.time() - t_start) >= max_secs:
            log.info(f"Runtime limit reached — thread exiting after {processed} sermons.")
            break

        slug, mp3_url, metadata = claim_sermon(conn, log)
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
            log.info(
                f"TRANSCRIBED {slug[:50]}  audio={info.duration:.0f}s  t={elapsed_t:.0f}s  chars={len(text)}  pod={WORKER_ID}",
                extra={"custom_dimensions": {
                    "event_type":    "transcription_complete",
                    "pod_id":        WORKER_ID,
                    "runpod_pod_id": RUNPOD_POD_ID,
                    "hostname":      HOSTNAME,
                    "slug":          slug,
                    "audio_secs":    round(info.duration),
                    "elapsed_secs":  round(elapsed_t),
                    "char_count":    len(text),
                }}
            )

            transcript_url = upload_transcript(blob_svc, slug, text)
            mark_transcribed(conn, slug, transcript_url)

            # Submit full enrichment pipeline to CPU thread pool (non-blocking)
            if cpu_pool is not None and _HAS_ENRICHMENT:
                cpu_pool.submit(enrich_sermon, slug, text, metadata, blob_svc)
                log.info(f"Enrichment submitted: {slug[:50]}")

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
# WorkerManager
# ---------------------------------------------------------------------------

class WorkerManager:
    def __init__(self, num_workers, stop_event, t_start, max_secs, cpu_pool):
        self.num_workers = num_workers
        self.stop_event  = stop_event
        self.t_start     = t_start
        self.max_secs    = max_secs
        self.cpu_pool    = cpu_pool
        self.threads     = {}
        self.idle_event  = threading.Event()

    def _spawn(self, n):
        t = threading.Thread(
            target=worker_thread,
            args=(n, self.stop_event, self.idle_event, self.t_start, self.max_secs, self.cpu_pool),
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
        log = make_log("manager")
        while not self.stop_event.is_set():
            time.sleep(10)
            all_done = True
            for n in range(self.num_workers):
                t = self.threads.get(n)
                if t and t.is_alive():
                    all_done = False
                elif not self.stop_event.is_set():
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
# Self-termination
# ---------------------------------------------------------------------------

def self_terminate():
    if not RUNPOD_POD_ID:
        make_log("main").info("No RUNPOD_POD_ID set — skipping self-termination.")
        return
    api_key = os.environ.get("RUNPOD_API_KEY", "")
    if not api_key:
        make_log("main").warning("No RUNPOD_API_KEY — cannot self-terminate.")
        return
    try:
        log = make_log("main")
        log.info(f"Self-terminating pod {RUNPOD_POD_ID}...")
        r = requests.post(
            "https://api.runpod.io/graphql",
            json={"query": f'mutation {{ podTerminate(input: {{podId: "{RUNPOD_POD_ID}"}}) }}'},
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=15
        )
        if r.status_code == 200:
            log.info("Pod termination requested successfully.")
        else:
            log.warning(f"Termination request returned {r.status_code}")
    except Exception as e:
        make_log("main").error(f"Self-termination failed: {e}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    log = make_log("main")

    t_start  = time.time()
    max_secs = MAX_HOURS * 3600 if MAX_HOURS > 0 else None

    enrichment_status = "enabled" if _HAS_ENRICHMENT else "DISABLED (sermon_processor not found)"
    log.info(f"pod_worker starting — {NUM_WORKERS} threads, enrichment={enrichment_status}, worker_id={WORKER_ID}")
    log.info(f"Max runtime: {f'{MAX_HOURS:.0f}h' if max_secs else 'unlimited'}")

    stop_event = threading.Event()

    # CPU thread pool for enrichment (separate from GPU threads — no CUDA sharing)
    cpu_pool = ThreadPoolExecutor(
        max_workers=NUM_CPU_WORKERS,
        thread_name_prefix="enrich"
    ) if _HAS_ENRICHMENT else None

    threading.Thread(target=gpu_monitor, args=(stop_event,), daemon=True, name="gpu-monitor").start()

    manager = WorkerManager(NUM_WORKERS, stop_event, t_start, max_secs, cpu_pool)

    def handle_sigterm(sig, frame):
        manager.stop()
        if cpu_pool:
            cpu_pool.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)

    manager.start()
    manager.monitor()   # blocks until all GPU threads done

    # Wait for any in-flight enrichment to finish
    if cpu_pool:
        log.info("GPU work done — waiting for enrichment threads to finish...")
        cpu_pool.shutdown(wait=True)

    log.info(f"All done. Total elapsed: {(time.time()-t_start)/3600:.2f}h")
    self_terminate()


if __name__ == "__main__":
    main()
