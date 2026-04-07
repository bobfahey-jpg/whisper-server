#!/usr/bin/env python3
"""
sermon_nlp.py — Per-sermon NLP metrics engine.

Downloads TXT transcripts from Azure Blob (caches locally as temp), computes:
  - Volume & rate (word count, WPM, sentence stats)
  - Readability (6 textstat scores) — uses MD cleaned transcript when available
  - Vocabulary richness (TTR, hapax legomena)
  - Filler words (normalized per 1000 words)
  - Pronoun orientation (I / WE / YOU / GOD)
  - Sentiment arc (VADER, 200-word windows → Lowry Loop detection)

Writes results to: Azure SQL sermon_metrics table
Backup parquet:    Azure Blob evaluations/sermon_metrics.parquet
Idempotent — skips already-computed slugs (in SQL) unless --force.

Usage:
  python3 tools/sermons/sermon_nlp.py --speaker "Robert Fahey"
  python3 tools/sermons/sermon_nlp.py --speaker "Nathan Ekama" --force
  python3 tools/sermons/sermon_nlp.py --all          # entire available corpus
  python3 tools/sermons/sermon_nlp.py --slug go-forward-10
"""

import argparse
import io
import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyodbc
import textstat
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

ROOT           = Path(__file__).resolve().parents[2]
TRANSCRIPT_DIR = ROOT / "data" / "sermons" / "transcripts"   # temp download cache
PROCESSED_DIR  = ROOT / "data" / "sermons" / "processed"     # temp download cache
CONFIG_DIR     = ROOT / "tools" / "sermons" / "eval_config"

TRANSCRIPT_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

sys.path.insert(0, str(ROOT / "tools" / "sermons"))
load_dotenv(ROOT / ".env")

# ── Load reference config ──────────────────────────────────────────────────────
with open(CONFIG_DIR / "filler_words.json") as f:
    _FILLER_CONFIG = json.load(f)

with open(CONFIG_DIR / "pronouns.json") as f:
    _PRONOUN_CONFIG = json.load(f)

VADER = SentimentIntensityAnalyzer()

BLOB_TRANSCRIPTS = os.environ.get("AZURE_BLOB_TRANSCRIPTS", "transcripts")
BLOB_PROCESSED   = os.environ.get("AZURE_BLOB_PROCESSED",   "processed")
BLOB_EVALUATIONS = os.environ.get("AZURE_BLOB_EVALUATIONS", "evaluations")

# Regex to extract ## Cleaned Transcript section from MD file
_CLEANED_RE = re.compile(
    r"##\s+Cleaned Transcript\s*\n(.*?)(?=\n##\s|\Z)", re.DOTALL | re.IGNORECASE
)

# Columns that map to Azure SQL sermon_metrics table (must match DDL exactly)
_SQL_COLS = [
    "slug", "speaker", "date", "word_count", "unique_words",
    "sentence_count", "avg_sentence_len", "readability_source",
    "duration_minutes", "estimated_wpm", "wpm_class",
    "flesch_reading_ease", "flesch_kincaid_grade", "gunning_fog",
    "smog_index", "ari", "coleman_liau", "avg_readability_grade",
    "type_token_ratio", "ttr_200_words", "hapax_legomena_count",
    "hapax_legomena_rate", "question_count", "question_rate",
    "filler_total", "filler_class",
    "pronoun_i_rate", "pronoun_we_rate", "pronoun_you_rate",
    "pronoun_god_rate", "pronoun_dominant",
    "sentiment_overall", "sentiment_positive_pct", "sentiment_negative_pct",
    "sentiment_dip_depth", "sentiment_peak", "lowry_loop_detected",
]


# ── Azure helpers ──────────────────────────────────────────────────────────────

def get_blob_svc() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(
        os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    )


def get_transcript(slug: str, blob_svc: BlobServiceClient) -> str | None:
    """Check local temp cache first; download TXT from Blob if missing."""
    local = TRANSCRIPT_DIR / f"{slug}.txt"
    if local.exists():
        return local.read_text(encoding="utf-8")
    try:
        data = blob_svc.get_container_client(BLOB_TRANSCRIPTS) \
                       .download_blob(f"{slug}.txt").readall()
        text = data.decode("utf-8")
        local.write_text(text, encoding="utf-8")
        return text
    except Exception:
        return None


def get_cleaned_transcript(slug: str, blob_svc: BlobServiceClient) -> str | None:
    """
    Fetch Grok-processed MD and extract ## Cleaned Transcript section.
    Returns None if MD not available — caller falls back to raw TXT.
    """
    local = PROCESSED_DIR / f"{slug}.md"
    if not local.exists():
        try:
            data = blob_svc.get_container_client(BLOB_PROCESSED) \
                           .download_blob(f"{slug}.md").readall()
            local.write_text(data.decode("utf-8"), encoding="utf-8")
        except Exception:
            return None
    md_text = local.read_text(encoding="utf-8")
    m = _CLEANED_RE.search(md_text)
    return m.group(1).strip() if m else None


# ── SQL helpers ────────────────────────────────────────────────────────────────

def get_sql_conn():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.environ['AZURE_SQL_SERVER']};"
        f"DATABASE={os.environ.get('AZURE_SQL_DB', 'sermons')};"
        f"UID={os.environ['AZURE_SQL_USER']};"
        f"PWD={os.environ['AZURE_SQL_PASSWORD']};"
        f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )


def fetch_sermons(speaker: str | None, slug: str | None, all_sermons: bool) -> list[dict]:
    conn = get_sql_conn()
    try:
        cur = conn.cursor()
        if slug:
            cur.execute(
                "SELECT slug, title, speaker, date, duration FROM sermons WHERE slug=?", (slug,)
            )
        elif speaker:
            cur.execute("""
                SELECT slug, title, speaker, date, duration FROM sermons
                WHERE speaker=? AND status IN ('transcribed','processed')
                ORDER BY date DESC
            """, (speaker,))
        else:
            cur.execute("""
                SELECT slug, title, speaker, date, duration FROM sermons
                WHERE status IN ('transcribed','processed')
                ORDER BY date DESC
            """)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def get_done_slugs_sql(speaker: str | None = None) -> set:
    """Return set of slugs already computed in Azure SQL sermon_metrics."""
    conn = get_sql_conn()
    try:
        cur = conn.cursor()
        if speaker:
            cur.execute("SELECT slug FROM sermon_metrics WHERE speaker=?", (speaker,))
        else:
            cur.execute("SELECT slug FROM sermon_metrics")
        return {r[0] for r in cur.fetchall()}
    finally:
        conn.close()


def write_metrics_sql(results: list[dict], speaker: str | None):
    """Delete existing rows by slug, bulk insert new metrics.

    Always deletes by slug (not by speaker) so that PK conflicts cannot occur
    even when a slug was previously attributed to a different speaker.
    """
    if not results:
        return
    conn = get_sql_conn()
    try:
        cur = conn.cursor()
        slugs = [r["slug"] for r in results]
        for i in range(0, len(slugs), 500):
            batch = slugs[i:i+500]
            placeholders = ",".join("?" * len(batch))
            cur.execute(f"DELETE FROM sermon_metrics WHERE slug IN ({placeholders})", batch)

        insert_sql = (
            f"INSERT INTO sermon_metrics ({', '.join(_SQL_COLS)}) "
            f"VALUES ({', '.join('?' * len(_SQL_COLS))})"
        )
        params = []
        for r in results:
            row = []
            for c in _SQL_COLS:
                v = r.get(c)
                if isinstance(v, bool):
                    v = int(v)
                row.append(v)
            params.append(row)
        cur.fast_executemany = True
        cur.executemany(insert_sql, params)
        conn.commit()
    finally:
        conn.close()


def upload_parquet_backup(df: pd.DataFrame):
    """Upload snapshot to Azure Blob evaluations/sermon_metrics.parquet"""
    try:
        # Fetch full dataset from SQL for complete backup
        conn = get_sql_conn()
        try:
            full_df = pd.read_sql("SELECT * FROM sermon_metrics", conn)
        finally:
            conn.close()
        buf = io.BytesIO()
        full_df.to_parquet(buf, index=False)
        buf.seek(0)
        svc = get_blob_svc()
        svc.get_container_client(BLOB_EVALUATIONS).upload_blob(
            "sermon_metrics.parquet", buf, overwrite=True
        )
    except Exception as e:
        print(f"  ⚠  Blob backup failed: {e}")


# ── NLP computation ────────────────────────────────────────────────────────────

def parse_duration_minutes(dur: str) -> float | None:
    if not dur:
        return None
    try:
        parts = [int(p) for p in str(dur).split(":")]
        if len(parts) == 3:
            return parts[0] * 60 + parts[1] + parts[2] / 60
        if len(parts) == 2:
            return parts[0] + parts[1] / 60
    except (ValueError, AttributeError):
        pass
    return None


def count_fillers(text_lower: str, word_count: int) -> dict:
    results = {}
    for filler in _FILLER_CONFIG["fillers"]:
        count = len(re.findall(r'\b' + re.escape(filler) + r'\b', text_lower))
        key = "filler_" + re.sub(r'\W+', '_', filler).strip('_')
        results[key] = round(count / word_count * 1000, 2) if word_count > 0 else 0
    results["filler_total"] = round(sum(results.values()), 2)
    total = results["filler_total"]
    results["filler_class"] = "low" if total < 2 else ("moderate" if total < 5 else "high")
    return results


def count_pronouns(tokens: list[str], word_count: int) -> dict:
    i_count   = sum(1 for t in tokens if t in _PRONOUN_CONFIG["i_words"])
    we_count  = sum(1 for t in tokens if t in _PRONOUN_CONFIG["we_words"])
    you_count = sum(1 for t in tokens if t in _PRONOUN_CONFIG["you_words"])
    god_count = sum(1 for t in tokens if t in _PRONOUN_CONFIG["god_words"])

    scale = 1000 / word_count if word_count > 0 else 0
    i_r   = round(i_count   * scale, 2)
    we_r  = round(we_count  * scale, 2)
    you_r = round(you_count * scale, 2)
    god_r = round(god_count * scale, 2)

    dominant = max(
        [("I", i_r), ("WE", we_r), ("YOU", you_r), ("GOD", god_r)],
        key=lambda x: x[1]
    )[0]

    return {
        "pronoun_i_rate":   i_r,
        "pronoun_we_rate":  we_r,
        "pronoun_you_rate": you_r,
        "pronoun_god_rate": god_r,
        "pronoun_dominant": dominant,
    }


def sentiment_arc(text: str, window_words: int = 200) -> dict:
    words = text.split()
    if len(words) < window_words:
        score = VADER.polarity_scores(text)["compound"]
        return {
            "sentiment_overall":       round(score, 3),
            "sentiment_positive_pct":  100.0 if score > 0.05 else 0.0,
            "sentiment_negative_pct":  100.0 if score < -0.05 else 0.0,
            "sentiment_arc":           [round(score, 3)],
            "sentiment_dip_depth":     round(score, 3),
            "sentiment_peak":          round(score, 3),
            "lowry_loop_detected":     False,
        }

    windows = []
    for i in range(0, len(words), window_words):
        chunk = " ".join(words[i:i + window_words])
        windows.append(VADER.polarity_scores(chunk)["compound"])

    overall   = VADER.polarity_scores(text)["compound"]
    pos_pct   = round(sum(1 for w in windows if w > 0.05)  / len(windows) * 100, 1)
    neg_pct   = round(sum(1 for w in windows if w < -0.05) / len(windows) * 100, 1)
    dip_depth = min(windows)
    peak      = max(windows)

    lowry = False
    if len(windows) >= 3:
        mid = len(windows) // 2
        lowry = min(windows[:mid]) < -0.10 and max(windows[mid:]) > 0.20

    return {
        "sentiment_overall":      round(overall, 3),
        "sentiment_positive_pct": pos_pct,
        "sentiment_negative_pct": neg_pct,
        "sentiment_arc":          [round(w, 3) for w in windows],  # not stored in SQL
        "sentiment_dip_depth":    round(dip_depth, 3),
        "sentiment_peak":         round(peak, 3),
        "lowry_loop_detected":    lowry,
    }


def compute_metrics(sermon: dict, txt_text: str, clean_text: str | None = None) -> dict:
    """
    txt_text   — raw Whisper TXT: word count, WPM, fillers, pronouns, sentiment, vocabulary
    clean_text — Grok MD cleaned transcript: sentence length + readability (has punctuation)
                 Falls back to txt_text if None or too short.
    """
    slug    = sermon["slug"]
    dur_min = parse_duration_minutes(sermon.get("duration"))

    raw    = txt_text.strip()
    tokens = re.findall(r"\b\w+\b", raw.lower())
    word_count = len(tokens)

    if word_count < 50:
        return {"slug": slug, "error": "transcript too short", "word_count": word_count}

    unique_words = len(set(tokens))

    from collections import Counter
    freq        = Counter(tokens)
    hapax_count = sum(1 for w, c in freq.items() if c == 1)
    hapax_rate  = round(hapax_count / unique_words, 3) if unique_words > 0 else 0

    first_200 = tokens[:200]
    ttr_200   = round(len(set(first_200)) / len(first_200), 3) if first_200 else 0
    ttr_full  = round(unique_words / word_count, 3)

    wpm = round(word_count / dur_min, 0) if dur_min and dur_min > 0 else None
    wpm_class = None
    if wpm:
        wpm_class = "slow" if wpm < 125 else ("ideal" if wpm <= 175 else "fast")

    question_count = raw.count("?")
    question_rate  = round(question_count / word_count * 1000, 2)

    # Sentence + readability: use MD cleaned if available (has punctuation)
    read_text   = clean_text.strip() if clean_text and len(clean_text.split()) > 100 else raw
    read_source = "md_cleaned" if (clean_text and len(clean_text.split()) > 100) else "txt_raw"

    sentences      = re.split(r'(?<=[.!?])\s+', read_text)
    sentence_count = len([s for s in sentences if len(s.strip()) > 3])
    read_words     = len(read_text.split())
    avg_sentence_len = round(read_words / sentence_count, 1) if sentence_count > 0 else 0

    flesch_ease  = round(textstat.flesch_reading_ease(read_text), 1)
    flesch_grade = round(textstat.flesch_kincaid_grade(read_text), 1)
    gunning_fog  = round(textstat.gunning_fog(read_text), 1)
    smog         = round(textstat.smog_index(read_text), 1)
    ari          = round(textstat.automated_readability_index(read_text), 1)
    coleman      = round(textstat.coleman_liau_index(read_text), 1)
    grade_vals   = [g for g in [flesch_grade, gunning_fog, smog, ari, coleman] if g > 0]
    avg_grade    = round(sum(grade_vals) / len(grade_vals), 1) if grade_vals else 0

    metrics = {
        "slug":              slug,
        "speaker":           sermon.get("speaker", ""),
        "date":              sermon.get("date", ""),
        "word_count":        word_count,
        "unique_words":      unique_words,
        "sentence_count":    sentence_count,
        "avg_sentence_len":  avg_sentence_len,
        "readability_source": read_source,
        "duration_minutes":  round(dur_min, 1) if dur_min else None,
        "estimated_wpm":     wpm,
        "wpm_class":         wpm_class,
        "flesch_reading_ease":   flesch_ease,
        "flesch_kincaid_grade":  flesch_grade,
        "gunning_fog":           gunning_fog,
        "smog_index":            smog,
        "ari":                   ari,
        "coleman_liau":          coleman,
        "avg_readability_grade": avg_grade,
        "type_token_ratio":      ttr_full,
        "ttr_200_words":         ttr_200,
        "hapax_legomena_count":  hapax_count,
        "hapax_legomena_rate":   hapax_rate,
        "question_count":        question_count,
        "question_rate":         question_rate,
    }

    metrics.update(count_fillers(raw.lower(), word_count))
    metrics.update(count_pronouns(tokens, word_count))
    metrics.update(sentiment_arc(raw))
    metrics["computed_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    return metrics


# ── Main ──────────────────────────────────────────────────────────────────────

def run(speaker=None, slug=None, all_sermons=False, force=False, workers=4):
    print(f"\n{'='*55}")
    print(f"  sermon_nlp.py")
    if speaker:
        print(f"  Speaker: {speaker}")
    elif slug:
        print(f"  Slug: {slug}")
    else:
        print(f"  Mode: all available")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*55}")

    sermons = fetch_sermons(speaker, slug, all_sermons)
    print(f"\n  Fetched {len(sermons):,} sermons from Azure SQL")

    # Skip already computed unless --force
    if not force:
        done_slugs = get_done_slugs_sql(speaker)
        before = len(sermons)
        sermons = [s for s in sermons if s["slug"] not in done_slugs]
        print(f"  Skipping {before - len(sermons):,} already computed  ({len(sermons):,} to process)")

    if not sermons:
        print(f"  Nothing to process.")
        return

    blob_svc = get_blob_svc()
    results  = []
    errors   = []

    def process_one(sermon):
        txt_text = get_transcript(sermon["slug"], blob_svc)
        if not txt_text:
            return None, sermon["slug"]
        clean_text = get_cleaned_transcript(sermon["slug"], blob_svc)
        return compute_metrics(sermon, txt_text, clean_text), None

    print(f"\n  Processing {len(sermons):,} sermons  (workers={workers})...")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(process_one, s): s["slug"] for s in sermons}
        done = 0
        for future in as_completed(futures):
            metrics, err = future.result()
            done += 1
            if metrics:
                results.append(metrics)
            else:
                errors.append(err)
            if done % 10 == 0 or done == len(sermons):
                print(f"  {done}/{len(sermons)}  ({len(errors)} errors)", end="\r")

    print(f"\n  Computed: {len(results):,}  |  Errors/missing: {len(errors):,}")

    if not results:
        print("  No results to write.")
        return

    write_metrics_sql(results, speaker)
    print(f"  ✓ Written to Azure SQL sermon_metrics")

    upload_parquet_backup(pd.DataFrame(results))
    print(f"  ✓ Backup at blob evaluations/sermon_metrics.parquet")

    if speaker and results:
        df = pd.DataFrame(results)
        print(f"\n  {speaker} — summary ({len(df)} sermons):")
        def _col_mean(col, fmt):
            if col in df.columns:
                return format(df[col].dropna().mean(), fmt)
            return "  N/A"
        print(f"    avg word count:    {_col_mean('word_count', '7,.0f')}")
        print(f"    avg WPM:           {_col_mean('estimated_wpm', '7.0f')}")
        print(f"    avg FK grade:      {_col_mean('flesch_kincaid_grade', '7.1f')}")
        print(f"    avg TTR (200w):    {_col_mean('ttr_200_words', '7.3f')}")
        print(f"    avg filler total:  {_col_mean('filler_total', '7.2f')} /1000w")
        if "pronoun_dominant" in df.columns and not df["pronoun_dominant"].dropna().empty:
            print(f"    pronoun dominant:  {df['pronoun_dominant'].mode().iloc[0]:>7}")
        if "lowry_loop_detected" in df.columns:
            print(f"    Lowry Loop:        {int(df['lowry_loop_detected'].sum()):>7} / {len(df)} sermons")
        src = df["readability_source"].value_counts()
        print(f"    readability src:   md_cleaned={src.get('md_cleaned',0)}  txt_raw={src.get('txt_raw',0)}")

    return pd.DataFrame(results)


def main():
    parser = argparse.ArgumentParser(description="Per-sermon NLP metrics")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--speaker", help="Speaker name (exact)")
    group.add_argument("--slug",    help="Single sermon slug")
    group.add_argument("--all",     action="store_true", help="All available sermons")
    parser.add_argument("--force",   action="store_true", help="Recompute already-done sermons")
    parser.add_argument("--workers", type=int, default=4, help="Parallel download workers")
    args = parser.parse_args()

    if not any([args.speaker, args.slug, args.all]):
        parser.error("Provide --speaker, --slug, or --all")

    run(speaker=args.speaker, slug=args.slug, all_sermons=args.all,
        force=args.force, workers=args.workers)


if __name__ == "__main__":
    main()
