#!/usr/bin/env python3
"""
sermon_scripture.py — Scripture citation extraction + depth scoring.

For each TXT transcript:
  - Extracts all Bible citations using regex (all 66 books + abbreviations)
  - Scores each citation block by depth: proof_text / topical / passage_based / deep_exposition
  - Computes OT/NT ratio, canon breadth, exposition depth score
  - Classifies speaker's overall preaching style

Writes to:
  Azure SQL sermon_scriptures table     — one row per citation
  Azure SQL sermon_scripture_summary    — one row per sermon
  Azure Blob evaluations/              — parquet backups

Usage:
  python3 tools/sermons/sermon_scripture.py --speaker "Robert Fahey"
  python3 tools/sermons/sermon_scripture.py --speaker "Nathan Ekama" --force
  python3 tools/sermons/sermon_scripture.py --all
"""

import argparse
import io
import json
import os
import re
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

ROOT           = Path(__file__).resolve().parents[2]
TRANSCRIPT_DIR = ROOT / "data" / "sermons" / "transcripts"   # temp cache
CONFIG_DIR     = ROOT / "tools" / "sermons" / "eval_config"

TRANSCRIPT_DIR.mkdir(parents=True, exist_ok=True)

sys.path.insert(0, str(ROOT / "tools" / "sermons"))
load_dotenv(ROOT / ".env")

BLOB_EVALUATIONS = os.environ.get("AZURE_BLOB_EVALUATIONS", "evaluations")

# ── Load bible books config ────────────────────────────────────────────────────
with open(CONFIG_DIR / "bible_books.json") as f:
    _BOOKS_DATA = json.load(f)

OT_BOOKS = set(_BOOKS_DATA["old_testament"])
NT_BOOKS = set(_BOOKS_DATA["new_testament"])
ABBREV   = _BOOKS_DATA["abbreviations"]

_ALL_NAMES = list(OT_BOOKS) + list(NT_BOOKS) + list(ABBREV.keys())
_ALL_NAMES_SORTED = sorted(set(_ALL_NAMES), key=len, reverse=True)

_CITATION_RE = re.compile(
    r'\b(' + '|'.join(re.escape(b) for b in _ALL_NAMES_SORTED) + r')\.?'
    r'\s+(\d{1,3})'
    r'(?::(\d{1,3})(?:\s*[-–]\s*(\d{1,3}))?)?',
    re.IGNORECASE
)

# SQL columns for each table
_CITATION_COLS = [
    "slug", "speaker", "book", "chapter", "verse_start", "verse_end",
    "testament", "citation_text", "position_in_sermon",
    "words_before_next", "consecutive_same_chapter", "depth_class",
]

_SUMMARY_COLS = [
    "slug", "speaker", "citation_count", "unique_books",
    "ot_count", "nt_count", "ot_ratio", "scripture_density",
    "avg_words_between", "exposition_depth_score", "preaching_style", "top_books",
]


def normalise_book(raw: str) -> tuple[str, str]:
    for abbr, full in ABBREV.items():
        if raw.strip().lower() == abbr.lower():
            raw = full
            break
    for book in list(OT_BOOKS) + list(NT_BOOKS):
        if raw.strip().lower() == book.lower():
            raw = book
            break
    testament = "OT" if raw in OT_BOOKS else ("NT" if raw in NT_BOOKS else "unknown")
    return raw, testament


# ── Azure helpers ──────────────────────────────────────────────────────────────

def get_blob_svc() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(
        os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    )


def get_transcript(slug: str, blob_svc: BlobServiceClient) -> str | None:
    local = TRANSCRIPT_DIR / f"{slug}.txt"
    if local.exists():
        return local.read_text(encoding="utf-8")
    try:
        data = blob_svc.get_container_client(
            os.environ.get("AZURE_BLOB_TRANSCRIPTS", "transcripts")
        ).download_blob(f"{slug}.txt").readall()
        text = data.decode("utf-8")
        local.write_text(text, encoding="utf-8")
        return text
    except Exception:
        return None


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


def fetch_sermons(speaker=None, slug=None):
    conn = get_sql_conn()
    try:
        cur = conn.cursor()
        if slug:
            cur.execute(
                "SELECT slug, title, speaker, date FROM sermons WHERE slug=?", (slug,)
            )
        elif speaker:
            cur.execute("""
                SELECT slug, title, speaker, date FROM sermons
                WHERE speaker=? AND status IN ('transcribed','processed')
                ORDER BY date DESC
            """, (speaker,))
        else:
            cur.execute("""
                SELECT slug, title, speaker, date FROM sermons
                WHERE status IN ('transcribed','processed')
                ORDER BY date DESC
            """)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def get_done_slugs_sql(speaker: str | None = None) -> set:
    conn = get_sql_conn()
    try:
        cur = conn.cursor()
        if speaker:
            cur.execute("SELECT slug FROM sermon_scripture_summary WHERE speaker=?", (speaker,))
        else:
            cur.execute("SELECT slug FROM sermon_scripture_summary")
        return {r[0] for r in cur.fetchall()}
    finally:
        conn.close()


def write_scriptures_sql(all_citations: list[dict], all_summaries: list[dict],
                         speaker: str | None):
    """
    For citations: delete by slug then bulk insert.
    For summaries: delete by speaker (or slugs), bulk insert.
    """
    if not all_summaries:
        return

    conn = get_sql_conn()
    try:
        cur = conn.cursor()
        slugs = [s["slug"] for s in all_summaries]

        # Delete existing citations and summaries for these slugs
        for i in range(0, len(slugs), 500):
            batch = slugs[i:i+500]
            ph    = ",".join("?" * len(batch))
            cur.execute(f"DELETE FROM sermon_scriptures WHERE slug IN ({ph})", batch)
            cur.execute(f"DELETE FROM sermon_scripture_summary WHERE slug IN ({ph})", batch)

        # Insert citations
        if all_citations:
            cit_sql = (
                f"INSERT INTO sermon_scriptures ({', '.join(_CITATION_COLS)}) "
                f"VALUES ({', '.join('?' * len(_CITATION_COLS))})"
            )
            cit_params = []
            for c in all_citations:
                cit_params.append([
                    c["slug"], c.get("speaker", ""),
                    c["book"], c["chapter"], c.get("verse_start"), c.get("verse_end"),
                    c["testament"], c.get("citation_text", ""),
                    c.get("position"), c.get("words_to_next"),
                    int(c.get("consecutive_same_chapter", False)),
                    c.get("depth_class", ""),
                ])
            cur.fast_executemany = True
            cur.executemany(cit_sql, cit_params)

        # Insert summaries
        sum_sql = (
            f"INSERT INTO sermon_scripture_summary ({', '.join(_SUMMARY_COLS)}) "
            f"VALUES ({', '.join('?' * len(_SUMMARY_COLS))})"
        )
        sum_params = []
        for s in all_summaries:
            sum_params.append([
                s["slug"], s.get("speaker", ""),
                s["citation_count"], s["unique_books"],
                s["ot_count"], s["nt_count"],
                s.get("ot_nt_ratio"),      # mapped to ot_ratio in SQL
                s["scripture_density"],
                s.get("avg_words_between"),
                s["exposition_depth_score"],
                s["preaching_style"],
                s.get("top_books", ""),
            ])
        cur.fast_executemany = True
        cur.executemany(sum_sql, sum_params)
        conn.commit()
    finally:
        conn.close()


def upload_parquet_backups(all_citations: list[dict], all_summaries: list[dict]):
    try:
        svc = get_blob_svc()
        container = svc.get_container_client(BLOB_EVALUATIONS)
        for data, name in [
            (all_summaries, "sermon_scripture_summary.parquet"),
            (all_citations, "sermon_scriptures.parquet"),
        ]:
            if data:
                buf = io.BytesIO()
                pd.DataFrame(data).to_parquet(buf, index=False)
                buf.seek(0)
                container.upload_blob(name, buf, overwrite=True)
    except Exception as e:
        print(f"  ⚠  Blob backup failed: {e}")


# ── Core extraction ────────────────────────────────────────────────────────────

def extract_citations(slug: str, speaker: str, text: str) -> list[dict]:
    words      = text.split()
    word_count = len(words)
    citations  = []

    for m in _CITATION_RE.finditer(text):
        raw_book = m.group(1)
        chapter  = int(m.group(2))
        verse_s  = int(m.group(3)) if m.group(3) else None
        verse_e  = int(m.group(4)) if m.group(4) else verse_s
        book, testament = normalise_book(raw_book)

        char_pos = m.start()
        word_pos = len(text[:char_pos].split())
        position = round(word_pos / word_count, 3) if word_count > 0 else 0

        citation_text = f"{book} {chapter}" + (f":{verse_s}" if verse_s else "")

        citations.append({
            "slug":         slug,
            "speaker":      speaker,
            "book":         book,
            "testament":    testament,
            "chapter":      chapter,
            "verse_start":  verse_s,
            "verse_end":    verse_e,
            "citation_text": citation_text,
            "char_pos":     char_pos,
            "word_pos":     word_pos,
            "position":     position,
        })

    for i, c in enumerate(citations):
        if i + 1 < len(citations):
            c["words_to_next"] = citations[i + 1]["word_pos"] - c["word_pos"]
            c["consecutive_same_chapter"] = (
                citations[i + 1]["book"] == c["book"] and
                citations[i + 1]["chapter"] == c["chapter"]
            )
        else:
            c["words_to_next"] = word_count - c["word_pos"]
            c["consecutive_same_chapter"] = False
        c["depth_class"] = depth_classify(c["words_to_next"], c["consecutive_same_chapter"])

    return citations


def depth_classify(words_to_next: int, consecutive: bool) -> str:
    if words_to_next < 100:
        return "proof_text"
    if words_to_next < 200:
        return "topical"
    if words_to_next < 500 or not consecutive:
        return "passage_based"
    return "deep_exposition"


def sermon_summary(slug: str, speaker: str, citations: list[dict], word_count: int) -> dict:
    if not citations:
        return {
            "slug": slug, "speaker": speaker,
            "citation_count": 0, "unique_books": 0,
            "ot_count": 0, "nt_count": 0, "ot_nt_ratio": None,
            "scripture_density": 0, "canon_breadth_score": 0,
            "avg_words_between": None, "citation_clustering_score": 0,
            "max_consecutive_same_chapter": 0,
            "exposition_depth_score": 0, "preaching_style": "unknown",
            "top_books": "",
        }

    total_books = len(OT_BOOKS) + len(NT_BOOKS)
    books       = [c["book"] for c in citations]
    unique_bks  = len(set(books))
    ot_count    = sum(1 for c in citations if c["testament"] == "OT")
    nt_count    = sum(1 for c in citations if c["testament"] == "NT")
    total_cites = len(citations)

    ot_nt_ratio = round(ot_count / (ot_count + nt_count), 3) if (ot_count + nt_count) > 0 else None
    density     = round(total_cites / word_count * 1000, 2) if word_count > 0 else 0
    breadth     = round(unique_bks / total_books, 3)

    wtns    = [c["words_to_next"] for c in citations if c.get("words_to_next") is not None]
    avg_wtn = round(sum(wtns) / len(wtns), 0) if wtns else None

    consec     = sum(1 for c in citations if c.get("consecutive_same_chapter"))
    cluster_sc = round(consec / total_cites, 3) if total_cites > 0 else 0

    max_run = cur_run = 0
    for c in citations:
        if c.get("consecutive_same_chapter"):
            cur_run += 1
            max_run = max(max_run, cur_run)
        else:
            cur_run = 0

    depths       = [c["depth_class"] for c in citations]
    depth_counts = Counter(depths)
    depth_weights = {"proof_text": 0, "topical": 1, "passage_based": 2, "deep_exposition": 3}
    exp_score = round(
        sum(depth_weights.get(d, 0) for d in depths) / (total_cites * 3), 3
    ) if total_cites > 0 else 0

    if exp_score >= 0.60:
        style = "expository"
    elif exp_score >= 0.35:
        style = "passage_based"
    elif exp_score >= 0.15:
        style = "topical"
    else:
        style = "proof_text"

    top_books = ", ".join(b for b, _ in Counter(books).most_common(3))

    return {
        "slug":                         slug,
        "speaker":                      speaker,
        "citation_count":               total_cites,
        "unique_books":                 unique_bks,
        "ot_count":                     ot_count,
        "nt_count":                     nt_count,
        "ot_nt_ratio":                  ot_nt_ratio,
        "scripture_density":            density,
        "canon_breadth_score":          breadth,
        "avg_words_between":            avg_wtn,
        "citation_clustering_score":    cluster_sc,
        "max_consecutive_same_chapter": max_run,
        "exposition_depth_score":       exp_score,
        "preaching_style":              style,
        "top_books":                    top_books,
        "depth_proof_text":             depth_counts.get("proof_text", 0),
        "depth_topical":                depth_counts.get("topical", 0),
        "depth_passage_based":          depth_counts.get("passage_based", 0),
        "depth_deep_exposition":        depth_counts.get("deep_exposition", 0),
    }


def process_one(sermon: dict, blob_svc: BlobServiceClient) -> tuple[list, dict | None]:
    text = get_transcript(sermon["slug"], blob_svc)
    if not text:
        return [], None
    word_count = len(text.split())
    citations  = extract_citations(sermon["slug"], sermon.get("speaker", ""), text)
    summary    = sermon_summary(sermon["slug"], sermon.get("speaker", ""), citations, word_count)
    return citations, summary


# ── Main ──────────────────────────────────────────────────────────────────────

def run(speaker=None, slug=None, all_sermons=False, force=False, workers=4):
    print(f"\n{'='*55}")
    print(f"  sermon_scripture.py")
    if speaker:
        print(f"  Speaker: {speaker}")
    elif slug:
        print(f"  Slug: {slug}")
    else:
        print(f"  Mode: all available")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*55}")

    sermons = fetch_sermons(speaker, slug)
    print(f"\n  Fetched {len(sermons):,} sermons from Azure SQL")

    if not force:
        done = get_done_slugs_sql(speaker)
        before = len(sermons)
        sermons = [s for s in sermons if s["slug"] not in done]
        print(f"  Skipping {before - len(sermons):,} already computed  ({len(sermons):,} to process)")

    if not sermons:
        print("  Nothing to process.")
        return

    blob_svc      = get_blob_svc()
    all_citations = []
    all_summaries = []
    errors        = []

    print(f"\n  Processing {len(sermons):,} sermons  (workers={workers})...")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(process_one, s, blob_svc): s["slug"] for s in sermons}
        done_count = 0
        for future in as_completed(futures):
            citations, summary = future.result()
            done_count += 1
            if summary:
                all_citations.extend(citations)
                all_summaries.append(summary)
            else:
                errors.append(futures[future])
            if done_count % 10 == 0 or done_count == len(sermons):
                print(f"  {done_count}/{len(sermons)}  ({len(errors)} errors)", end="\r")

    print(f"\n  Extracted: {len(all_summaries):,}  |  Errors/missing: {len(errors):,}")
    print(f"  Total citations found: {len(all_citations):,}")

    if not all_summaries:
        print("  No results to write.")
        return

    write_scriptures_sql(all_citations, all_summaries, speaker)
    print(f"  ✓ Written to Azure SQL (sermon_scriptures + sermon_scripture_summary)")

    upload_parquet_backups(all_citations, all_summaries)
    print(f"  ✓ Backups at blob evaluations/sermon_scripture*.parquet")

    if speaker:
        df = pd.DataFrame(all_summaries)
        df = df[df["citation_count"] > 0]
        if not df.empty:
            print(f"\n  {speaker} — scripture summary ({len(df)} sermons with citations):")
            print(f"    avg citations/sermon:   {df['citation_count'].mean():>6.1f}")
            print(f"    avg unique books:        {df['unique_books'].mean():>6.1f}")
            print(f"    avg OT ratio:            {df['ot_nt_ratio'].dropna().mean():>6.1%}")
            print(f"    avg scripture density:   {df['scripture_density'].mean():>6.2f} /1000w")
            print(f"    avg exposition score:    {df['exposition_depth_score'].mean():>6.3f}")
            print(f"    preaching style (mode):  {df['preaching_style'].mode().iloc[0]:>10}")
            print(f"\n    Style breakdown:")
            for style, count in df["preaching_style"].value_counts().items():
                print(f"      {style:<20} {count:>4} sermons")
            all_books: list[str] = []
            for top in df["top_books"]:
                all_books.extend([b.strip() for b in str(top).split(",") if b.strip()])
            print(f"\n    Most cited books:")
            for book, cnt in Counter(all_books).most_common(5):
                print(f"      {book:<25} cited in {cnt} sermons")

    new_summaries = pd.DataFrame(all_summaries)
    return new_summaries


def main():
    parser = argparse.ArgumentParser(description="Scripture extraction + depth scoring")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--speaker", help="Speaker name (exact)")
    group.add_argument("--slug",    help="Single sermon slug")
    group.add_argument("--all",     action="store_true")
    parser.add_argument("--force",   action="store_true")
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    if not any([args.speaker, args.slug, args.all]):
        parser.error("Provide --speaker, --slug, or --all")

    run(speaker=args.speaker, slug=args.slug, all_sermons=args.all,
        force=args.force, workers=args.workers)


if __name__ == "__main__":
    main()
