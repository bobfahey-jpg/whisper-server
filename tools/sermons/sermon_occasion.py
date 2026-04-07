#!/usr/bin/env python3
"""
sermon_occasion.py — Classify every sermon by occasion type.

Types: holy_day | sermon | sermonette | bible_study | other

Priority order (first match wins):
  1. Title contains bible-study keywords
  2. Title contains book+chapter pattern (e.g. "Romans 8")
  3. Title contains "Bible" + chapter/verse reference
  4. Date is an exact UCG Holy Day calendar match
  5. Date within ±2 days of a Holy Day (upload lag), not Sat/Sun
  6. Title contains Holy Day keywords
  7. Duration < 25 min on Sat/Sun → sermonette
  8. Saturday or Sunday, duration 25–70 min → sermon
  9. Wednesday → bible_study
 10. Title contains special-event keywords → other
 11. Fallback → sermon

Writes results to: Azure SQL sermon_occasions table
Also writes parquet backup to: Azure Blob evaluations/sermon_occasions.parquet

Usage:
  python3 tools/sermons/sermon_occasion.py                      # all sermons
  python3 tools/sermons/sermon_occasion.py --speaker "Robert Fahey"
  python3 tools/sermons/sermon_occasion.py --dry-run            # print only
"""

import argparse
import io
import json
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools" / "sermons"))
load_dotenv(ROOT / ".env")

from holy_day_calendar import is_holy_day, HOLY_DATES as _HOLY_DATES_SET

HOLY_DATES  = _HOLY_DATES_SET
CONFIG_DIR  = ROOT / "tools" / "sermons" / "eval_config"

BLOB_EVALUATIONS = os.environ.get("AZURE_BLOB_EVALUATIONS", "evaluations")

# ── Bible book regex ───────────────────────────────────────────────────────────
with open(CONFIG_DIR / "bible_books.json") as f:
    _BOOKS_DATA = json.load(f)

_ALL_BOOKS = (
    _BOOKS_DATA["old_testament"] +
    _BOOKS_DATA["new_testament"] +
    list(_BOOKS_DATA["abbreviations"].keys())
)
_ALL_BOOKS_SORTED = sorted(_ALL_BOOKS, key=len, reverse=True)
_BOOK_PATTERN = re.compile(
    r'\b(' + '|'.join(re.escape(b) for b in _ALL_BOOKS_SORTED) + r')\.?\s+\d+',
    re.IGNORECASE
)

# ── Keyword lists ──────────────────────────────────────────────────────────────
_BIBLE_STUDY_KEYWORDS = [
    "bible study", "bible question", "q&a", "questions and answers",
    "questions & answers", "bible studies", "study series", "bible class",
]

_HOLY_DAY_KEYWORDS = {
    "Passover":              ["passover"],
    "Unleavened Bread":      ["unleavened bread", "days of unleavened", "feast of unleavened"],
    "Pentecost":             ["pentecost", "firstfruits", "feast of weeks"],
    "Feast of Trumpets":     ["feast of trumpets", "day of trumpets", "rosh hashanah"],
    "Day of Atonement":      ["day of atonement", "atonement", "yom kippur"],
    "Feast of Tabernacles":  ["feast of tabernacles", "tabernacles", "ingathering", "sukkot"],
    "Last Great Day":        ["last great day", "eighth day", "great last day"],
}

_SPECIAL_EVENT_KEYWORDS = {
    "camp":        ["camp", "summer camp", "youth camp"],
    "conference":  ["conference", "ministerial", "leadership"],
    "wedding":     ["wedding", "marriage ceremony"],
    "memorial":    ["memorial", "funeral", "in memory"],
    "ordination":  ["ordination", "ordained"],
    "graduation":  ["graduation", "commencement"],
    "youth":       ["youth", "teen", "young adult"],
    "visiting":    ["visiting", "visit from"],
}


# ── Duration parser ────────────────────────────────────────────────────────────

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


# ── Core classifier ────────────────────────────────────────────────────────────

def classify(slug: str, title: str, date_str: str, duration: str) -> dict:
    title_lower = (title or "").lower()
    dur_min     = parse_duration_minutes(duration)
    date_clean  = (date_str or "")[:10]
    weekday     = None

    if len(date_clean) == 10:
        try:
            weekday = datetime.strptime(date_clean, "%Y-%m-%d").weekday()
        except ValueError:
            pass

    if any(kw in title_lower for kw in _BIBLE_STUDY_KEYWORDS):
        return _result(slug, "bible_study", None, dur_min, "title_keyword", "high")

    if _BOOK_PATTERN.search(title or ""):
        return _result(slug, "bible_study", None, dur_min, "title_book_chapter", "high")

    if re.search(r'\bBible\b.{0,20}\d+:\d+', title or "", re.IGNORECASE):
        return _result(slug, "bible_study", None, dur_min, "title_bible_ref", "high")

    if date_clean and is_holy_day(date_clean):
        hd_name = next(
            (name for name, kws in _HOLY_DAY_KEYWORDS.items()
             if any(kw in title_lower for kw in kws)),
            "Holy Day"
        )
        return _result(slug, "holy_day", hd_name, dur_min, "calendar_exact", "high")

    if date_clean and weekday not in (5, 6):
        if any(d[:10] in HOLY_DATES for d in [
            (datetime.strptime(date_clean, "%Y-%m-%d") + timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(-2, 3)
        ] if len(date_clean) == 10):
            hd_name = next(
                (name for name, kws in _HOLY_DAY_KEYWORDS.items()
                 if any(kw in title_lower for kw in kws)),
                "Holy Day"
            )
            return _result(slug, "holy_day", hd_name, dur_min, "calendar_proximity", "medium")

    for hd_name, keywords in _HOLY_DAY_KEYWORDS.items():
        if any(kw in title_lower for kw in keywords):
            return _result(slug, "holy_day", hd_name, dur_min, "title_keyword", "high")

    if dur_min is not None and weekday in (5, 6):
        if dur_min < 25:
            conf = "high" if dur_min < 20 else "medium"
            return _result(slug, "sermonette", None, dur_min, "duration", conf)

    if weekday in (5, 6):
        return _result(slug, "sermon", None, dur_min, "weekday", "high")

    if weekday == 2:
        return _result(slug, "bible_study", None, dur_min, "weekday_wednesday", "medium")

    for sub_type, keywords in _SPECIAL_EVENT_KEYWORDS.items():
        if any(kw in title_lower for kw in keywords):
            return _result(slug, "other", sub_type, dur_min, "title_keyword", "medium")

    return _result(slug, "sermon", None, dur_min, "fallback", "low")


def _result(slug, occasion_type, sub_type, dur_min, source, confidence):
    return {
        "slug":                  slug,
        "occasion_type":         occasion_type,
        "sub_type":              sub_type,
        "is_sermonette":         occasion_type == "sermonette",
        "duration_minutes":      round(dur_min, 1) if dur_min else None,
        "classification_source": source,
        "confidence":            confidence,
    }


# ── Azure helpers ──────────────────────────────────────────────────────────────

def get_sql_conn():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.environ['AZURE_SQL_SERVER']};"
        f"DATABASE={os.environ.get('AZURE_SQL_DB', 'sermons')};"
        f"UID={os.environ['AZURE_SQL_USER']};"
        f"PWD={os.environ['AZURE_SQL_PASSWORD']};"
        f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )


def get_blob_svc() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(
        os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    )


def fetch_sermons(speaker: str | None = None, slug: str | None = None) -> list[dict]:
    conn = get_sql_conn()
    try:
        query = "SELECT slug, title, speaker, date, duration FROM sermons"
        conditions, params = [], []
        if speaker:
            conditions.append("speaker = ?")
            params.append(speaker)
        if slug:
            conditions.append("slug = ?")
            params.append(slug)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY date DESC"
        cur = conn.cursor()
        cur.execute(query, params)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


# ── SQL write ─────────────────────────────────────────────────────────────────

_OCCASION_COLS = [
    "slug", "speaker", "occasion_type", "sub_type",
    "is_sermonette", "duration_minutes", "classification_source", "confidence",
]

def write_occasions_sql(rows: list[dict], speaker: str | None, slug: str | None = None):
    """Delete existing rows for this speaker/slug (or all), bulk insert new rows."""
    if not rows:
        return
    conn = get_sql_conn()
    try:
        cur = conn.cursor()
        if slug:
            cur.execute("DELETE FROM sermon_occasions WHERE slug=?", (slug,))
        elif speaker:
            cur.execute("DELETE FROM sermon_occasions WHERE speaker=?", (speaker,))
        else:
            cur.execute("TRUNCATE TABLE sermon_occasions")

        insert_sql = (
            f"INSERT INTO sermon_occasions ({', '.join(_OCCASION_COLS)}) "
            f"VALUES ({', '.join('?' * len(_OCCASION_COLS))})"
        )
        params = []
        for r in rows:
            params.append([
                r["slug"], r.get("speaker", ""), r["occasion_type"], r["sub_type"],
                int(r["is_sermonette"]),
                r["duration_minutes"],
                r["classification_source"], r["confidence"],
            ])
        cur.fast_executemany = True
        cur.executemany(insert_sql, params)
        conn.commit()
    finally:
        conn.close()


def upload_parquet_backup(df: pd.DataFrame):
    """Upload parquet snapshot to Azure Blob evaluations/sermon_occasions.parquet"""
    try:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        svc = get_blob_svc()
        svc.get_container_client(BLOB_EVALUATIONS).upload_blob(
            "sermon_occasions.parquet", buf, overwrite=True
        )
    except Exception as e:
        print(f"  ⚠  Blob backup failed: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────

def run(speaker: str | None = None, slug: str | None = None, dry_run: bool = False):
    print(f"\n{'='*55}")
    print(f"  sermon_occasion.py")
    if speaker:
        print(f"  Speaker: {speaker}")
    if slug:
        print(f"  Slug:    {slug}")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*55}")

    sermons = fetch_sermons(speaker=speaker, slug=slug)
    print(f"\n  Fetched {len(sermons):,} sermons from Azure SQL")

    results = [classify(s["slug"], s["title"], s["date"], s["duration"]) for s in sermons]

    slug_to_speaker = {s["slug"]: s["speaker"] for s in sermons}
    for r in results:
        r["speaker"] = slug_to_speaker.get(r["slug"], "")

    df = pd.DataFrame(results)

    print(f"\n  Occasion breakdown:")
    for occ, count in df["occasion_type"].value_counts().items():
        pct = count / len(df) * 100
        print(f"    {occ:<20} {count:>5,}  {pct:>4.0f}%")

    print(f"\n  Confidence breakdown:")
    for conf, count in df["confidence"].value_counts().items():
        pct = count / len(df) * 100
        print(f"    {conf:<10} {count:>5,}  {pct:>4.0f}%")

    if speaker:
        print(f"\n  Sermonettes detected: {df['is_sermonette'].sum()}")
        print(f"  Bible studies:        {(df['occasion_type']=='bible_study').sum()}")
        print(f"  Holy days:            {(df['occasion_type']=='holy_day').sum()}")
        hd = df[df["occasion_type"] == "holy_day"]["sub_type"].value_counts()
        for name, count in hd.items():
            print(f"    {name:<30} {count}")

    if dry_run:
        print(f"\n  [dry-run] Would write {len(df)} rows to Azure SQL sermon_occasions")
        return df

    write_occasions_sql(results, speaker, slug=slug)
    print(f"\n  ✓ Written {len(results):,} rows to Azure SQL sermon_occasions")

    if not slug:  # skip parquet backup for single-slug runs
        upload_parquet_backup(df)
        print(f"  ✓ Backup uploaded to blob evaluations/sermon_occasions.parquet")

    return df


def main():
    parser = argparse.ArgumentParser(description="Classify sermon occasions")
    parser.add_argument("--speaker", help="Filter to one speaker (exact name)")
    parser.add_argument("--slug",    help="Single sermon slug")
    parser.add_argument("--dry-run", action="store_true", help="Print results, don't write")
    args = parser.parse_args()
    run(speaker=args.speaker, slug=args.slug, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
