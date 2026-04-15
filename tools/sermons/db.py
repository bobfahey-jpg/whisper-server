#!/usr/bin/env python3
"""
db.py — Azure SQL connection helper for the sermon archive.

Replaces the former SQLite implementation. All connection details are read
from environment variables (loaded from .env by the caller or here).

Public API:
  get_db()               → pyodbc.Connection (caller must close)
  upsert_sermon(conn, slug, **fields)
  update_sermon(conn, slug, **fields)
  get_by_status(conn, status) → list of (slug, fields_dict)
  rows_as_dicts(cursor)  → list[dict]
  content_type_for_sermon(date_str, title) → str
"""

import os
import pyodbc
from datetime import datetime, timezone, timedelta as _timedelta
from pathlib import Path

from dotenv import load_dotenv
from holy_day_calendar import is_holy_day, HOLY_DATES

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

# ── Content type logic (pure — no DB dependency) ──────────────────────────────

_BIBLE_STUDY_KEYWORDS = (
    "bible study", "bible question", "q&a", "questions and answers",
    "questions & answers", "bible studies", "study series",
)


def _near_holy_day(d) -> bool:
    """True if date d is within ±2 days of a UCG holy day and is not Sat/Sun."""
    if d.weekday() in (5, 6):
        return False
    for delta in range(-2, 3):
        if (d + _timedelta(days=delta)).strftime("%Y-%m-%d") in HOLY_DATES:
            return True
    return False


def content_type_for_sermon(date_str: str, title: str = "") -> str:
    """Return content type using UCG holy day calendar, proximity, title keywords, day-of-week.

    Priority order:
      1. Title contains Bible-study keywords → bible_study (any day)
      2. Date is in UCG holy day calendar → holy_day
      3. Date within ±2 days of a holy day, not Sat/Sun → holy_day (posting lag)
      4. Saturday or Sunday → sabbath
      5. Wednesday → bible_study
      6. Missing/unparseable date → unknown
      7. All other weekdays → other
    """
    if title and any(kw in title.lower() for kw in _BIBLE_STUDY_KEYWORDS):
        return "bible_study"
    if not date_str or len(date_str) < 10:
        return "unknown"
    if is_holy_day(date_str):
        return "holy_day"
    try:
        d = datetime.strptime(date_str[:10], "%Y-%m-%d")
        wd = d.weekday()
        if _near_holy_day(d.date()):
            return "holy_day"
        if wd == 5: return "sabbath"
        if wd == 6: return "sabbath"
        if wd == 2: return "bible_study"
        return "other"
    except ValueError:
        return "unknown"


def content_type_for_date(date_str: str) -> str:
    """Backwards-compatible wrapper."""
    return content_type_for_sermon(date_str, "")


# ── Azure SQL connection ───────────────────────────────────────────────────────

def get_db() -> pyodbc.Connection:
    """
    Return an open pyodbc connection to Azure SQL.
    Caller is responsible for closing.
    """
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.environ['AZURE_SQL_SERVER']};"
        f"DATABASE={os.environ.get('AZURE_SQL_DB', 'sermons')};"
        f"UID={os.environ['AZURE_SQL_USER']};"
        f"PWD={os.environ['AZURE_SQL_PASSWORD']};"
        f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)


# ── Row helpers ───────────────────────────────────────────────────────────────

def rows_as_dicts(cursor) -> list[dict]:
    """Convert pyodbc cursor results to a list of dicts (mimics sqlite3.Row)."""
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── CRUD ──────────────────────────────────────────────────────────────────────

def upsert_sermon(conn: pyodbc.Connection, slug: str, **fields) -> None:
    """
    Insert or update a sermon row. updated_at is always set to now.
    Uses UPDATE-then-INSERT pattern (safe for single-process orchestrator).
    """
    fields["updated_at"] = _now()
    cur = conn.cursor()

    # Try UPDATE first
    set_clause = ", ".join(f"{col} = ?" for col in fields)
    cur.execute(
        f"UPDATE sermons SET {set_clause} WHERE slug = ?",
        list(fields.values()) + [slug],
    )
    conn.commit()
    if cur.rowcount > 0:
        return

    # Row doesn't exist — INSERT
    all_fields = {"slug": slug, **fields}
    cols = ", ".join(all_fields.keys())
    vals = ", ".join("?" * len(all_fields))
    cur.execute(
        f"INSERT INTO sermons ({cols}) VALUES ({vals})",
        list(all_fields.values()),
    )
    conn.commit()


def update_sermon(conn: pyodbc.Connection, slug: str, **fields) -> None:
    """Update specific fields on an existing row. updated_at is always refreshed."""
    if not fields:
        return
    fields["updated_at"] = _now()
    assignments = ", ".join(f"{col} = ?" for col in fields)
    values = list(fields.values()) + [slug]
    conn.cursor().execute(
        f"UPDATE sermons SET {assignments} WHERE slug = ?", values
    )
    conn.commit()


def get_by_status(conn: pyodbc.Connection, status: str) -> list:
    """Return list of (slug, fields_dict) for the given status, ordered by date."""
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM sermons WHERE status = ? ORDER BY date ASC, slug ASC",
        (status,),
    )
    cols = [d[0] for d in cur.description]
    result = []
    for row in cur.fetchall():
        d = dict(zip(cols, row))
        slug = d.pop("slug")
        result.append((slug, d))
    return result
