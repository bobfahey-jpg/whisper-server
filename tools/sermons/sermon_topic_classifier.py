#!/usr/bin/env python3
"""
sermon_topic_classifier.py — Classify processed sermons by topic using Grok.

For each processed sermon:
  1. Downloads the .md file from Azure Blob (processed container)
  2. Sends title + first 500 words to Grok with the UCG topic taxonomy
  3. Stores results in Azure SQL (sermon_topics_v2 table)
  4. Updates the .md file in Blob with YAML frontmatter containing topic metadata

Usage:
    python3 tools/sermons/sermon_topic_classifier.py              # all unclassified
    python3 tools/sermons/sermon_topic_classifier.py --speaker "Robert Fahey"
    python3 tools/sermons/sermon_topic_classifier.py --limit 20   # test batch
    python3 tools/sermons/sermon_topic_classifier.py --dry-run --limit 5
    python3 tools/sermons/sermon_topic_classifier.py --reclassify # redo all
    python3 tools/sermons/sermon_topic_classifier.py --reclassify --where "primary_topic = 'Other'"

Cost: ~$3-5 for all 9,693 processed sermons using grok-3-fast.
"""

import argparse
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import pyodbc
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from openai import OpenAI

ROOT = Path(__file__).parent.parent.parent
load_dotenv(ROOT / ".env")

# ── Config ────────────────────────────────────────────────────────────────────

GROK_MODEL      = os.environ.get("GROK_MODEL", "grok-3-fast")
GROK_BASE_URL   = "https://api.x.ai/v1"
MAX_WORKERS     = 20       # concurrent Grok calls
WORDS_TO_SEND   = 500      # words of sermon text per classification
RUN_DATE        = date.today().isoformat()

# ── UCG Topic Taxonomy ────────────────────────────────────────────────────────
# Edit this list to add, remove, or rename topics.
# Re-run with --reclassify --where "primary_topic = 'Other'" after adding topics.

TAXONOMY = [
    # Holy Days & Sacred Calendar
    "Passover",
    "Feast of Unleavened Bread",
    "Pentecost & Firstfruits",
    "Feast of Trumpets",
    "Day of Atonement",
    "Feast of Tabernacles",
    "Last Great Day",
    "Holy Days (general)",

    # Kingdom of God & Prophecy
    "Kingdom of God",
    "Return of Christ",
    "Millennium & New Earth",
    "Great Tribulation & End Times",
    "Resurrection & Judgment",
    "Prophecy & OT Fulfillment",
    "Revelation Study",

    # Salvation & Core Doctrine
    "Repentance & Baptism",
    "Grace",
    "Law & New Covenant",
    "Faith",
    "Sanctification",
    "Spiritual Growth",
    "Gospel of the Kingdom",

    # God, Christ & Spirit
    "Nature & Character of God",
    "Jesus Christ — Life & Ministry",
    "Sacrifice & Atonement",
    "Holy Spirit",
    "Spiritual Warfare",

    # Christian Living
    "Prayer",
    "Fasting",
    "Bible Study",
    "Sabbath Observance",
    "Tithing & Stewardship",
    "Overcoming Sin",
    "Humility",
    "Service",
    "Integrity",

    # Relationships & Community
    "Marriage & Family",
    "Parenting",
    "Forgiveness",
    "Reconciliation",
    "Unity",
    "Fellowship",
    "Love",
    "Compassion",
    "Leadership & Authority",
    "Evangelism & Witness",

    # Character & Virtues
    "Faithfulness",
    "Wisdom",
    "Discernment",
    "Courage",
    "Perseverance",
    "Gratitude",
    "Contentment",
    "Trials",
    "Suffering",
    "Hope",
    "Encouragement",
    "Fear of God",

    # OT Narrative & Study
    "Abraham & Patriarchs",
    "Moses, Exodus & Wilderness",
    "Israel & Promised Land",
    "David & the Kings",
    "Major Prophets",
    "Minor Prophets",
    "Psalms & Wisdom Literature",

    # NT Study
    "Sermon on the Mount",
    "Parables of Jesus",
    "Acts & Early Church",
    "Paul's Letters",
    "General Epistles",
    "Gospels Study",

    # Special / Occasion
    "Memorial Service",
    "Youth & Young Adults",        # sermon given to/for a youth audience
    "Ambassador Bible College",
    "Church History & Mission",
    "Special Address",

    # Escape hatch — review these for taxonomy gaps
    "Other — does not fit taxonomy",
]

TAXONOMY_STR = "\n".join(f"- {t}" for t in TAXONOMY)

SYSTEM_PROMPT = f"""You are classifying UCG (United Church of God) sermons by theological topic.

You will receive a sermon title, speaker name, congregation, and the opening text of the sermon.
Select the best matching topics from the taxonomy below.

Rules:
- primary_topic: the single dominant topic (required — must be from the list)
- secondary_1: second topic if clearly present (optional, null if not)
- secondary_2: third topic if clearly present (optional, null if not)
- confidence: "high" if the primary topic is obvious, "medium" if reasonable, "low" if uncertain or poor fit
- Use "Other — does not fit taxonomy" ONLY if no topic is a reasonable match
- Respond with valid JSON only — no explanation, no markdown

Taxonomy:
{TAXONOMY_STR}

Response format:
{{"primary_topic": "...", "secondary_1": "...", "secondary_2": null, "confidence": "high"}}"""


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_conn():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.environ['AZURE_SQL_SERVER']};"
        f"DATABASE={os.environ.get('AZURE_SQL_DB', 'sermons')};"
        f"UID={os.environ['AZURE_SQL_USER']};"
        f"PWD={os.environ['AZURE_SQL_PASSWORD']};"
        f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )


_thread_local = threading.local()

def get_thread_conn():
    """Return a per-thread DB connection (pyodbc is not thread-safe)."""
    if not getattr(_thread_local, "conn", None):
        _thread_local.conn = get_conn()
    return _thread_local.conn


def ensure_table(conn):
    """Create sermon_topics_v2 if it doesn't exist."""
    cur = conn.cursor()
    cur.execute("""
        IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='sermon_topics_v2')
        BEGIN
            CREATE TABLE sermon_topics_v2 (
                slug          VARCHAR(200) PRIMARY KEY,
                speaker       VARCHAR(200),
                congregation  VARCHAR(200),
                primary_topic VARCHAR(100) NOT NULL,
                secondary_1   VARCHAR(100),
                secondary_2   VARCHAR(100),
                confidence    VARCHAR(10),
                model         VARCHAR(50),
                run_date      DATE
            );
            CREATE INDEX idx_stv2_speaker       ON sermon_topics_v2 (speaker);
            CREATE INDEX idx_stv2_topic         ON sermon_topics_v2 (primary_topic);
            CREATE INDEX idx_stv2_congregation  ON sermon_topics_v2 (congregation);
        END
    """)
    conn.commit()


def get_sermons(conn, speaker=None, slug=None, reclassify=False, where_clause=None, limit=None):
    """Return list of (slug, title, speaker, congregation) to classify."""
    if reclassify and where_clause:
        # Re-classify specific existing records
        sql = f"""
            SELECT s.slug, s.title, s.speaker, s.congregation
            FROM sermons s
            JOIN sermon_topics_v2 v2 ON s.slug = v2.slug
            WHERE s.status = 'processed'
            AND {where_clause}
        """
    elif reclassify:
        sql = """
            SELECT s.slug, s.title, s.speaker, s.congregation
            FROM sermons s WHERE s.status = 'processed'
        """
    else:
        # Only unclassified sermons
        sql = """
            SELECT s.slug, s.title, s.speaker, s.congregation
            FROM sermons s
            LEFT JOIN sermon_topics_v2 v2 ON s.slug = v2.slug
            WHERE s.status = 'processed'
            AND v2.slug IS NULL
        """

    params = []
    if speaker:
        sql += " AND s.speaker = ?"
        params.append(speaker)
    if slug:
        sql += " AND s.slug = ?"
        params.append(slug)

    sql += " ORDER BY s.date DESC"

    if limit:
        sql = sql.replace("SELECT s.slug", f"SELECT TOP {limit} s.slug")

    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchall()


def upsert_topic(conn, slug, speaker, congregation, result):
    cur = conn.cursor()
    cur.execute("""
        MERGE sermon_topics_v2 AS target
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)) AS src
              (slug, speaker, congregation, primary_topic, secondary_1, secondary_2, confidence, model, run_date)
        ON target.slug = src.slug
        WHEN MATCHED THEN UPDATE SET
            primary_topic = src.primary_topic,
            secondary_1   = src.secondary_1,
            secondary_2   = src.secondary_2,
            confidence    = src.confidence,
            model         = src.model,
            run_date      = src.run_date
        WHEN NOT MATCHED THEN INSERT
            (slug, speaker, congregation, primary_topic, secondary_1, secondary_2, confidence, model, run_date)
            VALUES (src.slug, src.speaker, src.congregation, src.primary_topic,
                    src.secondary_1, src.secondary_2, src.confidence, src.model, src.run_date);
    """, (
        slug, speaker, congregation,
        result["primary_topic"],
        result.get("secondary_1"),
        result.get("secondary_2"),
        result.get("confidence", "medium"),
        GROK_MODEL,
        RUN_DATE,
    ))
    conn.commit()


# ── Blob helpers ──────────────────────────────────────────────────────────────

def get_blob_client(blob_svc, slug):
    return blob_svc.get_container_client("processed").get_blob_client(f"{slug}.md")


def download_md(blob_svc, slug):
    """Download .md file, return text or None."""
    try:
        data = get_blob_client(blob_svc, slug).download_blob().readall()
        return data.decode("utf-8", errors="ignore")
    except Exception:
        return None


def strip_frontmatter(text):
    """Remove existing YAML frontmatter block if present. Return body text."""
    if text.startswith("---"):
        end = text.find("\n---", 3)
        if end != -1:
            return text[end + 4:].lstrip("\n")
    return text


def first_n_words(text, n=500):
    """Return first n words of text as a string."""
    words = text.split()
    return " ".join(words[:n])


def build_frontmatter(slug, title, speaker, congregation, sermon_date, result):
    """Build YAML frontmatter block from classification result."""
    def yml(val):
        if val is None:
            return "null"
        return f'"{val}"'

    lines = [
        "---",
        f"slug: {slug}",
        f"title: {yml(title)}",
        f"speaker: {yml(speaker)}",
        f"congregation: {yml(congregation)}",
        f"date: {sermon_date}",
        f"topic_primary: {yml(result['primary_topic'])}",
        f"topic_secondary_1: {yml(result.get('secondary_1'))}",
        f"topic_secondary_2: {yml(result.get('secondary_2'))}",
        f"topic_confidence: {result.get('confidence', 'medium')}",
        f"topic_model: {GROK_MODEL}",
        f"topic_date: {RUN_DATE}",
        "---",
        "",
    ]
    return "\n".join(lines)


def upload_md(blob_svc, slug, new_text):
    """Upload updated .md file back to Blob."""
    client = get_blob_client(blob_svc, slug)
    client.upload_blob(new_text.encode("utf-8"), overwrite=True)


# ── Grok classification ───────────────────────────────────────────────────────

def classify(grok_client, title, speaker, congregation, body_text, dry_run=False):
    """Send sermon excerpt to Grok, return parsed topic dict."""
    user_msg = (
        f"Title: {title}\n"
        f"Speaker: {speaker}\n"
        f"Congregation: {congregation or 'Unknown'}\n\n"
        f"Opening text:\n{first_n_words(body_text, WORDS_TO_SEND)}"
    )

    if dry_run:
        print("\n── DRY RUN PROMPT ──────────────────────────────────")
        print(f"[System] {SYSTEM_PROMPT[:300]}...")
        print(f"[User]\n{user_msg[:500]}...")
        print("────────────────────────────────────────────────────")
        return {
            "primary_topic": "DRY RUN",
            "secondary_1": None,
            "secondary_2": None,
            "confidence": "high",
        }

    resp = grok_client.chat.completions.create(
        model=GROK_MODEL,
        max_tokens=120,
        temperature=0.0,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_msg},
        ],
    )
    raw = resp.choices[0].message.content.strip()

    # Strip markdown code fences if Grok wrapped it
    raw = re.sub(r"^```json\s*|\s*```$", "", raw, flags=re.DOTALL).strip()

    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        # Fallback: try to extract JSON object
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            result = json.loads(m.group())
        else:
            result = {"primary_topic": "Other — does not fit taxonomy", "confidence": "low"}

    # Validate primary_topic is in taxonomy
    if result.get("primary_topic") not in TAXONOMY:
        result["primary_topic"] = "Other — does not fit taxonomy"
        result["confidence"] = "low"

    return result


# ── Per-sermon worker ─────────────────────────────────────────────────────────

def process_sermon(row, grok_client, blob_svc, conn, dry_run):
    """Classify one sermon and update DB + Blob. Returns (slug, status, topic)."""
    slug, title, speaker, congregation = row[0], row[1], row[2], row[3]

    # Download MD from Blob
    md_text = download_md(blob_svc, slug)
    if not md_text:
        return slug, "no_blob", None

    # Strip existing frontmatter, get body
    body = strip_frontmatter(md_text)

    # Classify
    try:
        result = classify(grok_client, title, speaker, congregation, body, dry_run=dry_run)
    except Exception as e:
        return slug, f"grok_error: {e}", None

    if dry_run:
        return slug, "dry_run", result

    # Use a per-thread connection to avoid pyodbc thread-safety issues
    tconn = get_thread_conn()

    # Update DB
    try:
        upsert_topic(tconn, slug, speaker, congregation, result)
    except Exception as e:
        return slug, f"db_error: {e}", result

    # Get sermon date for frontmatter (query from sermons table)
    try:
        _cur = tconn.cursor()
        _cur.execute("SELECT date FROM sermons WHERE slug = ?", (slug,))
        date_row = _cur.fetchone()
        sermon_date = str(date_row[0]) if date_row else ""
    except Exception:
        sermon_date = ""

    # Build new frontmatter + body, upload back to Blob
    try:
        frontmatter = build_frontmatter(slug, title, speaker, congregation, sermon_date, result)
        new_md = frontmatter + body
        upload_md(blob_svc, slug, new_md)
    except Exception as e:
        return slug, f"blob_write_error: {e}", result

    return slug, "ok", result


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Classify UCG sermons by topic using Grok")
    parser.add_argument("--speaker",    help="Filter to one speaker")
    parser.add_argument("--slug",       help="Single sermon slug")
    parser.add_argument("--limit",      type=int, help="Max sermons to process")
    parser.add_argument("--dry-run",    action="store_true", help="Show prompt, no API calls or writes")
    parser.add_argument("--reclassify", action="store_true", help="Re-classify already-classified sermons")
    parser.add_argument("--where",      help="SQL WHERE clause for --reclassify (e.g. \"primary_topic='Other'\")")
    parser.add_argument("--workers",    type=int, default=MAX_WORKERS, help=f"Concurrent workers (default {MAX_WORKERS})")
    args = parser.parse_args()

    print(f"\n=== Sermon Topic Classifier ===")
    print(f"  Model:      {GROK_MODEL}")
    print(f"  Topics:     {len(TAXONOMY)}")
    print(f"  Dry run:    {args.dry_run}")
    print(f"  Reclassify: {args.reclassify}")
    if args.speaker:
        print(f"  Speaker:    {args.speaker}")
    if args.limit:
        print(f"  Limit:      {args.limit}")

    # Connections
    conn = get_conn()
    ensure_table(conn)

    rows = get_sermons(conn, speaker=args.speaker, slug=args.slug,
                       reclassify=args.reclassify, where_clause=args.where, limit=args.limit)
    print(f"  Sermons to classify: {len(rows)}\n")

    if not rows:
        print("Nothing to classify. Use --reclassify to redo existing classifications.")
        return

    if args.dry_run:
        # Process first 5 serially for readability
        blob_svc = BlobServiceClient.from_connection_string(os.environ["AZURE_STORAGE_CONNECTION_STRING"])
        grok = OpenAI(api_key=os.environ["OPENAI_API_KEY"], base_url=GROK_BASE_URL)
        for row in rows[:5]:
            slug, status, result = process_sermon(row, grok, blob_svc, conn, dry_run=True)
            print(f"  {slug}: {result}")
        return

    blob_svc = BlobServiceClient.from_connection_string(os.environ["AZURE_STORAGE_CONNECTION_STRING"])
    grok     = OpenAI(api_key=os.environ["OPENAI_API_KEY"], base_url=GROK_BASE_URL)

    ok = errors = no_blob = 0
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(process_sermon, row, grok, blob_svc, conn, False): row[0]
            for row in rows
        }
        for i, future in enumerate(as_completed(futures), 1):
            slug, status, result = future.result()
            if status == "ok":
                ok += 1
                topic = result["primary_topic"] if result else "?"
                conf  = result.get("confidence", "?") if result else "?"
                if i % 50 == 0 or i <= 5:
                    print(f"  [{i}/{len(rows)}] {slug}: {topic} ({conf})")
            elif status == "no_blob":
                no_blob += 1
                print(f"  [{i}] NO BLOB: {slug}")
            else:
                errors += 1
                print(f"  [{i}] ERROR {slug}: {status}")

    elapsed = time.time() - t0
    print(f"\n  Done in {elapsed:.0f}s")
    print(f"  Classified: {ok} | No blob: {no_blob} | Errors: {errors}")
    print(f"\n  Review 'Other' bucket:")
    print(f"  SELECT s.title, s.speaker, s.date FROM sermon_topics_v2 v")
    print(f"  JOIN sermons s ON v.slug = s.slug")
    print(f"  WHERE v.primary_topic = 'Other — does not fit taxonomy'")
    print(f"\n  Review low-confidence:")
    print(f"  SELECT s.title, v.primary_topic FROM sermon_topics_v2 v")
    print(f"  JOIN sermons s ON v.slug = s.slug WHERE v.confidence = 'low'")

    conn.close()


if __name__ == "__main__":
    main()
