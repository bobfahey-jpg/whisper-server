#!/usr/bin/env python3
"""
pod_throughput.py — Per-pod RTF health check using Azure SQL telemetry.

Queries pod_gpu_metrics and sermons tables for real-time GPU utilization
and transcription RTF. No App Insights lag — data is live.

RTF thresholds:
  >= 20x  good host
  15-19x  marginal
  < 15x   slow — replace

Usage:
  python3 tools/sermons/pod_throughput.py
  python3 tools/sermons/pod_throughput.py --window 60
  python3 tools/sermons/pod_throughput.py --min-sermons 3
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

sys.path.insert(0, str(ROOT / "tools" / "sermons"))
from db import get_db


def get_transcription_stats(window: int) -> dict[str, dict]:
    """RTF stats per pod from sermons table, last `window` minutes."""
    conn = get_db(); cur = conn.cursor()
    cur.execute("""
        SELECT
            pod_id,
            COUNT(*)                        AS sermons,
            AVG(CAST(elapsed_secs AS FLOAT)) AS avg_elapsed,
            AVG(CAST(audio_secs   AS FLOAT)) AS avg_audio,
            AVG(rtf)                        AS rtf
        FROM (
            SELECT
                JSON_VALUE(custom_dims, '$.pod_id') AS pod_id,
                elapsed_secs,
                rtf,
                -- derive audio_secs from duration column (MM:SS or H:MM:SS)
                CASE
                    WHEN LEN(duration) - LEN(REPLACE(duration,':','')) = 1
                    THEN CAST(PARSENAME(REPLACE(duration,':','.'),2) AS INT)*60
                       + CAST(PARSENAME(REPLACE(duration,':','.'),1) AS INT)
                    WHEN LEN(duration) - LEN(REPLACE(duration,':','')) = 2
                    THEN CAST(PARSENAME(REPLACE(duration,':','.'),3) AS INT)*3600
                       + CAST(PARSENAME(REPLACE(duration,':','.'),2) AS INT)*60
                       + CAST(PARSENAME(REPLACE(duration,':','.'),1) AS INT)
                    ELSE NULL
                END AS audio_secs
            FROM sermons
            WHERE rtf IS NOT NULL
              AND elapsed_secs IS NOT NULL
              AND updated_at >= DATEADD(MINUTE, ?, GETUTCDATE())
        ) t
        WHERE pod_id IS NOT NULL
        GROUP BY pod_id
    """, (-window,))
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    conn.close()
    return {r[0]: dict(zip(cols, r)) for r in rows}


def get_gpu_stats() -> dict[str, float]:
    """Average GPU utilisation per pod over the last 5 minutes from pod_gpu_metrics."""
    conn = get_db(); cur = conn.cursor()
    cur.execute("""
        SELECT pod_id, AVG(CAST(gpu_util_pct AS FLOAT)) AS avg_gpu
        FROM pod_gpu_metrics
        WHERE recorded_at >= DATEADD(MINUTE, -5, GETUTCDATE())
        GROUP BY pod_id
    """)
    rows = cur.fetchall()
    conn.close()
    return {r[0]: r[1] for r in rows}


def get_gpu_stats_recent() -> list[dict]:
    """Most recent GPU reading per worker for live display."""
    conn = get_db(); cur = conn.cursor()
    cur.execute("""
        SELECT m.pod_id, m.gpu_util_pct, m.mem_used_mb, m.vram_total_mb,
               m.gpu_temp_c, m.recorded_at
        FROM pod_gpu_metrics m
        INNER JOIN (
            SELECT pod_id, MAX(recorded_at) AS latest
            FROM pod_gpu_metrics
            WHERE recorded_at >= DATEADD(MINUTE, -10, GETUTCDATE())
            GROUP BY pod_id
        ) latest ON m.pod_id = latest.pod_id AND m.recorded_at = latest.latest
        ORDER BY m.recorded_at DESC
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    conn.close()
    return [dict(zip(cols, r)) for r in rows]


def verdict(rtf: float) -> str:
    if rtf >= 20:
        return "good"
    if rtf >= 15:
        return "marginal"
    return "slow"


def main():
    parser = argparse.ArgumentParser(description="Per-pod RTF health check")
    parser.add_argument("--window",      type=int, default=30, help="Lookback minutes (default 30)")
    parser.add_argument("--min-sermons", type=int, default=1,  help="Min transcriptions to show (default 1)")
    args = parser.parse_args()

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"\n{'='*65}")
    print(f"  Pod Throughput  —  {now}")
    print(f"  Window: last {args.window} min")
    print(f"{'='*65}\n")

    tx_stats  = get_transcription_stats(args.window)
    gpu_stats = get_gpu_stats()

    if not tx_stats:
        print("  No transcription data in this window.")
        print("  (Pods may still be warming up — try again in 2-3 min)\n")

        # Show live GPU anyway
        live = get_gpu_stats_recent()
        if live:
            print("  Live GPU readings (last 10 min):")
            for r in live:
                print(f"    {r['pod_id']:<30} GPU {r['gpu_util_pct']}%  "
                      f"VRAM {r['mem_used_mb']}/{r['vram_total_mb']}MB  "
                      f"Temp {r['gpu_temp_c']}C  @ {str(r['recorded_at'])[:19]}")
        sys.exit(0)

    pods = {k: v for k, v in tx_stats.items() if v["sermons"] >= args.min_sermons}
    if not pods:
        print(f"  No pods with >= {args.min_sermons} sermon(s) in this window.\n")
        sys.exit(0)

    hdr = f"  {'pod_id':<30} {'sermons':>7}  {'avg_elapsed':>11}  {'avg_audio':>9}  {'RTF':>6}  {'GPU%':>5}  verdict"
    print(hdr)
    print(f"  {'─'*80}")

    total = 0
    for pod_id, row in pods.items():
        sermons     = row["sermons"]
        avg_elapsed = row["avg_elapsed"] or 0
        avg_audio   = row["avg_audio"]   or 0
        rtf         = row["rtf"]         or 0
        gpu         = gpu_stats.get(pod_id)
        gpu_str     = f"{gpu:.0f}%" if gpu is not None else "n/a"
        vrd         = verdict(rtf)
        total      += sermons

        print(
            f"  {pod_id:<30} {sermons:>7}  "
            f"{avg_elapsed:>9.0f}s  {avg_audio:>7,.0f}s  "
            f"{rtf:>5.1f}x  {gpu_str:>5}  {vrd}"
        )

    rate = total * (60 / args.window)
    print(f"\n  Total sermons last {args.window} min: {total}  (~{rate:.0f}/hr)\n")


if __name__ == "__main__":
    main()
