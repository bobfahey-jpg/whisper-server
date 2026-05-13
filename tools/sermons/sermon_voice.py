#!/usr/bin/env python3
"""
sermon_voice.py — Voice analysis for sermon MP3s using Parselmouth/Praat.

Downloads the MP3 from UCG S3 (or accepts a local path), runs acoustic analysis,
and writes results to sermon_voice_metrics + speaker_voice_profiles.

Usage:
    python3 tools/sermons/sermon_voice.py --slug some-sermon-slug
    python3 tools/sermons/sermon_voice.py --speaker "Ken Loucks"
    python3 tools/sermons/sermon_voice.py --speaker "Ken Loucks" --limit 20
    python3 tools/sermons/sermon_voice.py --mp3-path /tmp/sermon.mp3 --slug some-slug

Notes:
    - Requires: parselmouth, praat-parselmouth, numpy, requests, pyodbc, pydub
    - Jitter/shimmer are flagged unreliable for MP3s with heavy compression artifacts.
      audio_quality_flag is set to 'mp3_compressed' as a reminder to treat those
      numbers as directional only.
    - Speaking rate (syllables/sec) is estimated via intensity envelope peaks —
      an approximation; true syllable count requires a phonetic aligner.
"""

import argparse
import os
import sys
import tempfile
import time
from pathlib import Path

import numpy as np
import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools" / "sermons"))
from db import get_db

DOWNLOAD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}


# ---------------------------------------------------------------------------
# Audio helpers
# ---------------------------------------------------------------------------

def mp3_to_wav(mp3_path: str) -> str:
    """Convert MP3 to WAV for Parselmouth. Returns temp WAV path."""
    from pydub import AudioSegment
    wav_path = mp3_path.replace(".mp3", ".wav").replace(".mp4", ".wav")
    if not wav_path.endswith(".wav"):
        wav_path = mp3_path + ".wav"
    audio = AudioSegment.from_file(mp3_path)
    audio = audio.set_channels(1).set_frame_rate(16000)
    audio.export(wav_path, format="wav")
    return wav_path


def download_mp3(mp3_url: str) -> str:
    """Download MP3 to a temp file. Returns local path."""
    r = requests.get(mp3_url, headers=DOWNLOAD_HEADERS, timeout=120, stream=True)
    r.raise_for_status()
    suffix = ".mp4" if ".mp4" in mp3_url else ".mp3"
    tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    for chunk in r.iter_content(chunk_size=65536):
        tmp.write(chunk)
    tmp.close()
    return tmp.name


# ---------------------------------------------------------------------------
# Voice analysis
# ---------------------------------------------------------------------------

def analyze_voice(audio_path: str) -> dict:
    """
    Run acoustic analysis on a local audio file (MP3 or WAV).
    Returns a dict of metrics or raises on failure.
    """
    import parselmouth
    from parselmouth.praat import call

    # Convert to mono 16kHz WAV if needed
    wav_path = None
    cleanup_wav = False
    if not audio_path.endswith(".wav"):
        wav_path = mp3_to_wav(audio_path)
        cleanup_wav = True
    else:
        wav_path = audio_path

    try:
        snd = parselmouth.Sound(wav_path)
        duration_s = snd.duration

        # ── Pitch ────────────────────────────────────────────────────────────
        pitch = snd.to_pitch(time_step=0.01, pitch_floor=60, pitch_ceiling=400)
        pitch_values = pitch.selected_array["frequency"]
        voiced = pitch_values[pitch_values > 0]

        pitch_mean  = float(np.mean(voiced))   if len(voiced) > 10 else None
        pitch_sd    = float(np.std(voiced))    if len(voiced) > 10 else None
        pitch_min   = float(np.min(voiced))    if len(voiced) > 10 else None
        pitch_max   = float(np.max(voiced))    if len(voiced) > 10 else None
        pitch_range = (pitch_max - pitch_min)  if pitch_min and pitch_max else None

        # ── Intensity ────────────────────────────────────────────────────────
        intensity   = snd.to_intensity(time_step=0.01)
        int_values  = intensity.values[0]
        int_values  = int_values[int_values > 0]
        int_mean    = float(np.mean(int_values)) if len(int_values) > 0 else None
        int_sd      = float(np.std(int_values))  if len(int_values) > 0 else None

        # ── Pauses (via intensity threshold) ────────────────────────────────
        # Silence = intensity < mean - 1 SD, sustained > 200ms
        if int_mean and int_sd:
            silence_threshold = int_mean - int_sd
            frame_duration    = 0.01  # 10ms frames
            in_pause          = False
            pause_start       = 0.0
            pauses            = []

            for i, val in enumerate(intensity.values[0]):
                t = intensity.xs()[i]
                if val < silence_threshold:
                    if not in_pause:
                        in_pause   = True
                        pause_start = t
                else:
                    if in_pause:
                        pause_dur = t - pause_start
                        if pause_dur >= 0.2:
                            pauses.append(pause_dur)
                        in_pause = False

            pause_count          = len(pauses)
            pause_mean_dur       = float(np.mean(pauses)) if pauses else 0.0
            pause_count_per_min  = pause_count / (duration_s / 60) if duration_s > 0 else 0.0
        else:
            pause_count = pause_mean_dur = pause_count_per_min = None

        # ── Speaking rate (syllable estimation via intensity peaks) ──────────
        # Approximation: count intensity local maxima above threshold
        from scipy.signal import find_peaks
        int_smooth   = np.convolve(intensity.values[0], np.ones(5)/5, mode="same")
        threshold_sr = np.percentile(int_smooth, 30)
        peaks, _     = find_peaks(int_smooth, height=threshold_sr, distance=10)
        total_pause_s     = sum(pauses) if pauses else 0.0
        speech_duration_s = max(duration_s - total_pause_s, 1.0)
        speaking_rate_syl = len(peaks) / duration_s         if duration_s > 0 else None
        articulation_rate = len(peaks) / speech_duration_s  if speech_duration_s > 0 else None

        # ── Voice quality (jitter, shimmer, HNR) ────────────────────────────
        point_process = call(snd, "To PointProcess (periodic, cc)", 60, 400)

        try:
            jitter_pct = call(point_process, "Get jitter (local)", 0, 0, 0.0001, 0.02, 1.3) * 100
        except Exception:
            jitter_pct = None

        try:
            shimmer_pct = call([snd, point_process], "Get shimmer (local)", 0, 0, 0.0001, 0.02, 1.3, 1.6) * 100
        except Exception:
            shimmer_pct = None

        try:
            harmonicity = call(snd, "To Harmonicity (cc)", 0.01, 60, 0.1, 1.0)
            hnr_db      = call(harmonicity, "Get mean", 0, 0)
        except Exception:
            hnr_db = None

        # ── CPP (Cepstral Peak Prominence) ───────────────────────────────────
        try:
            cpp_db = _compute_cpp(snd)
        except Exception:
            cpp_db = None

        return {
            "pitch_mean_hz":          round(pitch_mean, 2)         if pitch_mean else None,
            "pitch_sd_hz":            round(pitch_sd, 2)           if pitch_sd else None,
            "pitch_min_hz":           round(pitch_min, 2)          if pitch_min else None,
            "pitch_max_hz":           round(pitch_max, 2)          if pitch_max else None,
            "pitch_range_hz":         round(pitch_range, 2)        if pitch_range else None,
            "speaking_rate_syl":      round(speaking_rate_syl, 3)  if speaking_rate_syl else None,
            "articulation_rate_syl":  round(articulation_rate, 3)  if articulation_rate else None,
            "pause_count":            pause_count,
            "pause_count_per_min":    round(pause_count_per_min, 2) if pause_count_per_min else None,
            "pause_mean_duration_s":  round(pause_mean_dur, 3)     if pause_mean_dur else None,
            "intensity_mean_db":      round(int_mean, 2)           if int_mean else None,
            "intensity_sd_db":        round(int_sd, 2)             if int_sd else None,
            "jitter_pct":             round(jitter_pct, 4)         if jitter_pct else None,
            "shimmer_pct":            round(shimmer_pct, 4)        if shimmer_pct else None,
            "hnr_db":                 round(hnr_db, 2)             if hnr_db else None,
            "cpp_db":                 round(cpp_db, 2)             if cpp_db else None,
            "duration_s":             round(duration_s, 1),
            "audio_quality_flag":     "mp3_compressed",
        }

    finally:
        if cleanup_wav and wav_path and os.path.exists(wav_path):
            os.unlink(wav_path)


def _compute_cpp(snd) -> float:
    """Estimate mean CPP via cepstrum analysis (simplified)."""
    import parselmouth
    from parselmouth.praat import call

    # Use LTAS-based cepstrum approximation
    power_cepstrogram = call(snd, "To PowerCepstrogram", 60, 0.002, 5000, 50)
    cpp = call(power_cepstrogram, "Get CPPS", "yes", 0.02, 0.0, 60, 330, 0.05, "Parabolic", 0.001, 0, "Exponential decay", "Robust")
    return float(cpp)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def store_voice_metrics(slug: str, speaker: str, date, metrics: dict):
    conn = get_db()
    try:
        conn.execute("""
            MERGE sermon_voice_metrics AS target
            USING (SELECT ? AS slug) AS src ON target.slug = src.slug
            WHEN MATCHED THEN UPDATE SET
                speaker=?, date=?,
                pitch_mean_hz=?, pitch_sd_hz=?, pitch_min_hz=?, pitch_max_hz=?, pitch_range_hz=?,
                speaking_rate_syl=?, articulation_rate_syl=?,
                pause_count=?, pause_count_per_min=?, pause_mean_duration_s=?,
                intensity_mean_db=?, intensity_sd_db=?,
                jitter_pct=?, shimmer_pct=?, hnr_db=?, cpp_db=?,
                duration_s=?, audio_quality_flag=?, computed_at=GETUTCDATE()
            WHEN NOT MATCHED THEN INSERT
                (slug, speaker, date,
                 pitch_mean_hz, pitch_sd_hz, pitch_min_hz, pitch_max_hz, pitch_range_hz,
                 speaking_rate_syl, articulation_rate_syl,
                 pause_count, pause_count_per_min, pause_mean_duration_s,
                 intensity_mean_db, intensity_sd_db,
                 jitter_pct, shimmer_pct, hnr_db, cpp_db,
                 duration_s, audio_quality_flag)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
        """,
            slug,
            speaker, date,
            metrics.get("pitch_mean_hz"),    metrics.get("pitch_sd_hz"),
            metrics.get("pitch_min_hz"),     metrics.get("pitch_max_hz"),
            metrics.get("pitch_range_hz"),   metrics.get("speaking_rate_syl"),
            metrics.get("articulation_rate_syl"), metrics.get("pause_count"),
            metrics.get("pause_count_per_min"),   metrics.get("pause_mean_duration_s"),
            metrics.get("intensity_mean_db"),     metrics.get("intensity_sd_db"),
            metrics.get("jitter_pct"),       metrics.get("shimmer_pct"),
            metrics.get("hnr_db"),           metrics.get("cpp_db"),
            metrics.get("duration_s"),       metrics.get("audio_quality_flag"),
            slug,
            speaker, date,
            metrics.get("pitch_mean_hz"),    metrics.get("pitch_sd_hz"),
            metrics.get("pitch_min_hz"),     metrics.get("pitch_max_hz"),
            metrics.get("pitch_range_hz"),   metrics.get("speaking_rate_syl"),
            metrics.get("articulation_rate_syl"), metrics.get("pause_count"),
            metrics.get("pause_count_per_min"),   metrics.get("pause_mean_duration_s"),
            metrics.get("intensity_mean_db"),     metrics.get("intensity_sd_db"),
            metrics.get("jitter_pct"),       metrics.get("shimmer_pct"),
            metrics.get("hnr_db"),           metrics.get("cpp_db"),
            metrics.get("duration_s"),       metrics.get("audio_quality_flag"),
        )
        conn.commit()
    finally:
        conn.close()


def rollup_speaker(speaker: str):
    """Recompute speaker_voice_profiles aggregate for one speaker."""
    conn = get_db()
    try:
        conn.execute("""
            MERGE speaker_voice_profiles AS target
            USING (
                SELECT
                    speaker,
                    COUNT(*)                    AS sermon_count,
                    AVG(pitch_mean_hz)          AS pitch_mean_hz,
                    AVG(pitch_sd_hz)            AS pitch_sd_hz,
                    AVG(pitch_range_hz)         AS pitch_range_hz,
                    AVG(speaking_rate_syl)      AS speaking_rate_syl,
                    AVG(articulation_rate_syl)  AS articulation_rate_syl,
                    AVG(pause_count_per_min)    AS pause_count_per_min,
                    AVG(pause_mean_duration_s)  AS pause_mean_duration_s,
                    AVG(intensity_sd_db)        AS intensity_sd_db,
                    AVG(jitter_pct)             AS jitter_pct,
                    AVG(shimmer_pct)            AS shimmer_pct,
                    AVG(hnr_db)                 AS hnr_db,
                    AVG(cpp_db)                 AS cpp_db
                FROM sermon_voice_metrics
                WHERE speaker = ?
                GROUP BY speaker
            ) AS src ON target.speaker = src.speaker
            WHEN MATCHED THEN UPDATE SET
                sermon_count=src.sermon_count,
                pitch_mean_hz=src.pitch_mean_hz, pitch_sd_hz=src.pitch_sd_hz,
                pitch_range_hz=src.pitch_range_hz,
                speaking_rate_syl=src.speaking_rate_syl,
                articulation_rate_syl=src.articulation_rate_syl,
                pause_count_per_min=src.pause_count_per_min,
                pause_mean_duration_s=src.pause_mean_duration_s,
                intensity_sd_db=src.intensity_sd_db,
                jitter_pct=src.jitter_pct, shimmer_pct=src.shimmer_pct,
                hnr_db=src.hnr_db, cpp_db=src.cpp_db,
                computed_at=GETUTCDATE()
            WHEN NOT MATCHED THEN INSERT
                (speaker, sermon_count,
                 pitch_mean_hz, pitch_sd_hz, pitch_range_hz,
                 speaking_rate_syl, articulation_rate_syl,
                 pause_count_per_min, pause_mean_duration_s, intensity_sd_db,
                 jitter_pct, shimmer_pct, hnr_db, cpp_db)
            VALUES
                (src.speaker, src.sermon_count,
                 src.pitch_mean_hz, src.pitch_sd_hz, src.pitch_range_hz,
                 src.speaking_rate_syl, src.articulation_rate_syl,
                 src.pause_count_per_min, src.pause_mean_duration_s, src.intensity_sd_db,
                 src.jitter_pct, src.shimmer_pct, src.hnr_db, src.cpp_db);
        """, speaker)
        conn.commit()
        print(f"Speaker profile updated: {speaker}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Per-sermon processing
# ---------------------------------------------------------------------------

def process_slug(slug: str, mp3_path: str = None, skip_existing: bool = True):
    conn = get_db()
    cur = conn.cursor()

    if skip_existing:
        cur.execute("SELECT 1 FROM sermon_voice_metrics WHERE slug=?", slug)
        if cur.fetchone():
            conn.close()
            print(f"SKIP {slug} — already computed")
            return

    cur.execute("SELECT mp3_url, speaker, date FROM sermons WHERE slug=?", slug)
    row = cur.fetchone()
    conn.close()

    if not row:
        print(f"SKIP {slug} — not found in DB")
        return

    mp3_url, speaker, date = row.mp3_url, row.speaker, row.date

    local_path = mp3_path
    cleanup    = False
    if not local_path:
        print(f"Downloading {slug}...")
        local_path = download_mp3(mp3_url)
        cleanup    = True

    try:
        t0 = time.time()
        print(f"Analyzing {slug}...")
        metrics = analyze_voice(local_path)
        elapsed = time.time() - t0
        store_voice_metrics(slug, speaker, date, metrics)
        print(f"OK  {slug}  pitch_sd={metrics.get('pitch_sd_hz')}Hz  cpp={metrics.get('cpp_db')}dB  ({elapsed:.1f}s)")
    finally:
        if cleanup and local_path and os.path.exists(local_path):
            os.unlink(local_path)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Voice analysis for sermon MP3s")
    parser.add_argument("--slug",        help="Single sermon slug")
    parser.add_argument("--speaker",     help="Process all sermons for this speaker")
    parser.add_argument("--limit",       type=int, default=0, help="Max sermons to process (0=all)")
    parser.add_argument("--mp3-path",    help="Use local MP3 instead of downloading")
    parser.add_argument("--recompute",    action="store_true", help="Recompute even if already in DB")
    parser.add_argument("--delete-after", action="store_true", help="Delete --mp3-path after analysis (for pod_worker cleanup)")
    args = parser.parse_args()

    skip_existing = not args.recompute

    if args.slug:
        process_slug(args.slug, mp3_path=args.mp3_path, skip_existing=skip_existing)
        if args.delete_after and args.mp3_path:
            try:
                os.unlink(args.mp3_path)
            except Exception:
                pass
        if args.slug:
            conn = get_db()
            cur  = conn.cursor()
            cur.execute("SELECT speaker FROM sermons WHERE slug=?", args.slug)
            row  = cur.fetchone()
            conn.close()
            if row:
                rollup_speaker(row[0])

    elif args.speaker:
        conn = get_db()
        cur  = conn.cursor()
        q    = """
            SELECT slug FROM sermons
            WHERE speaker=? AND status='processed'
              AND mp3_url IS NOT NULL
            ORDER BY date DESC
        """
        params = [args.speaker]
        if args.limit:
            q = q.replace("SELECT slug", f"SELECT TOP {args.limit} slug")
        cur.execute(q, params)
        slugs = [r[0] for r in cur.fetchall()]
        conn.close()

        print(f"Processing {len(slugs)} sermons for {args.speaker}...")
        for slug in slugs:
            try:
                process_slug(slug, skip_existing=skip_existing)
            except Exception as e:
                print(f"ERROR {slug}: {e}")

        rollup_speaker(args.speaker)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
