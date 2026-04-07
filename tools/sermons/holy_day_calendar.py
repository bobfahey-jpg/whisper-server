#!/usr/bin/env python3
"""
holy_day_calendar.py — UCG annual holy day dates, 2013–2026.

Source: "The Annual Festivals of God" table (UCG publication).
Used by db.py to accurately classify sermons as holy_day vs sabbath/bible_study.

Feasts included (all days):
  Passover, Feast of Unleavened Bread, Pentecost, Feast of Trumpets,
  Day of Atonement, Feast of Tabernacles, The Eighth Day
"""

from datetime import date, timedelta
import re

_MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "March": 3,
    "April": 4, "May": 5, "June": 6, "Jun": 6,
    "July": 7, "Aug": 8, "Sept": 9, "Sep": 9,
    "Oct": 10, "Nov": 11, "Dec": 12,
}


def _parse_date(year: int, s: str) -> date:
    m = re.match(r"([A-Za-z]+)\s+(\d+)", s.strip())
    if not m:
        raise ValueError(f"Cannot parse date: {s!r}")
    return date(year, _MONTH_MAP[m.group(1)], int(m.group(2)))


def _expand(year: int, s: str) -> list[date]:
    """Expand 'April 5', 'Sept 19-25', or 'Sept 28-Oct 4' to a list of dates."""
    s = s.strip()
    # Cross-month range: "March 31-April 6"
    m = re.match(r"([A-Za-z]+ \d+)-([A-Za-z]+ \d+)", s)
    if m:
        start = _parse_date(year, m.group(1))
        end   = _parse_date(year, m.group(2))
    else:
        # Same-month range: "Sept 19-25"
        m = re.match(r"([A-Za-z]+) (\d+)-(\d+)", s)
        if m:
            start = _parse_date(year, f"{m.group(1)} {m.group(2)}")
            end   = _parse_date(year, f"{m.group(1)} {m.group(3)}")
        else:
            return [_parse_date(year, s)]

    out, d = [], start
    while d <= end:
        out.append(d)
        d += timedelta(days=1)
    return out


# ── Calendar data from UCG table ───────────────────────────────────────────────
_RAW = {
    2013: dict(passover="March 25",      ub="March 26-April 1",  pent="May 19",
               trumpets="Sept 5",        aton="Sept 14",         tab="Sept 19-25",  eighth="Sept 26"),
    2014: dict(passover="April 14",      ub="April 15-21",       pent="June 8",
               trumpets="Sept 25",       aton="Oct 4",           tab="Oct 9-15",    eighth="Oct 16"),
    2015: dict(passover="April 3",       ub="April 4-10",        pent="May 24",
               trumpets="Sept 14",       aton="Sept 23",         tab="Sept 28-Oct 4", eighth="Oct 5"),
    2016: dict(passover="April 22",      ub="April 23-29",       pent="June 12",
               trumpets="Oct 3",         aton="Oct 12",          tab="Oct 17-23",   eighth="Oct 24"),
    2017: dict(passover="April 10",      ub="April 11-17",       pent="June 4",
               trumpets="Sept 21",       aton="Sept 30",         tab="Oct 5-11",    eighth="Oct 12"),
    2018: dict(passover="March 30",      ub="March 31-April 6",  pent="May 20",
               trumpets="Sept 10",       aton="Sept 19",         tab="Sept 24-30",  eighth="Oct 1"),
    2019: dict(passover="April 19",      ub="April 20-26",       pent="June 9",
               trumpets="Sept 30",       aton="Oct 9",           tab="Oct 14-20",   eighth="Oct 21"),
    2020: dict(passover="April 8",       ub="April 9-15",        pent="May 31",
               trumpets="Sept 19",       aton="Sept 28",         tab="Oct 3-9",     eighth="Oct 10"),
    2021: dict(passover="March 27",      ub="March 28-April 3",  pent="May 16",
               trumpets="Sept 7",        aton="Sept 16",         tab="Sept 21-27",  eighth="Sept 28"),
    2022: dict(passover="April 15",      ub="April 16-22",       pent="June 5",
               trumpets="Sept 26",       aton="Oct 5",           tab="Oct 10-16",   eighth="Oct 17"),
    2023: dict(passover="April 5",       ub="April 6-12",        pent="May 28",
               trumpets="Sept 16",       aton="Sept 25",         tab="Sept 30-Oct 6", eighth="Oct 7"),
    2024: dict(passover="April 22",      ub="April 23-29",       pent="June 16",
               trumpets="Oct 3",         aton="Oct 12",          tab="Oct 17-23",   eighth="Oct 24"),
    2025: dict(passover="April 12",      ub="April 13-19",       pent="June 1",
               trumpets="Sept 23",       aton="Oct 2",           tab="Oct 7-13",    eighth="Oct 14"),
    2026: dict(passover="April 1",       ub="April 2-8",         pent="May 24",
               trumpets="Sept 12",       aton="Sept 21",         tab="Sept 26-Oct 2", eighth="Oct 3"),
}

# Build the set of all holy day date strings "YYYY-MM-DD"
HOLY_DATES: frozenset[str] = frozenset(
    d.strftime("%Y-%m-%d")
    for year, feasts in _RAW.items()
    for val in feasts.values()
    for d in _expand(year, val)
)


def is_holy_day(date_str: str) -> bool:
    """Return True if date_str (YYYY-MM-DD) is a UCG holy day (2013–2026)."""
    return bool(date_str) and date_str[:10] in HOLY_DATES
