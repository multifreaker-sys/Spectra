"""Month-to-month financial cycles.

The user picks a *start day* (1-31).  A cycle runs from the start day
of one calendar month to the day before the start day of the next month.

Examples (start_day = 25):
    25 Jan  ->  24 Feb
    25 Feb  ->  24 Mar
    25 Mar  ->  24 Apr

When start_day = 1 the cycles equal calendar months:
    1 Jan  ->  31 Jan
    1 Feb  ->  28/29 Feb
"""

from __future__ import annotations

import calendar
from datetime import date, timedelta


DEFAULT_CYCLE_START_DAY = 1
MIN_CYCLE_START_DAY = 1
MAX_CYCLE_START_DAY = 28


def normalize_cycle_start_day(value: int) -> int:
    """Clamp and validate the start day (1-28)."""
    v = int(value)
    if not MIN_CYCLE_START_DAY <= v <= MAX_CYCLE_START_DAY:
        raise ValueError(
            f"cycle_start_day must be between {MIN_CYCLE_START_DAY} and {MAX_CYCLE_START_DAY}"
        )
    return v


def parse_iso_date(value: str) -> date:
    """Parse an ISO-8601 date string."""
    return date.fromisoformat(value)


# ── month arithmetic ──────────────────────────────────────────────

def _add_months(year: int, month: int, n: int) -> tuple[int, int]:
    """Add *n* months (can be negative) to (year, month)."""
    m = year * 12 + (month - 1) + n
    return divmod(m, 12)[0], divmod(m, 12)[1] + 1


def _anchor(year: int, month: int, start_day: int) -> date:
    """The start-day anchor for a given (year, month).

    If the month is shorter than start_day the last day of the month is
    used (e.g. start_day=28 in Feb of a non-leap year → Feb 28).
    """
    last = calendar.monthrange(year, month)[1]
    return date(year, month, min(start_day, last))


# ── public API ────────────────────────────────────────────────────

def cycle_start_for(ref: date, start_day: int) -> date:
    """Return the start date of the cycle that contains *ref*."""
    start_day = normalize_cycle_start_day(start_day)
    anchor = _anchor(ref.year, ref.month, start_day)
    if ref >= anchor:
        return anchor
    # ref is before this month's anchor → previous month's anchor
    py, pm = _add_months(ref.year, ref.month, -1)
    return _anchor(py, pm, start_day)


def next_cycle_start(cycle_start: date, start_day: int) -> date:
    """Return the first day of the next cycle after *cycle_start*."""
    start_day = normalize_cycle_start_day(start_day)
    ny, nm = _add_months(cycle_start.year, cycle_start.month, 1)
    return _anchor(ny, nm, start_day)


def cycle_window_for(ref: date, start_day: int) -> tuple[date, date]:
    """Return ``(start, end_exclusive)`` for the cycle containing *ref*."""
    start = cycle_start_for(ref, start_day)
    return start, next_cycle_start(start, start_day)


def cycle_key_for(ref: date, start_day: int) -> str:
    """Stable sort-key for the cycle containing *ref*."""
    return cycle_start_for(ref, start_day).isoformat()


def format_cycle_label(start: date, end_exclusive: date) -> str:
    """Human-readable label: ``'25 Feb 2026 -> 24 Mar 2026'``."""
    end_inclusive = end_exclusive - timedelta(days=1)
    return f"{start.strftime('%d %b %Y')} -> {end_inclusive.strftime('%d %b %Y')}"