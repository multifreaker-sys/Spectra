"""Helpers for financial cycles that do not align with calendar months."""

from __future__ import annotations

import calendar
from datetime import date, timedelta


DEFAULT_CYCLE_START_DAY = 1
MIN_CYCLE_START_DAY = 1
MAX_CYCLE_START_DAY = 31
WINDOW_DAYS = 31  # payday + 30 days (inclusive)


def normalize_cycle_start_day(value: int) -> int:
    """Validate the cycle start day used for financial-month grouping."""
    if not MIN_CYCLE_START_DAY <= int(value) <= MAX_CYCLE_START_DAY:
        raise ValueError(
            f"cycle_start_day must be between {MIN_CYCLE_START_DAY} and {MAX_CYCLE_START_DAY}"
        )
    return int(value)


def parse_iso_date(value: str) -> date:
    """Parse an ISO-8601 date string."""
    return date.fromisoformat(value)


def _month_anchor(year: int, month: int, start_day: int) -> date:
    start_day = normalize_cycle_start_day(start_day)
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, min(start_day, last_day))


def cycle_start_for(value: date, start_day: int) -> date:
    """Return the start date of the payday cycle containing *value*."""
    start_day = normalize_cycle_start_day(start_day)
    current_anchor = _month_anchor(value.year, value.month, start_day)

    if value.month == 1:
        prev_year, prev_month = value.year - 1, 12
    else:
        prev_year, prev_month = value.year, value.month - 1
    previous_anchor = _month_anchor(prev_year, prev_month, start_day)

    # Pick the latest monthly payday anchor not after the target date.
    start = current_anchor if value >= current_anchor else previous_anchor

    # Normalize to fixed windows of payday + 30 days.
    while value < start:
        start -= timedelta(days=WINDOW_DAYS)
    while value >= start + timedelta(days=WINDOW_DAYS):
        start += timedelta(days=WINDOW_DAYS)
    return start


def next_cycle_start(cycle_start: date, start_day: int) -> date:
    """Return the start of the next payday window."""
    _ = normalize_cycle_start_day(start_day)
    return cycle_start + timedelta(days=WINDOW_DAYS)


def cycle_window_for(value: date, start_day: int) -> tuple[date, date]:
    """Return the [start, end) window of the financial cycle containing *value*."""
    start = cycle_start_for(value, start_day)
    return start, next_cycle_start(start, start_day)


def cycle_key_for(value: date, start_day: int) -> str:
    """Return a stable key for the financial cycle containing *value*."""
    return cycle_start_for(value, start_day).isoformat()


def format_cycle_label(start: date, end_exclusive: date) -> str:
    """Format a human-readable cycle label."""
    end_inclusive = end_exclusive - timedelta(days=1)
    return f"{start.strftime('%d %b %Y')} -> {end_inclusive.strftime('%d %b %Y')}"