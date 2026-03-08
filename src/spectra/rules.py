"""User-defined categorization rules (contains/regex)."""

from __future__ import annotations

import re
from typing import Any


VALID_RULE_TYPES = {"contains", "regex"}


def normalize_rule_type(value: str) -> str:
    """Normalize and validate the rule type."""
    rule_type = (value or "").strip().lower()
    if rule_type not in VALID_RULE_TYPES:
        raise ValueError("rule_type must be one of: contains, regex")
    return rule_type


def match_rule(rule: dict[str, Any], *, clean_name: str, raw_description: str) -> bool:
    """Return True when the rule matches merchant or raw description text."""
    if not rule.get("is_active", True):
        return False

    rule_type = normalize_rule_type(str(rule.get("rule_type", "")))
    pattern = str(rule.get("pattern", "")).strip()
    if not pattern:
        return False

    text = f"{clean_name} {raw_description}".lower()

    if rule_type == "contains":
        return pattern.lower() in text

    try:
        return re.search(pattern, text, flags=re.IGNORECASE) is not None
    except re.error:
        return False


def first_matching_rule(
    rules: list[dict[str, Any]], *, clean_name: str, raw_description: str
) -> dict[str, Any] | None:
    """Return the first matching rule in priority order."""
    for rule in rules:
        if match_rule(rule, clean_name=clean_name, raw_description=raw_description):
            return rule
    return None
