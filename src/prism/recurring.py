"""Deterministic recurring transaction detection via pattern matching."""

from __future__ import annotations

import re

# ── Known subscription merchants (lowercase) ─────────────────────
_SUBSCRIPTION_MERCHANTS = {
    # Streaming
    "netflix", "spotify", "apple.com/bill", "apple.com", "apple one",
    "disney+", "disney plus", "prime video", "amazon prime",
    "youtube premium", "youtube music", "dazn", "crunchyroll",
    "paramount+", "peacock", "now tv",
    # Cloud & SaaS
    "icloud", "google one", "google storage", "dropbox", "onedrive",
    "adobe", "notion", "chatgpt", "openai", "github", "1password",
    "lastpass", "bitwarden", "canva", "figma", "slack",
    # Telecom & Internet
    "vodafone", "iliad", "ho mobile",
    "fastweb", "tiscali", "poste mobile",
    # Fitness & Health
    "palestra", "virgin active", "mcfit",
    "technogym", "anytime fitness",
    # Insurance
    "assicurazione", "insurance", "unipolsai", "generali",
    "allianz", "zurich",
    # Domain & Hosting
    "porkbun", "godaddy", "namecheap", "cloudflare", "digitalocean",
    "heroku", "vercel", "netlify",
    # Gaming
    "playstation", "xbox game pass", "nintendo",
    # News & Media
    "medium", "substack", "patreon",
}

# Short names that need word-boundary matching to avoid false positives
# (e.g. "tre" must NOT match "trenitalia", "sky" must NOT match "skyline")
_SHORT_MERCHANTS_REGEX = [
    re.compile(r"\btre\b", re.IGNORECASE),     # Italian telecom
    re.compile(r"\btim\b", re.IGNORECASE),      # Italian telecom
    re.compile(r"\bwind\b", re.IGNORECASE),     # Italian telecom
    re.compile(r"\bsky\b", re.IGNORECASE),      # Sky TV
    re.compile(r"\bhbo\b", re.IGNORECASE),      # HBO
    re.compile(r"\bhulu\b", re.IGNORECASE),     # Hulu
    re.compile(r"\baxa\b", re.IGNORECASE),      # AXA insurance
    re.compile(r"\baws\b", re.IGNORECASE),      # Amazon Web Services
    re.compile(r"\bgym\b", re.IGNORECASE),      # Generic gym
    re.compile(r"\bxbox\b", re.IGNORECASE),     # Xbox
]

# ── Known income patterns (lowercase) ────────────────────────────
_INCOME_PATTERNS = [
    re.compile(r"\bstipendio\b", re.IGNORECASE),
    re.compile(r"\bsalary\b", re.IGNORECASE),
    re.compile(r"\bpensione\b", re.IGNORECASE),
    re.compile(r"\bpension\b", re.IGNORECASE),
    re.compile(r"\baccredito\s+(?:stipendio|competenze|emolumenti)\b", re.IGNORECASE),
    re.compile(r"\bbonifico\s+(?:a\s+(?:vostro|vs)\s+favore|ricevuto)\b", re.IGNORECASE),
    re.compile(r"\bpayroll\b", re.IGNORECASE),
    re.compile(r"\bwage\b", re.IGNORECASE),
    re.compile(r"\bcompensation\b", re.IGNORECASE),
]


def detect_recurring(
    clean_name: str,
    original_description: str,
    amount: float,
) -> str:
    """Return 'Subscription', 'Salary/Income', or '' based on pattern matching."""
    combined = f"{clean_name} {original_description}".lower()

    # ── Subscriptions: long merchant names (substring match) ──────
    for merchant in _SUBSCRIPTION_MERCHANTS:
        if merchant in combined:
            return "Subscription"

    # ── Subscriptions: short merchant names (word-boundary match) ─
    for pattern in _SHORT_MERCHANTS_REGEX:
        if pattern.search(combined):
            return "Subscription"

    # ── Recurring income (positive amount only) ───────────────────
    if amount > 0:
        for pattern in _INCOME_PATTERNS:
            if pattern.search(combined):
                return "Salary/Income"

    return ""
