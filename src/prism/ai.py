"""LLM-based transaction categoriser — supports Gemini and OpenAI."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from pydantic import BaseModel

logger = logging.getLogger("prism.ai")


# ── Data models ──────────────────────────────────────────────────


class CategorisedTransaction(BaseModel):
    """A transaction after LLM processing."""

    original_description: str
    clean_name: str
    category: str
    amount: float
    currency: str
    date: str
    recurring: bool = False


# ── Prompt ───────────────────────────────────────────────────────

_SYSTEM_PROMPT = """\
You are a personal finance assistant. Your job is to analyze raw bank transaction \
descriptions and return a structured JSON array.

RULES:
1. Clean the raw description into a human-readable merchant name \
   (e.g. "POS 1234 STRBKS IT" → "Starbucks").
2. Assign a category from the list of existing categories if one fits.
3. If no existing category fits, create a meaningful new one in English.
4. If truly ambiguous, use the category "Other".
5. Set "recurring" to true if the transaction looks like a subscription or \
   recurring payment (e.g. Netflix, Spotify, Apple, iCloud, gym memberships, \
   insurance, phone plans, streaming services, SaaS). Otherwise set it to false.
6. Reply ONLY with a valid JSON array, no extra text.

OUTPUT FORMAT (JSON array):
[
  {
    "original": "<raw description>",
    "clean_name": "<merchant name>",
    "category": "<category>",
    "amount": <amount as number>,
    "currency": "<currency code>",
    "date": "<YYYY-MM-DD>",
    "recurring": <true or false>
  }
]
"""


def _build_user_prompt(
    transactions: list[dict[str, Any]],
    existing_categories: list[str],
) -> str:
    """Build the user-part of the prompt with transactions + categories."""
    cats_str = ", ".join(f'"{c}"' for c in existing_categories) if existing_categories else "(nessuna ancora)"

    lines = [
        f"CATEGORIE ESISTENTI: [{cats_str}]",
        "",
        "TRANSAZIONI DA CLASSIFICARE:",
    ]
    for t in transactions:
        lines.append(
            f'- descrizione: "{t["raw_description"]}", '
            f'importo: {t["amount"]}, valuta: {t["currency"]}, data: {t["date"]}'
        )

    return "\n".join(lines)


# ── Provider implementations ─────────────────────────────────────


def _call_gemini(
    user_prompt: str,
    api_key: str,
    model: str,
) -> str:
    """Call Google Gemini and return the raw text response."""
    import google.generativeai as genai  # type: ignore[import-untyped]

    genai.configure(api_key=api_key)
    llm = genai.GenerativeModel(
        model_name=model,
        system_instruction=_SYSTEM_PROMPT,
    )
    response = llm.generate_content(user_prompt)
    return response.text


def _call_openai(
    user_prompt: str,
    api_key: str,
    model: str,
) -> str:
    """Call OpenAI-compatible API and return the raw text response."""
    from openai import OpenAI

    client = OpenAI(api_key=api_key)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
    )
    return response.choices[0].message.content or "[]"


# ── Public API ───────────────────────────────────────────────────


def _extract_json(text: str) -> list[dict[str, Any]]:
    """Robustly extract a JSON array from LLM output."""
    text = text.strip()
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            # Look for any key whose value is a list (e.g. "transactions", "data", etc.)
            for v in parsed.values():
                if isinstance(v, list):
                    return v
            # Single object — wrap in list
            return [parsed]
    except json.JSONDecodeError:
        pass

    # Try to find a JSON array buried in prose/markdown
    match = re.search(r"\[.*\]", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    logger.error("Failed to parse LLM response as JSON: %s", text[:300])
    return []


def categorise(
    transactions: list[dict[str, Any]],
    existing_categories: list[str],
    *,
    provider: str,
    api_key: str,
    model: str,
) -> list[CategorisedTransaction]:
    """Send transactions to the LLM and return structured results.

    Parameters
    ----------
    transactions:
        List of dicts with keys: raw_description, amount, currency, date
    existing_categories:
        Categories already present in the spreadsheet.
    provider:
        "gemini" or "openai"
    api_key:
        The API key for the chosen provider.
    model:
        Model name/ID to use.
    """
    if not transactions:
        logger.info("No transactions to categorise")
        return []

    user_prompt = _build_user_prompt(transactions, existing_categories)
    logger.info(
        "Sending %d transaction(s) to %s (%s)", len(transactions), provider, model
    )
    logger.debug("Prompt:\n%s", user_prompt)

    # Call the provider
    if provider == "gemini":
        raw = _call_gemini(user_prompt, api_key, model)
    elif provider == "openai":
        raw = _call_openai(user_prompt, api_key, model)
    else:
        raise ValueError(f"Unknown AI provider: {provider!r}")

    logger.debug("Raw LLM response:\n%s", raw)

    # Parse JSON
    items = _extract_json(raw)
    logger.info("LLM returned %d categorised item(s)", len(items))

    results: list[CategorisedTransaction] = []
    for item in items:
        try:
            results.append(
                CategorisedTransaction(
                    original_description=item.get("original", ""),
                    clean_name=item.get("clean_name", ""),
                    category=item.get("category", "Other"),
                    amount=float(item.get("amount", 0)),
                    currency=item.get("currency", "EUR"),
                    date=item.get("date", ""),
                    recurring=bool(item.get("recurring", False)),
                )
            )
        except Exception:
            logger.warning("Skipping malformed LLM item: %s", item)

    return results
