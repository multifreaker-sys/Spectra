"""Offline local categoriser — merchant memory + fuzzy match + ML classifier."""

from __future__ import annotations

import logging
import re
from typing import Any

from spectra.ai import CategorisedTransaction

logger = logging.getLogger("spectra.local")


# ── Merchant name extraction ─────────────────────────────────────

_NAME_FIELD_RE = re.compile(
    r"(?i)\bNaam:\s*(?P<name>.+?)(?=(?:\s*\|\s*|\s+(?:Omschrijving|IBAN|Kenmerk|Machtiging ID|Incassant ID|Datum/Tijd|Valutadatum|Overige partij):)|$)"
)
_TRAILING_METHOD_RE = re.compile(
    r"(?i)\b(?:incasso|overschrijving|overboeking|online bankieren|betaalautomaat|kaartbetaling|ideal|iDEAL|diversen)\b$"
)

_STRIP_PREFIXES = re.compile(
    r"(?i)^(?:"
    # Italian
    r"POS\s*\d*\s*|"
    r"ADDEBITO\s+SDD\s+|"
    r"ADDEBITO\s+DIRETTO\s+|"
    r"PAGAMENTO\s+(?:SU\s+POS\s+(?:ESTERO\s+)?)?|"
    r"PRELIEVO\s+(?:BANCOMAT\s+)?|"
    r"COMMISSIONE\s+|"
    r"CANONE\s+|"
    r"ACCREDITO\s+|"
    r"BONIFICO\s+(?:ISTANTANEO\s+)?(?:DA\s+VOI\s+DISPOSTO\s+)?A\s+FAVORE\s+DI\s+|"
    r"BONIFICO\s+(?:ISTANTANEO\s+)?A\s+VOSTRO\s+FAVORE\s+DISPOSTO\s+DA\s+|"
    r"RICARICA\s+(?:TELEFONICA\s+)?|"
    # English / UK / US / international
    r"CARD\s+PAYMENT\s+(?:TO\s+)?|"
    r"DIRECT\s+DEBIT\s+|"
    r"CONTACTLESS\s+(?:PAYMENT\s+)?|"
    r"FASTER\s+PAYMENT\s+(?:TO\s+)?|"
    r"STANDING\s+ORDER\s+(?:TO\s+)?|"
    r"BANK\s+TRANSFER\s+(?:TO\s+)?|"
    r"PURCHASE\s+AT\s+|"
    r"DEBIT\s+CARD\s+|"
    r"ACH\s+(?:DEBIT\s+|PAYMENT\s+)?|"
    # German
    r"Kartenzahlung\s+|"
    r"Lastschrift\s+|"
    r"\u00dcberweisung\s+(?:an\s+)?|"
    r"Dauerauftrag\s+(?:an\s+)?|"
    r"Geldautomat\s+|"
    # French
    r"Paiement\s+(?:par\s+carte\s+)?(?:CB\s+)?|"
    r"Pr\u00e9l\u00e8vement\s+(?:SEPA\s+)?|"
    r"Virement\s+(?:SEPA\s+)?|"
    r"Retrait\s+(?:DAB\s+)?|"
    # Spanish / Portuguese
    r"Pago\s+(?:con\s+tarjeta\s+)?|"
    r"Transferencia\s+(?:SEPA\s+)?|"
    r"Domiciliaci\u00f3n\s+"
    r")"
)

_STRIP_SUFFIXES = re.compile(
    r"(?i)(?:"
    r"\s+(?:IT|DE|FR|ES|NL|GB|US|EU|COM|SPA|SRL|SARL|GMBH|LTD|INC|AG|AB|BV|SA|PLC|NV|KG|OHG|SAS|EURL|OY|AS|PTY|SLU)\s*$|"
    r"\s+\d{2}/\d{2}/\d{2,4}$|"
    r"\s+CARTA\s.*$|"
    r"\s+ATM\s+\d+.*$|"
    r"\s+VIA\s+.*$"
    r")"
)

_STRIP_NOISE = re.compile(
    r"(?i)(?:"
    r"\bCarta\s+n\.?\s*\d*[\*X]+[\d\*X\s]+\b|"
    r"\b[A-Z0-9]{15,}\b|"
    r"\bABI\s+\d+\b|"
    r"\bCAB\s+\d+\b|"
    r"\bCOD\.?\s*\d+/?\d*\b|"
    r"\(\s*ctv\..*?\)|"
    r"\b\d{2}[/.]\d{2}[/.]\d{2,4}\b|"
    r"\b\d{4,}\b|"
    r"\*+"        # bare asterisks (e.g. "Massaua Ci*")
    r")"
)

# ── Deterministic merchant hints (high precision) ───────────────
#
# Applied before ML, after exact/fuzzy merchant memory.
# Keep these focused to avoid broad false positives.
_KEYWORD_CATEGORY_HINTS: list[tuple[tuple[str, ...], str]] = [
    # Taxes / government
    ((
        "BELASTINGDIENST",
        "GBTWENTE",
        "GBLT",
        "LANDKREIS GRAFSCHAFT BENTHEIM",
        "SAMTGEMEINDE UELSEN",
        "GEMEENTE HENGELO",
        "BNG GEMEENTE ALMELO",
    ), "Taxes"),
    # Utilities / telecom
    ((
        "VITENS",
        "DELTA ENERGIE",
        "E.ON ENERGIE",
        "COOLBLUE ENERGIE",
        "KPN B.V",
        "KPN - MOBIEL",
        "GRAFSCHAFTER BREITBAND",
    ), "Utilities"),
    # Insurance
    ((
        "VGZ",
        "NATIONALE-NEDERLANDEN",
        "NN SCHADEVERZEKERING",
        "VOOGD",
        "MONUTA VERZEKERINGEN",
        "UNIVE OOST",
    ), "Insurance"),
    # Housing / debt / bank
    (("ING HYPOTHEKEN", "VERENIGING VAN EIGENAARS"), "Housing"),
    (("DEFAM", "KLARNA BANK"), "Debt Repayment"),
    (("KOSTEN BETAALPAKKET",), "Bank Fees"),
    # Transfers / known person-to-person counterparties
    ((
        "ORANJE SPAARREKENING",
        "EEI KAMP",
        "HERRN E. KAMP",
        "M.P.S. KOOIJ",
        "ALBERT ROTTERINK",
        "AAB INZ TIKKIE",
        "INTERNATIONAL CARD SERVICES",
        "ISAI",
    ), "Transfer"),
    # Groceries
    ((
        "PLUS ",
        "AH ",
        "ALBERT HEIJN",
        "JUMBO ",
        "WELKOOP",
    ), "Groceries"),
    # Transport
    ((
        "FREIE TANKST.",
        "RAIFFEISEN EMS VECHTE",
        "TINQ",
        "TANGO ",
        "YELLOWBRICK",
    ), "Transport"),
    # Health
    (("ETOS", "KRUIDVAT"), "Health"),
    # Shopping
    ((
        "BOL.COM",
        "BOLCOM",
        "ABOUT YOU",
        "CROCS.EU",
        "OTTO PAYMENTS",
        "HEMA ",
        "ACTION ",
    ), "Shopping"),
    # Entertainment / leisure
    (("MONKEY TOWN", "AVONTURENPARK", "MUSKETIERS"), "Entertainment"),
]


def _keyword_category_hint(raw: str, clean_name: str) -> str | None:
    """Return a deterministic category hint for known merchants, if any."""
    text = f"{raw} {clean_name}".upper()
    for keywords, category in _KEYWORD_CATEGORY_HINTS:
        if any(keyword in text for keyword in keywords):
            return category
    return None


def _extract_merchant_name(raw: str) -> str:
    """Extract a clean merchant name from a raw banking description."""
    text = raw.strip()

    # Prefer explicit "Naam: ..." metadata when available.
    if name_match := _NAME_FIELD_RE.search(text):
        text = str(name_match.group("name") or "").strip()
    elif "|" in text:
        # Metadata-enriched descriptions use `|` separators; the first segment is
        # usually the best merchant candidate.
        text = text.split("|", 1)[0].strip()

    # Strip banking prefixes
    text = _STRIP_PREFIXES.sub("", text)

    # Strip noise (card masks, trace IDs, dates, long numbers)
    text = _STRIP_NOISE.sub(" ", text)

    # Strip suffixes (country codes, legal forms, trailing dates)
    text = _STRIP_SUFFIXES.sub("", text)

    # Collapse whitespace and pipes
    text = re.sub(r"\s*\|\s*", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" .|,;:-*")
    text = _TRAILING_METHOD_RE.sub("", text).strip(" .|,;:-*")

    # Deduplicate repeated words (e.g. "Massaua Ci Massaua Ci Torino" → "Massaua Ci Torino")
    words = text.split()
    seen: list[str] = []
    for w in words:
        if w.lower() not in [s.lower() for s in seen]:
            seen.append(w)
    text = " ".join(seen)

    if not text:
        return raw.strip()

    # Title-case for readability
    return text.title()


# ── Fuzzy matching ───────────────────────────────────────────────


def _fuzzy_match(
    name: str,
    known_merchants: dict[str, str],
    threshold: int = 75,
) -> tuple[str, str] | None:
    """Find the closest known merchant using fuzzy string matching.

    Returns (matched_merchant_name, category) or None.
    """
    if not known_merchants or not name:
        return None

    try:
        from rapidfuzz import fuzz
    except ImportError:
        logger.debug("rapidfuzz not installed — skipping fuzzy match")
        return None

    best_score = 0
    best_merchant = ""
    best_category = ""

    for merchant, category in known_merchants.items():
        score = fuzz.token_sort_ratio(name.lower(), merchant.lower())
        if score > best_score:
            best_score = score
            best_merchant = merchant
            best_category = category

    if best_score >= threshold:
        logger.debug("Fuzzy match: %r → %r (score=%d)", name, best_merchant, best_score)
        return best_merchant, best_category

    return None


# ── Public API ───────────────────────────────────────────────────


def categorise_local(
    transactions: list[dict[str, Any]],
    merchant_db: dict[str, str],
    ml_classifier: Any | None = None,
) -> list[CategorisedTransaction]:
    """Categorise transactions using merchant memory + ML classifier.

    Cascade order:
    1. Exact merchant memory (clean_name lookup)
    2. Fuzzy match against known merchants
    3. Deterministic keyword hints (high-confidence merchants)
    4. ML classifier (always active — bootstrapped with seed data)
    5. Fallback → "Uncategorized"

    Parameters
    ----------
    transactions:
        List of dicts with keys: raw_description, amount, currency, date
    merchant_db:
        Dict of {clean_name: category} from the local DB.
    ml_classifier:
        Trained sklearn Pipeline (from ml_classifier.train_classifier).

    Returns
    -------
    List of CategorisedTransaction objects.
    """
    import hashlib

    if not transactions:
        logger.info("No transactions to categorise")
        return []

    logger.info("Categorising %d transaction(s) locally", len(transactions))

    results: list[CategorisedTransaction] = []
    stats = {"exact": 0, "fuzzy": 0, "rules": 0, "ml": 0, "fallback": 0}

    for t in transactions:
        raw = t["raw_description"]
        amount = float(t.get("amount", 0))
        currency = t.get("currency", "EUR")
        date = t.get("date", "")

        # Generate ID
        raw_id = f"{date}:{raw}:{amount}"
        txn_id = hashlib.sha1(raw_id.encode("utf-8")).hexdigest()

        # Step 1: Extract clean merchant name
        clean_name = _extract_merchant_name(raw)

        # Step 2: Exact merchant memory
        if clean_name in merchant_db:
            category = merchant_db[clean_name]
            stats["exact"] += 1
            logger.debug("Exact match: %r → %s", clean_name, category)

        # Step 3: Fuzzy match against known merchants
        elif (fuzzy_result := _fuzzy_match(clean_name, merchant_db)):
            matched_name, category = fuzzy_result
            clean_name = matched_name  # Use the canonical name
            stats["fuzzy"] += 1

        # Step 4: Deterministic keyword hints for high-signal merchants
        elif (hint_category := _keyword_category_hint(raw, clean_name)):
            category = hint_category
            stats["rules"] += 1

        # Step 5: ML classifier (always active thanks to seed data)
        elif ml_classifier is not None:
            try:
                from spectra.ml_classifier import predict
                pred_cat, confidence = predict(ml_classifier, raw)
                if confidence >= 0.20:
                    category = pred_cat
                    stats["ml"] += 1
                    logger.debug("ML: %r → %s (%.0f%%)", raw[:50], category, confidence * 100)
                else:
                    category = "Uncategorized"
                    stats["fallback"] += 1
                    logger.debug("ML low confidence for %r (%.0f%%) → Uncategorized", raw[:50], confidence * 100)
            except Exception:
                category = "Uncategorized"
                stats["fallback"] += 1

        # Step 6: Fallback
        else:
            category = "Uncategorized"
            stats["fallback"] += 1

        # Income override: positive amounts not already categorised as income/transfer
        _INCOME_CATS = {
            "Salary", "Pension", "Transfer In", "Transfer",
            "Cash Deposit", "Other Income", "Investment Return",
            "Reimbursement",
        }
        if amount > 0 and category not in _INCOME_CATS and category != "Uncategorized":
            category = "Other Income"

        recurring = ""
        results.append(
            CategorisedTransaction(
                id=txn_id,
                original_description=raw,
                clean_name=clean_name,
                category=category,
                amount=amount,
                currency=currency,
                date=date,
                recurring=recurring,
                original_amount=None,
                original_currency=None,
            )
        )

    logger.info(
        "Local categorisation: %d exact, %d fuzzy, %d rules, %d ML, %d fallback",
        stats["exact"], stats["fuzzy"], stats["rules"], stats["ml"], stats["fallback"],
    )

    return results
