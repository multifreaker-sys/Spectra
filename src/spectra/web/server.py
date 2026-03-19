"""Spectra Web Dashboard — FastAPI backend."""

from __future__ import annotations

import logging
import re
import secrets
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Query, Request, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from spectra.config import load_settings
from spectra.cycles import (
    DEFAULT_CYCLE_START_DAY,
    MAX_CYCLE_START_DAY,
    cycle_start_for,
    cycle_window_for,
    format_cycle_label,
    next_cycle_start,
    normalize_cycle_start_day,
    parse_iso_date,
)
from spectra.db import BookmarkDB
from spectra.error_watcher import (
    REQUEST_ID_HEADER,
    build_error_payload,
    capture_error_event,
    get_request_id,
    init_error_monitoring,
    resolve_language,
)
from spectra.recurring import detect_recurring_kind
from spectra.rules import VALID_RULE_TYPES, normalize_rule_type

logger = logging.getLogger("spectra.web")

_HERE = Path(__file__).parent
_TEMPLATES = _HERE / "templates"
_STATIC = _HERE / "static"

app = FastAPI(title="Spectra Dashboard", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=str(_STATIC)), name="static")
templates = Jinja2Templates(directory=str(_TEMPLATES))

_THEME_SETTING_KEY = "theme_preference"
_PAY_DAY_SETTING_KEY = "cycle_start_day"
_LANGUAGE_SETTING_KEY = "language_preference"
_VALID_THEME_PREFERENCES = {"auto", "light", "dark"}
_VALID_LANGUAGE_PREFERENCES = {"auto", "en", "nl"}
_VALID_SUMMARY_SCOPES = {"cycle", "90d", "ytd"}
_DATE_TIME_WITH_SEPARATOR = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})[T\s](?P<time>\d{2}:\d{2})(?::\d{2})?"
)
_TIME_IN_TEXT = re.compile(r"\b(?P<h>[01]?\d|2[0-3])[:.](?P<m>[0-5]\d)\b")
_VALUE_DATE_RE = re.compile(r"\bValutadatum:\s*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})", re.IGNORECASE)
_DATUM_TIJD_RE = re.compile(
    r"\bDatum/Tijd:\s*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})\s+(\d{1,2}:\d{2}(?::\d{2})?)",
    re.IGNORECASE,
)
_IBAN_RE = re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{10,30}\b")
_KNOWN_METADATA_LABELS = [
    "Naam",
    "Omschrijving",
    "IBAN",
    "Rekening",
    "Rekeningnummer",
    "Rekening nr",
    "Tegenrekening",
    "Tegenrekeningnummer",
    "Tegenrekening nr",
    "Code",
    "Mutatiesoort",
    "Mededelingen",
    "Saldo na mutatie",
    "Tag",
    "BIC",
    "Datum/Tijd",
    "Valutadatum",
    "Kenmerk",
    "Betalingskenmerk",
    "Machtigingskenmerk",
    "Machtiging ID",
    "Incassant ID",
    "Overige partij",
    "Pasvolgnr",
    "Transactie",
    "Term",
]
_LABEL_CAPTURE_RE = re.compile(
    r"(?P<label>"
    + "|".join(re.escape(label) for label in _KNOWN_METADATA_LABELS)
    + r"):\s*(?P<value>.*?)(?=(?:\s*\|\s*|\s+)(?:"
    + "|".join(re.escape(label) for label in _KNOWN_METADATA_LABELS)
    + r"):|$)",
    re.IGNORECASE | re.DOTALL,
)
_GENERIC_MERCHANT_NAMES = {
    "incasso",
    "overschrijving",
    "overboeking",
    "bankoverschrijving",
    "online bankieren",
    "betaalautomaat",
    "kaartbetaling",
    "betaling",
    "ideal",
    "i deal",
    "diversen",
    "sepa",
    "debit",
    "credit",
    "transfer",
    "betaling pin",
    "pin",
}


# ── Global error handler ─────────────────────────────────────────


from fastapi.responses import JSONResponse as _JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException


@app.on_event("startup")
async def _startup_event() -> None:
    settings = load_settings()
    init_error_monitoring(settings)


@app.middleware("http")
async def _request_id_middleware(request: Request, call_next):
    request_id = request.headers.get(REQUEST_ID_HEADER) or secrets.token_hex(8)
    request.state.request_id = request_id

    response = await call_next(request)
    response.headers[REQUEST_ID_HEADER] = request_id
    return response


def _resolve_request_language(request: Request, language_preference: str | None = None) -> str:
    preference = language_preference
    if preference is None:
        try:
            preference = _load_app_preferences().get("language_preference", "auto")
        except Exception:
            preference = load_settings().default_language
    return resolve_language(preference, request.headers.get("accept-language"))


def _api_error(
    request: Request,
    *,
    status_code: int,
    code: str,
    fallback_message: str | None = None,
    retryable: bool = False,
    severity: str = "warning",
    details: dict[str, Any] | None = None,
    exc: Exception | None = None,
) -> _JSONResponse:
    settings = load_settings()
    language = _resolve_request_language(request)
    request_id = get_request_id(request)
    payload = build_error_payload(
        code=code,
        request_id=request_id,
        language=language,
        retryable=retryable,
        fallback_message=fallback_message,
        details=details if settings.expose_debug_errors else None,
    )
    capture_error_event(
        code=code,
        status_code=status_code,
        message=fallback_message or payload["error"]["message_en"],
        severity=severity,
        retryable=retryable,
        request=request,
        details=details,
        exc=exc,
        settings=settings,
    )
    headers = {REQUEST_ID_HEADER: request_id} if request_id else None
    return _JSONResponse(payload, status_code=status_code, headers=headers)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    status_code = int(getattr(exc, "status_code", 500))
    if status_code == 404:
        code = "resource_not_found"
    elif status_code >= 500:
        code = "http_error"
    else:
        code = "bad_request"

    return _api_error(
        request,
        status_code=status_code,
        code=code,
        fallback_message=str(exc.detail),
        retryable=status_code >= 500,
        severity="error" if status_code >= 500 else "warning",
        details={"detail": str(exc.detail), "type": "http_exception"},
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    request_id = get_request_id(request)
    logger.exception("Unhandled error [%s]: %s", request_id, exc)
    return _api_error(
        request,
        status_code=500,
        code="internal_server_error",
        fallback_message="Unhandled server exception",
        retryable=True,
        severity="critical",
        details={"exception_type": exc.__class__.__name__},
        exc=exc,
    )


def _get_db() -> BookmarkDB:
    settings = load_settings()
    return BookmarkDB(settings.db_path)


def _load_app_preferences(db: BookmarkDB | None = None) -> dict[str, Any]:
    owns_db = db is None
    db = db or _get_db()
    settings = load_settings()
    try:
        raw_theme = (db.get_app_setting(_THEME_SETTING_KEY, "auto") or "auto").strip().lower()
        theme_preference = raw_theme if raw_theme in _VALID_THEME_PREFERENCES else "auto"

        default_language = str(settings.default_language or "auto").strip().lower()
        if default_language not in _VALID_LANGUAGE_PREFERENCES:
            default_language = "auto"
        raw_language = (
            db.get_app_setting(_LANGUAGE_SETTING_KEY, default_language) or default_language
        ).strip().lower()
        language_preference = (
            raw_language if raw_language in _VALID_LANGUAGE_PREFERENCES else default_language
        )

        raw_pay_day = db.get_app_setting(_PAY_DAY_SETTING_KEY, str(DEFAULT_CYCLE_START_DAY))
        try:
            raw_int = int(raw_pay_day or DEFAULT_CYCLE_START_DAY)
            # Clamp legacy values (29-31) into valid range
            clamped = max(1, min(raw_int, MAX_CYCLE_START_DAY))
            pay_day = normalize_cycle_start_day(clamped)
        except (TypeError, ValueError):
            pay_day = DEFAULT_CYCLE_START_DAY
    finally:
        if owns_db:
            db.close()

    return {
        "theme_preference": theme_preference,
        "language_preference": language_preference,
        "pay_day": pay_day,
        # Backward compatibility for older clients.
        "cycle_start_day": pay_day,
    }


def _build_cycle_payload(pay_day: int):
    from datetime import date, timedelta

    cycle_start, cycle_end = cycle_window_for(date.today(), pay_day)
    return {
        "start": cycle_start.isoformat(),
        "end": cycle_end.isoformat(),
        "label": format_cycle_label(cycle_start, cycle_end),
        "pay_day": pay_day,
        # Backward compatibility.
        "start_day": pay_day,
    }


def _build_cycle_burn_rate(*, today, period_start, period_end, total_spent: float) -> dict[str, Any]:
    """Estimate end-of-cycle spend based on the current daily burn."""
    total_days = max((period_end - period_start).days, 1)
    elapsed_days = min(max((today - period_start).days + 1, 1), total_days)
    remaining_days = max(total_days - elapsed_days, 0)
    daily_spend = total_spent / elapsed_days
    projected_total = daily_spend * total_days

    return {
        "elapsed_days": elapsed_days,
        "total_days": total_days,
        "remaining_days": remaining_days,
        "spent_so_far": round(total_spent, 2),
        "daily_spend": round(daily_spend, 2),
        "projected_total": round(projected_total, 2),
    }


def _template_context(request: Request) -> dict[str, Any]:
    preferences = _load_app_preferences()
    effective_language = _resolve_request_language(
        request,
        preferences.get("language_preference", "auto"),
    )
    return {
        "request": request,
        "language_code": effective_language,
        "effective_language": effective_language,
        **preferences,
    }


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return default


def _extract_booking_time(date_value: str, original_description: str = "") -> str:
    """Extract HH:MM from date-time strings or description text if available."""
    raw_date = str(date_value or "").strip()
    if raw_date:
        dt_match = _DATE_TIME_WITH_SEPARATOR.match(raw_date)
        if dt_match:
            return str(dt_match.group("time"))

    raw_description = str(original_description or "")
    match = _TIME_IN_TEXT.search(raw_description)
    if not match:
        return ""

    hour = int(match.group("h"))
    minute = int(match.group("m"))
    return f"{hour:02d}:{minute:02d}"


def _extract_value_date(original_description: str) -> str:
    text = str(original_description or "")
    match = _VALUE_DATE_RE.search(text)
    if not match:
        return ""

    raw_date = str(match.group(1))
    from datetime import datetime

    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw_date, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw_date


def _normalize_label(label: str) -> str:
    canonical = {
        "naam": "Naam",
        "omschrijving": "Omschrijving",
        "iban": "IBAN",
        "rekening": "Rekening",
        "rekeningnummer": "Rekening",
        "rekening nr": "Rekening",
        "tegenrekening": "Tegenrekening",
        "tegenrekeningnummer": "Tegenrekening",
        "tegenrekening nr": "Tegenrekening",
        "code": "Code",
        "mutatiesoort": "Mutatiesoort",
        "mededelingen": "Mededelingen",
        "saldo na mutatie": "Saldo na mutatie",
        "tag": "Tag",
        "bic": "BIC",
        "datum/tijd": "Datum/Tijd",
        "valutadatum": "Valutadatum",
        "kenmerk": "Kenmerk",
        "betalingskenmerk": "Betalingskenmerk",
        "machtigingskenmerk": "Machtigingskenmerk",
        "machtiging id": "Machtiging ID",
        "incassant id": "Incassant ID",
        "overige partij": "Overige partij",
        "pasvolgnr": "Pasvolgnr",
        "transactie": "Transactie",
        "term": "Term",
    }
    normalized = str(label or "").strip().lower()
    return canonical.get(normalized, str(label).strip())


def _extract_structured_fields(original_description: str) -> dict[str, str]:
    text = str(original_description or "")
    fields: dict[str, str] = {}
    for match in _LABEL_CAPTURE_RE.finditer(text):
        label = _normalize_label(match.group("label"))
        value = re.sub(r"\s+", " ", str(match.group("value") or "")).strip()
        if not value:
            continue
        fields[label] = value
    return fields


def _infer_payment_method(original_description: str) -> str:
    text = str(original_description or "").lower()
    if "betaalautomaat" in text or "apple pay" in text or "google pay" in text:
        return "Kaartbetaling"
    if "ideal" in text:
        return "iDEAL"
    if "incasso" in text:
        return "Incasso"
    if "overschrijving" in text:
        return "Overschrijving"
    if "online bankieren" in text:
        return "Online bankieren"
    if "diversen" in text:
        return "Diversen"
    return ""


def _field_value(fields: dict[str, str], *labels: str) -> str:
    for label in labels:
        value = str(fields.get(label, "")).strip()
        if value:
            return value
    return ""


def _normalize_match_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _resolve_display_merchant(clean_name: str, details: dict[str, Any]) -> str:
    merchant = str(clean_name or "").strip()
    counterparty = str(details.get("counterparty") or "").strip()
    payment_method = _normalize_match_text(str(details.get("payment_method") or ""))
    if not merchant:
        return counterparty
    if not counterparty:
        return merchant

    merchant_norm = _normalize_match_text(merchant)
    counterparty_norm = _normalize_match_text(counterparty)
    if (
        merchant_norm in _GENERIC_MERCHANT_NAMES
        and counterparty_norm not in _GENERIC_MERCHANT_NAMES
    ):
        return counterparty

    if payment_method:
        if merchant_norm in {
            f"{counterparty_norm} {payment_method}".strip(),
            f"{payment_method} {counterparty_norm}".strip(),
        }:
            return counterparty

    return merchant


def _extract_counterparty(
    original_description: str,
    payment_method: str,
    structured_fields: dict[str, str],
) -> str:
    direct = _field_value(structured_fields, "Naam", "Overige partij")
    if direct:
        return direct

    text = str(original_description or "").strip()
    if not text:
        return ""

    # Keep the first description chunk; metadata key-value pairs often follow after "|".
    head = text.split("|", 1)[0].strip()
    if not head:
        head = text

    # Remove known payment-method markers when they are embedded in the raw description.
    tokens = [
        payment_method,
        "Incasso",
        "Overschrijving",
        "Online bankieren",
        "iDEAL",
        "Betaalautomaat",
        "Kaartbetaling",
        "Diversen",
    ]
    for token in tokens:
        token = str(token or "").strip()
        if not token:
            continue
        head = re.sub(rf"\b{re.escape(token)}\b", "", head, flags=re.IGNORECASE)

    # Remove IBAN-like tokens from the counterparty guess.
    for iban in _IBAN_RE.findall(head.upper()):
        head = re.sub(re.escape(iban), "", head, flags=re.IGNORECASE)

    head = re.sub(r"\s+", " ", head).strip(" -|,")
    if not head:
        return ""
    if payment_method and head.lower() == payment_method.lower():
        return ""
    return head


def _extract_tx_details(
    *,
    date_value: str,
    original_description: str,
) -> dict[str, Any]:
    booking_time = _extract_booking_time(date_value, original_description)
    value_date = _extract_value_date(original_description)
    payment_method = _infer_payment_method(original_description)
    structured_fields = _extract_structured_fields(original_description)
    ibans_raw = _IBAN_RE.findall(str(original_description or ""))
    ibans: list[str] = []
    seen_ibans: set[str] = set()
    for iban in ibans_raw:
        normalized = str(iban or "").strip().upper()
        if not normalized or normalized in seen_ibans:
            continue
        seen_ibans.add(normalized)
        ibans.append(normalized)
    counterparty = _extract_counterparty(original_description, payment_method, structured_fields)
    account = _field_value(structured_fields, "Rekening")
    counter_account = _field_value(structured_fields, "Tegenrekening")
    reference = _field_value(
        structured_fields,
        "Kenmerk",
        "Betalingskenmerk",
        "Machtigingskenmerk",
    )
    transaction_code = _field_value(structured_fields, "Code")
    mutation_type = _field_value(structured_fields, "Mutatiesoort", "Transactie")
    message = _field_value(structured_fields, "Mededelingen")
    tag = _field_value(structured_fields, "Tag")
    running_balance = _field_value(structured_fields, "Saldo na mutatie")
    bic = _field_value(structured_fields, "BIC")

    # Fill missing account fields from IBAN sequence when labels are absent.
    if not account and ibans:
        if counter_account and len(ibans) >= 2:
            for iban in ibans:
                if iban != counter_account:
                    account = iban
                    break
        elif not counter_account:
            account = ibans[0]

    if not counter_account and ibans:
        if account and len(ibans) >= 2:
            for iban in ibans:
                if iban != account:
                    counter_account = iban
                    break
        elif not account and ibans:
            counter_account = ibans[0]

    return {
        "booking_time": booking_time,
        "value_date": value_date,
        "payment_method": payment_method,
        "counterparty": counterparty,
        "account": account,
        "counter_account": counter_account,
        "reference": reference,
        "transaction_code": transaction_code,
        "mutation_type": mutation_type,
        "message": message,
        "tag": tag,
        "running_balance": running_balance,
        "bic": bic,
        "iban_candidates": ibans[:3],
        "structured_fields": structured_fields,
        "original_description": str(original_description or ""),
    }


def _persist_learning(
    db: BookmarkDB,
    *,
    tx_id: str | None,
    original_description: str,
    clean_name: str,
    category: str,
    source: str,
    apply_to_future: bool,
) -> None:
    normalized_name = str(clean_name or "").strip()
    normalized_category = str(category or "").strip()
    normalized_original = str(original_description or "")
    if not normalized_name or not normalized_category or normalized_category == "Uncategorized":
        return

    if apply_to_future:
        db.save_merchant_category(normalized_name, normalized_category)
        if normalized_original:
            db.save_overrides(
                {
                    normalized_original: {
                        "clean_name": normalized_name,
                        "category": normalized_category,
                    }
                }
            )

    db.record_learning_feedback(
        tx_id=tx_id,
        original_description=normalized_original,
        clean_name=normalized_name,
        category=normalized_category,
        source=source,
        apply_to_future=apply_to_future,
    )


def _simulate_rule_impact(
    db: BookmarkDB,
    *,
    rule_type: str,
    pattern: str,
    sample_text: str = "",
) -> dict[str, Any]:
    from spectra.rules import match_rule

    candidate_rule = {
        "rule_type": rule_type,
        "pattern": pattern,
        "is_active": True,
    }
    rows = db._conn.execute(
        """
        SELECT tx_id, date, clean_name, original_description, category
        FROM tx_history
        ORDER BY date DESC, tx_id DESC
        """
    ).fetchall()

    examples: list[dict[str, Any]] = []
    impact_count = 0
    for tx_id, date_str, clean_name, original_description, category in rows:
        if not match_rule(
            candidate_rule,
            clean_name=str(clean_name),
            raw_description=str(original_description or clean_name),
        ):
            continue

        impact_count += 1
        if len(examples) < 5:
            examples.append(
                {
                    "tx_id": str(tx_id),
                    "date": str(date_str),
                    "merchant": str(clean_name),
                    "original_description": str(original_description or ""),
                    "current_category": str(category),
                }
            )

    matches_sample = False
    if sample_text:
        matches_sample = match_rule(
            candidate_rule,
            clean_name=sample_text,
            raw_description=sample_text,
        )

    return {
        "matches_sample": matches_sample,
        "impact_count": impact_count,
        "examples": examples,
    }


def _build_summary_insights(
    *,
    scope: str,
    rows: list[tuple[str, str, float, str]],
    period_start,
    period_end,
    pay_day: int,
    total_spent: float,
    uncategorized: int,
    burn_rate: dict[str, Any] | None,
    budget_limits: dict[str, float],
) -> list[dict[str, str]]:
    from collections import defaultdict
    from datetime import timedelta
    from statistics import median

    def fmt(amount: float) -> str:
        return f"EUR {amount:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")

    insights: list[dict[str, str]] = []

    if scope == "cycle" and burn_rate:
        total_budget = round(sum(limit for limit in budget_limits.values() if limit and limit > 0), 2)
        if total_budget > 0:
            projected_total = float(burn_rate.get("projected_total", 0.0))
            if projected_total > total_budget:
                insights.append(
                    {
                        "type": "budget_risk",
                        "severity": "warning",
                        "title": "Projected cycle spend is above your current budget",
                        "detail": f"Projected {fmt(projected_total)} vs configured budget {fmt(total_budget)}.",
                        "href": "/budget",
                    }
                )
            elif projected_total > total_budget * 0.9:
                insights.append(
                    {
                        "type": "budget_watch",
                        "severity": "info",
                        "title": "Cycle spend is close to the configured budget",
                        "detail": f"Projected {fmt(projected_total)} against budget {fmt(total_budget)}.",
                        "href": "/budget",
                    }
                )

    if uncategorized > 0:
        insights.append(
            {
                "type": "uncategorized",
                "severity": "warning",
                "title": "Some transactions still need a category",
                "detail": f"{uncategorized} transaction(s) are still uncategorized in the selected period.",
                "href": "/transactions",
            }
        )

    if scope != "cycle":
        return insights[:4]

    current_by_category: dict[str, float] = defaultdict(float)
    previous_by_category: dict[str, float] = defaultdict(float)
    history_by_merchant: dict[str, list[float]] = defaultdict(list)
    current_expenses: list[tuple[str, float, str]] = []
    subscription_by_merchant: dict[str, list[tuple[object, float]]] = defaultdict(list)

    previous_cycle_start = cycle_start_for(period_start - timedelta(days=1), pay_day)
    previous_cycle_end = period_start

    for tx_date_str, clean_name, amount, category in rows:
        tx_date = parse_iso_date(tx_date_str)
        if amount >= 0:
            continue

        spend = abs(float(amount))
        merchant = str(clean_name)
        category_name = str(category)

        if tx_date < period_start:
            history_by_merchant[merchant].append(spend)
        if previous_cycle_start <= tx_date < previous_cycle_end:
            previous_by_category[category_name] += spend
        if period_start <= tx_date < period_end:
            current_expenses.append((merchant, spend, category_name))
            current_by_category[category_name] += spend

        if category_name == "Digital Subscriptions":
            subscription_by_merchant[merchant].append((tx_date, spend))

    biggest_delta = None
    for category_name, current_total in current_by_category.items():
        previous_total = previous_by_category.get(category_name, 0.0)
        delta = current_total - previous_total
        if delta <= max(25.0, previous_total * 0.25):
            continue
        if biggest_delta is None or delta > biggest_delta[1]:
            biggest_delta = (category_name, delta, previous_total, current_total)

    if biggest_delta:
        category_name, delta, previous_total, current_total = biggest_delta
        previous_text = fmt(previous_total) if previous_total else "EUR 0,00"
        insights.append(
            {
                "type": "category_delta",
                "severity": "info",
                "title": f"{category_name} is the main driver this cycle",
                "detail": f"Up by {fmt(delta)} versus the previous cycle ({fmt(current_total)} vs {previous_text}).",
                "href": "/trends",
            }
        )

    anomalies: list[tuple[str, float, float]] = []
    first_time_large: list[tuple[str, float]] = []
    for merchant, spend, _category_name in current_expenses:
        past_amounts = history_by_merchant.get(merchant, [])
        if len(past_amounts) >= 2:
            baseline = float(median(past_amounts))
            if baseline > 0 and spend > baseline * 1.8 and (spend - baseline) >= 20:
                anomalies.append((merchant, spend, baseline))
        elif not past_amounts and spend >= 150:
            first_time_large.append((merchant, spend))

    if anomalies:
        anomalies.sort(key=lambda item: item[1] - item[2], reverse=True)
        merchant, spend, baseline = anomalies[0]
        insights.append(
            {
                "type": "anomaly",
                "severity": "warning",
                "title": f"{len(anomalies)} unusual charge(s) detected",
                "detail": f"{merchant} posted {fmt(spend)} vs a usual baseline near {fmt(baseline)}.",
                "href": "/transactions",
            }
        )
    elif first_time_large:
        first_time_large.sort(key=lambda item: item[1], reverse=True)
        merchant, spend = first_time_large[0]
        insights.append(
            {
                "type": "first_time_large",
                "severity": "info",
                "title": "Large first-time expense found in this cycle",
                "detail": f"{merchant} appears as a new merchant with a {fmt(spend)} charge.",
                "href": "/transactions",
            }
        )

    subscription_changes: list[tuple[str, float, float]] = []
    for merchant, history in subscription_by_merchant.items():
        history.sort(key=lambda item: item[0])
        if len(history) < 2:
            continue
        latest_date, latest_amount = history[-1]
        if not (period_start <= latest_date < period_end):
            continue

        previous_amounts = [amount for _dt, amount in history[:-1]]
        baseline = float(median(previous_amounts)) if previous_amounts else 0.0
        diff = latest_amount - baseline
        if baseline > 0 and abs(diff) >= max(1.0, baseline * 0.08):
            subscription_changes.append((merchant, latest_amount, baseline))

    if subscription_changes:
        subscription_changes.sort(key=lambda item: abs(item[1] - item[2]), reverse=True)
        merchant, latest_amount, baseline = subscription_changes[0]
        direction = "up" if latest_amount > baseline else "down"
        insights.append(
            {
                "type": "subscription_change",
                "severity": "info",
                "title": "A recurring subscription changed price",
                "detail": f"{merchant} moved {direction} to {fmt(latest_amount)} from a prior baseline near {fmt(baseline)}.",
                "href": "/subscriptions",
            }
        )

    return insights[:4]


# ── Pages ────────────────────────────────────────────────────────


@app.get("/", response_class=HTMLResponse)
async def page_dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", _template_context(request))


@app.get("/transactions", response_class=HTMLResponse)
async def page_transactions(request: Request):
    return templates.TemplateResponse("transactions.html", _template_context(request))


@app.get("/upload", response_class=HTMLResponse)
async def page_upload(request: Request):
    return templates.TemplateResponse("upload.html", _template_context(request))


@app.get("/settings", response_class=HTMLResponse)
async def page_settings(request: Request):
    return templates.TemplateResponse("settings.html", _template_context(request))


@app.get("/subscriptions", response_class=HTMLResponse)
async def page_subscriptions(request: Request):
    return templates.TemplateResponse("subscriptions.html", _template_context(request))


# ── API: Dashboard Summary ───────────────────────────────────────


@app.get("/api/summary")
async def api_summary(request: Request, scope: str = Query("cycle")):
    """Return dashboard-level stats."""
    scope = (scope or "cycle").strip().lower()
    if scope not in _VALID_SUMMARY_SCOPES:
        return _api_error(
            request,
            status_code=400,
            code="invalid_scope",
            fallback_message="scope must be one of cycle, 90d, ytd",
            severity="warning",
            details={"scope": scope, "allowed": sorted(_VALID_SUMMARY_SCOPES)},
        )

    preferences = _load_app_preferences()
    pay_day = preferences["pay_day"]

    from datetime import date, timedelta

    today = date.today()
    if scope == "cycle":
        period_start, period_end = cycle_window_for(today, pay_day)
        scope_label = format_cycle_label(period_start, period_end)
    elif scope == "90d":
        period_end = today + timedelta(days=1)
        period_start = period_end - timedelta(days=90)
        scope_label = f"Last 90 days ({format_cycle_label(period_start, period_end)})"
    else:  # ytd
        period_start = date(today.year, 1, 1)
        period_end = today + timedelta(days=1)
        scope_label = f"Year to date ({format_cycle_label(period_start, period_end)})"

    burn_rate = (
        _build_cycle_burn_rate(
            today=today,
            period_start=period_start,
            period_end=period_end,
            total_spent=0.0,
        )
        if scope == "cycle"
        else None
    )

    with _get_db() as db:
        rows = db._conn.execute(
            "SELECT date, clean_name, amount, category FROM tx_history ORDER BY date DESC"
        ).fetchall()
        budget_limits = db.get_budget_limits()

    if not rows:
        return {
            "total_spent": 0, "total_income": 0, "subscriptions": 0,
            "uncategorized": 0, "by_category": {}, "monthly": {},
            "monthly_ranges": {}, "top_merchants": [], "current_cycle": _build_cycle_payload(pay_day),
            "scope": scope, "scope_label": scope_label,
            "selected_period": {"start": period_start.isoformat(), "end": period_end.isoformat(), "label": scope_label},
            "pay_day": pay_day, "cycle_start_day": pay_day, "has_data": False,
            "burn_rate": burn_rate,
            "insights": [],
        }

    from collections import Counter, defaultdict

    total_spent = 0.0
    total_income = 0.0
    subscriptions = 0.0
    uncategorized = 0
    by_category: dict[str, float] = defaultdict(float)
    monthly: dict[str, float] = defaultdict(float)
    monthly_ranges: dict[str, dict[str, str]] = {}
    merchant_totals: Counter = Counter()
    in_scope_count = 0

    def _monthly_bucket(tx_date):
        if scope == "cycle":
            bucket_start = cycle_start_for(tx_date, pay_day)
            bucket_end = next_cycle_start(bucket_start, pay_day)
        else:
            bucket_start = tx_date.replace(day=1)
            if bucket_start.month == 12:
                bucket_end = bucket_start.replace(year=bucket_start.year + 1, month=1)
            else:
                bucket_end = bucket_start.replace(month=bucket_start.month + 1)
        return bucket_start, bucket_end

    for tx_date_str, clean_name, amount, cat in rows:
        tx_date = parse_iso_date(tx_date_str)
        if not (period_start <= tx_date < period_end):
            continue

        in_scope_count += 1

        bucket_start, bucket_end = _monthly_bucket(tx_date)
        month = bucket_start.isoformat()
        monthly_ranges[month] = {"start": bucket_start.isoformat(), "end": bucket_end.isoformat()}

        if amount < 0:
            monthly[month] += abs(amount)
            total_spent += abs(amount)
            by_category[cat] += abs(amount)
            merchant_totals[clean_name] += abs(amount)
        else:
            total_income += amount

        if cat == "Uncategorized":
            uncategorized += 1
        if cat == "Digital Subscriptions":
            subscriptions += abs(amount)

    # Last 6 months
    sorted_months = sorted(monthly.keys())[-6:]
    monthly_data = {m: round(monthly[m], 2) for m in sorted_months}
    monthly_ranges_data = {m: monthly_ranges[m] for m in sorted_months}

    # Top 5
    top5 = [{"name": n, "total": round(t, 2)} for n, t in merchant_totals.most_common(5)]

    if scope == "cycle":
        burn_rate = _build_cycle_burn_rate(
            today=today,
            period_start=period_start,
            period_end=period_end,
            total_spent=total_spent,
        )

    insights = _build_summary_insights(
        scope=scope,
        rows=rows,
        period_start=period_start,
        period_end=period_end,
        pay_day=pay_day,
        total_spent=total_spent,
        uncategorized=uncategorized,
        burn_rate=burn_rate,
        budget_limits=budget_limits,
    )

    return {
        "total_spent": round(total_spent, 2),
        "total_income": round(total_income, 2),
        "subscriptions": round(subscriptions, 2),
        "uncategorized": uncategorized,
        "by_category": {k: round(v, 2) for k, v in sorted(by_category.items(), key=lambda x: -x[1])},
        "monthly": monthly_data,
        "monthly_ranges": monthly_ranges_data,
        "top_merchants": top5,
        "current_cycle": _build_cycle_payload(pay_day),
        "scope": scope,
        "scope_label": scope_label,
        "selected_period": {
            "start": period_start.isoformat(),
            "end": period_end.isoformat(),
            "label": scope_label,
        },
        "pay_day": pay_day,
        "cycle_start_day": pay_day,
        "has_data": in_scope_count > 0,
        "burn_rate": burn_rate,
        "insights": insights,
    }


# ── API: Transactions ────────────────────────────────────────────


@app.get("/api/transactions")
async def api_transactions(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=10, le=200),
    category: str = Query("", alias="category"),
    uncategorized_only: bool = Query(False),
    search: str = Query(""),
    date_from: str = Query(""),
    date_to: str = Query(""),
):
    """Return paginated transactions from history."""
    with _get_db() as db:
        query = """
            SELECT tx_id, date, clean_name, amount, category, original_description
            FROM tx_history
            ORDER BY date DESC
        """
        rows = db._conn.execute(query).fetchall()

    # Build result with categories
    results = []
    for tx_id, date, clean_name, amount, cat, original_description in rows:

        # Filters
        if category and cat.lower() != category.lower():
            continue
        if uncategorized_only and cat != "Uncategorized":
            continue
        if date_from and date < date_from:
            continue
        if date_to and date > date_to:
            continue

        details = _extract_tx_details(
            date_value=str(date),
            original_description=str(original_description or ""),
        )
        merchant = _resolve_display_merchant(str(clean_name or ""), details)
        if search:
            needle = str(search).strip().lower()
            haystack = " ".join(
                [
                    str(clean_name or ""),
                    merchant,
                    str(details.get("counterparty") or ""),
                    str(original_description or ""),
                ]
            ).lower()
            if needle not in haystack:
                continue
        results.append({
            "id": tx_id,
            "date": date,
            "booking_time": str(details.get("booking_time") or ""),
            "merchant": merchant,
            "category": cat,
            "amount": amount,
            "details": details,
        })

    total = len(results)
    start = (page - 1) * per_page
    page_data = results[start : start + per_page]

    return {
        "transactions": page_data,
        "total": total,
        "uncategorized_total": sum(1 for tx in results if tx["category"] == "Uncategorized"),
        "page": page,
        "per_page": per_page,
        "pages": max(1, (total + per_page - 1) // per_page),
    }


@app.patch("/api/transactions/{tx_id}")
async def api_update_transaction(tx_id: str, request: Request):
    """Update merchant name and/or category for a transaction."""
    body = await request.json()
    new_category = body.get("category")
    new_merchant = body.get("merchant")
    apply_to_future = _coerce_bool(body.get("apply_to_future"), True)

    with _get_db() as db:
        # Get current merchant name for this transaction
        row = db._conn.execute(
            "SELECT clean_name, original_description, category FROM tx_history WHERE tx_id = ?",
            (tx_id,),
        ).fetchone()

        if not row:
            return _api_error(
                request,
                status_code=404,
                code="resource_not_found",
                fallback_message="Transaction not found",
                severity="warning",
                details={"tx_id": tx_id},
            )

        old_name = row[0]
        original_description = str(row[1] or "")
        current_category = str(row[2] or "Uncategorized")
        merchant_name = new_merchant or old_name
        category_name = new_category or current_category

        if new_merchant:
            db._conn.execute(
                "UPDATE tx_history SET clean_name = ? WHERE tx_id = ?",
                (merchant_name, tx_id),
            )

        if new_category:
            # Also update the category directly on this transaction
            db._conn.execute(
                "UPDATE tx_history SET category = ? WHERE tx_id = ?",
                (new_category, tx_id),
            )
        db._conn.commit()

        _persist_learning(
            db,
            tx_id=tx_id,
            original_description=original_description,
            clean_name=str(merchant_name),
            category=str(category_name),
            source="manual_edit",
            apply_to_future=apply_to_future,
        )

    return {"ok": True, "id": tx_id}


@app.post("/api/transactions/bulk-category")
async def api_bulk_update_category(request: Request):
    """Apply one category to multiple transactions quickly."""
    body = await request.json()
    ids = body.get("ids") or []
    category = str(body.get("category") or "").strip()
    apply_to_future = _coerce_bool(body.get("apply_to_future"), True)

    if not isinstance(ids, list) or not ids:
        return _api_error(
            request,
            status_code=400,
            code="validation_error",
            fallback_message="ids must be a non-empty list",
            severity="warning",
        )
    if not category:
        return _api_error(
            request,
            status_code=400,
            code="validation_error",
            fallback_message="category is required",
            severity="warning",
        )

    cleaned_ids = [str(v).strip() for v in ids if str(v).strip()]
    if not cleaned_ids:
        return _api_error(
            request,
            status_code=400,
            code="validation_error",
            fallback_message="ids must contain valid transaction IDs",
            severity="warning",
        )

    with _get_db() as db:
        placeholders = ",".join("?" for _ in cleaned_ids)
        merchant_rows = db._conn.execute(
            f"SELECT tx_id, clean_name, original_description FROM tx_history WHERE tx_id IN ({placeholders})",
            cleaned_ids,
        ).fetchall()
        db._conn.execute(
            f"UPDATE tx_history SET category = ? WHERE tx_id IN ({placeholders})",
            [category, *cleaned_ids],
        )
        db._conn.commit()

        for tx_row_id, merchant_name, original_description in merchant_rows:
            _persist_learning(
                db,
                tx_id=str(tx_row_id),
                original_description=str(original_description or ""),
                clean_name=str(merchant_name or ""),
                category=category,
                source="bulk_edit",
                apply_to_future=apply_to_future,
            )

    return {"ok": True, "updated": len(cleaned_ids), "category": category}


# ── API: Categories ──────────────────────────────────────────────


@app.get("/api/categories")
async def api_categories():
    """Return all known categories."""
    from spectra.ml_classifier import default_categories

    seed_categories = set(default_categories())
    with _get_db() as db:
        cats = db._conn.execute(
            "SELECT DISTINCT category FROM tx_history WHERE category != 'Uncategorized' ORDER BY category"
        ).fetchall()
    learned_categories = {str(row[0]) for row in cats if str(row[0]).strip()}
    merged = sorted(seed_categories | learned_categories)
    return {"categories": merged}


# ── API: Settings ────────────────────────────────────────────────


@app.get("/api/settings")
async def api_settings():
    """Return current config for the settings page."""
    settings = load_settings()
    with _get_db() as db:
        preferences = _load_app_preferences(db)
        tx_count = db._conn.execute("SELECT COUNT(*) FROM tx_history").fetchone()[0]
        merchant_count = db._conn.execute("SELECT COUNT(*) FROM merchant_categories").fetchone()[0]
        feedback_count = db._conn.execute("SELECT COUNT(*) FROM learning_feedback").fetchone()[0]
        active_rule_count = db._conn.execute(
            "SELECT COUNT(*) FROM category_rules WHERE is_active = 1"
        ).fetchone()[0]
        cats = db._conn.execute(
            "SELECT DISTINCT category FROM tx_history WHERE category != 'Uncategorized'"
        ).fetchall()

    return {
        "provider": settings.ai_provider,
        "currency": settings.base_currency,
        "default_language": settings.default_language,
        "error_watcher_enabled": settings.error_watcher_enabled,
        "sentry_enabled": bool(settings.sentry_enabled and settings.sentry_dsn),
        "alerts_enabled": bool(settings.error_alert_webhook_url),
        "tx_count": tx_count,
        "merchant_count": merchant_count,
        "feedback_count": feedback_count,
        "active_rule_count": active_rule_count,
        "category_count": len(cats),
        "sheets_connected": bool(settings.spreadsheet_id),
        **preferences,
        "current_cycle": _build_cycle_payload(preferences["pay_day"]),
    }


@app.get("/api/settings/errors")
async def api_error_watcher(
    request: Request,
    window_hours: int = Query(24 * 7, ge=1, le=24 * 30),
    limit: int = Query(25, ge=1, le=200),
):
    """Return watcher summary, trends, and recent error events."""
    settings = load_settings()
    if not settings.error_watcher_enabled:
        return {
            "ok": True,
            "watcher_enabled": False,
            "window_hours": int(window_hours),
            "total_events": 0,
            "unique_requests": 0,
            "server_errors": 0,
            "severities": {},
            "top_codes": [],
            "top_routes": [],
            "timeline": [],
            "recent_events": [],
            "sentry_enabled": bool(settings.sentry_enabled and settings.sentry_dsn),
            "alerts_enabled": bool(settings.error_alert_webhook_url),
        }

    try:
        with _get_db() as db:
            overview = db.get_error_overview(
                window_hours=window_hours,
                recent_limit=limit,
            )
    except Exception as exc:
        return _api_error(
            request,
            status_code=500,
            code="operation_failed",
            fallback_message="Failed to load error watcher data",
            severity="error",
            retryable=True,
            exc=exc,
        )

    return {
        "ok": True,
        "watcher_enabled": True,
        "sentry_enabled": bool(settings.sentry_enabled and settings.sentry_dsn),
        "alerts_enabled": bool(settings.error_alert_webhook_url),
        **overview,
    }


@app.patch("/api/settings/preferences")
async def api_update_preferences(request: Request):
    """Save dashboard-local preferences such as theme and cycle start day."""
    body = await request.json()
    updates: dict[str, str] = {}

    if "theme_preference" in body:
        theme_preference = str(body.get("theme_preference", "")).strip().lower()
        if theme_preference not in _VALID_THEME_PREFERENCES:
            return _api_error(
                request,
                status_code=400,
                code="validation_error",
                fallback_message="theme_preference must be one of auto, light, dark",
                severity="warning",
            )
        updates[_THEME_SETTING_KEY] = theme_preference

    if "language_preference" in body:
        language_preference = str(body.get("language_preference", "")).strip().lower()
        if language_preference not in _VALID_LANGUAGE_PREFERENCES:
            return _api_error(
                request,
                status_code=400,
                code="validation_error",
                fallback_message="language_preference must be one of auto, en, nl",
                severity="warning",
            )
        updates[_LANGUAGE_SETTING_KEY] = language_preference

    raw_pay_day = body.get("pay_day", body.get("cycle_start_day"))
    if raw_pay_day is not None:
        try:
            pay_day = normalize_cycle_start_day(int(raw_pay_day))
        except (TypeError, ValueError):
            return _api_error(
                request,
                status_code=400,
                code="validation_error",
                fallback_message=f"pay_day must be an integer between 1 and {MAX_CYCLE_START_DAY}",
                severity="warning",
            )
        updates[_PAY_DAY_SETTING_KEY] = str(pay_day)

    if not updates:
        return _api_error(
            request,
            status_code=400,
            code="validation_error",
            fallback_message="No supported preference fields provided",
            severity="warning",
        )

    with _get_db() as db:
        for key, value in updates.items():
            db.set_app_setting(key, value)
        preferences = _load_app_preferences(db)

    return {
        "ok": True,
        **preferences,
        "current_cycle": _build_cycle_payload(preferences["pay_day"]),
    }


@app.get("/api/settings/rules")
async def api_get_category_rules():
    """Return user-defined categorization rules."""
    with _get_db() as db:
        rules = db.get_category_rules()
    return {
        "rules": rules,
        "valid_rule_types": sorted(VALID_RULE_TYPES),
        "summary": {
            "total": len(rules),
            "active": sum(1 for rule in rules if rule.get("is_active", True)),
            "inactive": sum(1 for rule in rules if not rule.get("is_active", True)),
        },
    }


@app.post("/api/settings/rules")
async def api_create_category_rule(request: Request):
    """Create a categorization rule (contains/regex => category)."""
    body = await request.json()
    pattern = str(body.get("pattern") or "").strip()
    category = str(body.get("category") or "").strip()
    raw_rule_type = str(body.get("rule_type") or "contains")

    if not pattern:
        return _api_error(
            request,
            status_code=400,
            code="validation_error",
            fallback_message="pattern is required",
            severity="warning",
        )
    if not category:
        return _api_error(
            request,
            status_code=400,
            code="validation_error",
            fallback_message="category is required",
            severity="warning",
        )

    try:
        rule_type = normalize_rule_type(raw_rule_type)
    except ValueError as exc:
        return _api_error(
            request,
            status_code=400,
            code="validation_error",
            fallback_message=str(exc),
            severity="warning",
        )

    if rule_type == "regex":
        import re

        try:
            re.compile(pattern)
        except re.error as exc:
            return _api_error(
                request,
                status_code=400,
                code="validation_error",
                fallback_message=f"Invalid regex: {exc}",
                severity="warning",
            )

    with _get_db() as db:
        rule = db.add_category_rule(rule_type=rule_type, pattern=pattern, category=category)

    return {"ok": True, "rule": rule}


@app.patch("/api/settings/rules/{rule_id}")
async def api_update_category_rule(rule_id: int, request: Request):
    """Toggle or reorder a categorization rule."""
    body = await request.json()
    move = str(body.get("move") or "").strip().lower()
    is_active = body.get("is_active") if "is_active" in body else None

    with _get_db() as db:
        try:
            if move:
                rules = db.move_category_rule(rule_id, move)
                rule = next((item for item in rules if int(item["id"]) == int(rule_id)), None)
            else:
                rule = db.update_category_rule(
                    rule_id,
                    is_active=_coerce_bool(is_active) if is_active is not None else None,
                )
        except ValueError as exc:
            return _api_error(
                request,
                status_code=400,
                code="validation_error",
                fallback_message=str(exc),
                severity="warning",
            )
        except KeyError:
            return _api_error(
                request,
                status_code=404,
                code="resource_not_found",
                fallback_message="Rule not found",
                severity="warning",
                details={"rule_id": int(rule_id)},
            )

    if not rule:
        return _api_error(
            request,
            status_code=404,
            code="resource_not_found",
            fallback_message="Rule not found",
            severity="warning",
            details={"rule_id": int(rule_id)},
        )
    return {"ok": True, "rule": rule}


@app.post("/api/settings/rules/test")
async def api_test_category_rule(request: Request):
    """Preview whether a rule would match sample text and historical rows."""
    body = await request.json()
    pattern = str(body.get("pattern") or "").strip()
    sample_text = str(body.get("sample_text") or "").strip()

    if not pattern:
        return _api_error(
            request,
            status_code=400,
            code="validation_error",
            fallback_message="pattern is required",
            severity="warning",
        )

    try:
        rule_type = normalize_rule_type(str(body.get("rule_type") or "contains"))
    except ValueError as exc:
        return _api_error(
            request,
            status_code=400,
            code="validation_error",
            fallback_message=str(exc),
            severity="warning",
        )

    if rule_type == "regex":
        import re

        try:
            re.compile(pattern)
        except re.error as exc:
            return _api_error(
                request,
                status_code=400,
                code="validation_error",
                fallback_message=f"Invalid regex: {exc}",
                severity="warning",
            )

    with _get_db() as db:
        preview = _simulate_rule_impact(
            db,
            rule_type=rule_type,
            pattern=pattern,
            sample_text=sample_text,
        )

    return {"ok": True, **preview}


@app.delete("/api/settings/rules/{rule_id}")
async def api_delete_category_rule(rule_id: int, request: Request):
    """Delete a categorization rule by ID."""
    with _get_db() as db:
        deleted = db.delete_category_rule(rule_id)
    if not deleted:
        return _api_error(
            request,
            status_code=404,
            code="resource_not_found",
            fallback_message="Rule not found",
            severity="warning",
            details={"rule_id": int(rule_id)},
        )
    return {"ok": True, "id": rule_id}


@app.get("/api/settings/learning")
async def api_learning_summary():
    """Return recent learning events and summary counters."""
    with _get_db() as db:
        events = db.get_recent_learning_feedback(limit=40)
        feedback_count = db._conn.execute("SELECT COUNT(*) FROM learning_feedback").fetchone()[0]
        override_count = db._conn.execute("SELECT COUNT(*) FROM user_overrides").fetchone()[0]
        uncategorized_count = db._conn.execute(
            "SELECT COUNT(*) FROM tx_history WHERE category = 'Uncategorized'"
        ).fetchone()[0]
        learned_future_count = db._conn.execute(
            "SELECT COUNT(*) FROM learning_feedback WHERE apply_to_future = 1"
        ).fetchone()[0]

    return {
        "events": events,
        "summary": {
            "feedback_count": int(feedback_count),
            "override_count": int(override_count),
            "uncategorized_count": int(uncategorized_count),
            "learned_future_count": int(learned_future_count),
        },
    }


@app.post("/api/settings/learning/reapply")
async def api_reapply_learning():
    """Re-run deterministic learning on historical transactions."""
    with _get_db() as db:
        result = db.reapply_learning_to_history()
    return {"ok": True, **result}


@app.post("/api/settings/reset-db")
async def api_reset_db(request: Request):
    """Reset local SQLite data after explicit confirmation."""
    body = await request.json()
    if body.get("confirm") != "RESET":
        return _api_error(
            request,
            status_code=400,
            code="validation_error",
            fallback_message="Confirmation token missing",
            severity="warning",
        )

    with _get_db() as db:
        deleted = db.reset_all_data()

    logger.warning("Local DB reset requested from settings page: %s", deleted)
    return {
        "ok": True,
        "message": "Local database reset completed",
        "deleted": deleted,
    }


# ── API: Upload & Process ────────────────────────────────────────


@app.post("/api/upload")
async def api_upload(request: Request, file: UploadFile = File(...)):
    """Upload a supported file, parse & categorise, stream progress via SSE."""
    supported_file_types = [".csv", ".pdf", ".ofx"]
    import json as _json
    from fastapi.responses import StreamingResponse as _SR

    settings = load_settings()
    request_id = get_request_id(request)
    language = _resolve_request_language(request)

    suffix = Path(file.filename or "upload.csv").suffix.lower()
    if suffix not in supported_file_types:
        allowed = " or ".join(supported_file_types)
        return _api_error(
            request,
            status_code=400,
            code="unsupported_file_type",
            fallback_message=f"Unsupported file type: {suffix}. Upload a {allowed}",
            severity="warning",
            details={"suffix": suffix, "allowed": supported_file_types},
        )

    # Read upload content now (before streaming response starts)
    file_bytes = await file.read()

    async def _stream():
        def evt(pct: int, step: str, **extra) -> str:
            payload = _json.dumps({"pct": pct, "step": step, **extra})
            return f"data: {payload}\n\n"

        try:
            # ── Phase 1: save to temp ──────────────────────────────
            yield evt(5, "Saving file...")
            import asyncio, tempfile, shutil
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(file_bytes)
                tmp_path = tmp.name

            # ── Phase 2: parse ─────────────────────────────────────
            yield evt(15, "Parsing transactions...")
            await asyncio.sleep(0)  # yield control so event is flushed
            try:
                if suffix == ".pdf":
                    from spectra.pdf_parser import parse_pdf
                    parsed = parse_pdf(tmp_path, currency=settings.base_currency)
                elif suffix == ".csv":
                    from spectra.csv_parser import parse_csv
                    parsed = parse_csv(tmp_path, currency=settings.base_currency)
                else:
                    from spectra.ofx_parser import parse_ofx
                    parsed = parse_ofx(tmp_path, currency=settings.base_currency)
            finally:
                Path(tmp_path).unlink(missing_ok=True)

            # ── Phase 3: dedup ─────────────────────────────────────
            yield evt(25, "Checking for duplicates...")
            await asyncio.sleep(0)
            with _get_db() as db:
                new_txns = [t for t in parsed if not db.is_seen(t.id)]
                overrides = db.get_overrides()
                category_rules = db.get_category_rules()
                merchant_db = db.get_merchant_categories()
                training_data = db.get_training_data()
            parsed_total = len(parsed)
            new_total = len(new_txns)
            duplicate_total = max(parsed_total - new_total, 0)

            if not new_txns:
                yield evt(100, "All transactions already imported", done=True,
                          transactions=[],
                          message=f"{parsed_total} parsed · 0 new · {duplicate_total} already imported",
                          parsed_total=parsed_total,
                          new_total=0,
                          duplicate_total=duplicate_total)
                return

            # ── Phase 4: categorise (25% → 92%, per transaction) ───
            from spectra.ai import CategorisedTransaction

            # Pre-categorise from overrides (instant)
            pre_cat = []
            to_process = []
            override_count = 0
            rule_count = 0
            for t in new_txns:
                od = t.raw_description
                if od in overrides:
                    pre_cat.append(CategorisedTransaction(
                        id=t.id, original_description=od,
                        clean_name=overrides[od]["clean_name"],
                        category=overrides[od]["category"],
                        amount=t.amount, currency=t.currency, date=t.date,
                    ))
                    override_count += 1
                    continue

                from spectra.rules import first_matching_rule

                matched_rule = first_matching_rule(
                    category_rules,
                    clean_name=od,
                    raw_description=od,
                )
                if matched_rule:
                    pre_cat.append(CategorisedTransaction(
                        id=t.id,
                        original_description=od,
                        clean_name=od,
                        category=str(matched_rule["category"]),
                        amount=t.amount,
                        currency=t.currency,
                        date=t.date,
                    ))
                    rule_count += 1
                else:
                    to_process.append(t)

            if override_count or rule_count:
                yield evt(
                    28,
                    f"Applied local mappings: {override_count} overrides, {rule_count} rules",
                )

            categorised = list(pre_cat)

            if to_process:
                flat = [
                    {"raw_description": t.raw_description, "amount": t.amount,
                     "currency": t.currency, "date": t.date}
                    for t in to_process
                ]

                if settings.ai_provider == "local":
                    from spectra.local_categorizer import categorise_local
                    from spectra.ml_classifier import train_classifier
                    ml_clf = train_classifier(training_data)

                    # Categorise one-by-one so we can stream real progress
                    results = []
                    for i, row in enumerate(flat):
                        pct = 25 + int((i + 1) / len(flat) * 67)
                        yield evt(pct, f"Categorizing {i + 1} / {len(flat)}...")
                        await asyncio.sleep(0)
                        r = categorise_local([row], merchant_db=merchant_db, ml_classifier=ml_clf)
                        results.extend(r)
                    categorised.extend(results)

                else:
                    # Cloud: categorise in one batch (can't stream per-row)
                    from spectra.ai import categorise
                    provider = settings.ai_provider
                    if provider == "gemini":
                        api_key, model = settings.gemini_api_key, settings.gemini_model
                    else:
                        api_key, model = settings.openai_api_key, settings.openai_model

                    # Fake granular progress while waiting for API
                    for pct in range(30, 88, 5):
                        yield evt(pct, f"Waiting for {provider.title()} AI...")
                        await asyncio.sleep(0.4)

                    results = categorise(flat, [], provider=provider,
                                         api_key=api_key, model=model,
                                         base_currency=settings.base_currency)
                    categorised.extend(results)

            # ── Phase 5: recurring detection ───────────────────────
            yield evt(94, "Detecting recurring payments...")
            await asyncio.sleep(0)
            with _get_db() as db:
                history = db.get_merchant_history()
            from spectra.recurring import apply_recurring_tags
            apply_recurring_tags(categorised, history)

            # ── Phase 6: FX conversion ─────────────────────────────
            yield evt(97, "Converting currencies...")
            await asyncio.sleep(0)
            from spectra.fx import convert_currency
            for t in categorised:
                if t.currency.upper() != settings.base_currency:
                    orig_amt, orig_cur = t.amount, t.currency.upper()
                    t.amount = convert_currency(orig_amt, orig_cur, settings.base_currency, t.date)
                    t.original_amount, t.original_currency = orig_amt, orig_cur
                    t.currency = settings.base_currency

            # ── Done ───────────────────────────────────────────────
            preview = [
                {
                    "id": t.id, "date": t.date, "merchant": t.clean_name,
                    "category": t.category, "amount": t.amount,
                    "currency": t.currency, "recurring": t.recurring,
                    "original_description": t.original_description,
                }
                for t in categorised
            ]
            yield evt(100, f"{len(preview)} transactions ready", done=True,
                      transactions=preview,
                      message=(
                          f"{parsed_total} parsed · {len(preview)} new · "
                          f"{duplicate_total} already imported"
                      ),
                      parsed_total=parsed_total,
                      new_total=len(preview),
                      duplicate_total=duplicate_total)

        except Exception as e:
            logger.exception("Upload stream error [%s]: %s", request_id, e)
            capture_error_event(
                code="upload_processing_failed",
                status_code=500,
                message=str(e),
                severity="error",
                retryable=True,
                request=request,
                details={
                    "filename": str(file.filename or ""),
                    "suffix": suffix,
                },
                exc=e,
                settings=settings,
            )
            payload = build_error_payload(
                code="upload_processing_failed",
                request_id=request_id,
                language=language,
                retryable=True,
            )
            error_payload = {
                "pct": 0,
                "step": payload["error"]["message"],
                "error": True,
                **payload["error"],
            }
            yield f"data: {_json.dumps(error_payload)}\n\n"

    return _SR(_stream(), media_type="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",   # disable nginx buffering if behind proxy
    })



@app.post("/api/confirm")
async def api_confirm(request: Request):
    """Confirm and save previously previewed transactions to the DB."""
    body = await request.json()
    transactions = body.get("transactions", [])

    if not transactions:
        return _api_error(
            request,
            status_code=400,
            code="validation_error",
            fallback_message="No transactions to save",
            severity="warning",
        )

    settings = load_settings()

    with _get_db() as db:
        from spectra.ai import CategorisedTransaction

        cats = []
        future_mappings: dict[str, str] = {}
        future_overrides: dict[str, dict[str, str]] = {}
        learned_count = 0
        for t in transactions:
            ct = CategorisedTransaction(
                id=t["id"], original_description=t.get("original_description", ""),
                clean_name=t["merchant"], category=t["category"],
                amount=t["amount"], currency=t.get("currency", settings.base_currency),
                date=t["date"], recurring=t.get("recurring", ""),
            )
            cats.append(ct)

            apply_to_future = _coerce_bool(t.get("apply_to_future"), True)
            _persist_learning(
                db,
                tx_id=str(ct.id),
                original_description=str(ct.original_description),
                clean_name=str(ct.clean_name),
                category=str(ct.category),
                source="upload_confirm",
                apply_to_future=apply_to_future,
            )
            if apply_to_future and ct.category != "Uncategorized":
                future_mappings[ct.clean_name] = ct.category
                if ct.original_description:
                    future_overrides[ct.original_description] = {
                        "clean_name": ct.clean_name,
                        "category": ct.category,
                    }
                learned_count += 1

        db.save_history(cats)
        db.save_merchant_categories_batch(future_mappings)
        db.save_overrides(future_overrides)

        # Optionally sync to Google Sheets
        if settings.spreadsheet_id and (settings.google_sheets_credentials_b64 or
                                         Path(settings.google_sheets_credentials_file).exists()):
            try:
                from spectra.sheets import SheetsClient
                sheets = SheetsClient(
                    spreadsheet_id=settings.spreadsheet_id,
                    credentials_b64=settings.google_sheets_credentials_b64,
                    credentials_file=settings.google_sheets_credentials_file,
                )
                sheets.append_transactions(cats)
                from spectra.dashboard import refresh_dashboard
                refresh_dashboard(sheets)
                return {
                    "ok": True,
                    "message": f"Saved {len(cats)} transactions + synced to Sheets · learned {learned_count} future mapping(s)",
                }
            except Exception as e:
                logger.warning("Sheets sync failed: %s", e)
                capture_error_event(
                    code="sheets_sync_failed",
                    status_code=502,
                    message=str(e),
                    severity="warning",
                    retryable=True,
                    request=request,
                    details={"transactions_count": len(cats)},
                    exc=e,
                    settings=settings,
                )
                return {
                    "ok": True,
                    "message": f"Saved {len(cats)} transactions · learned {learned_count} future mapping(s) (Sheets sync failed)",
                }

    return {
        "ok": True,
        "message": f"Saved {len(cats)} transactions to local DB · learned {learned_count} future mapping(s)",
    }



# ── Pages: Budget & Trends ───────────────────────────────────────


@app.get("/budget", response_class=HTMLResponse)
async def page_budget(request: Request):
    return templates.TemplateResponse("budget.html", _template_context(request))


@app.get("/trends", response_class=HTMLResponse)
async def page_trends(request: Request):
    return templates.TemplateResponse("trends.html", _template_context(request))


# ── API: Budget ──────────────────────────────────────────────────


@app.get("/api/budget")
async def api_budget():
    """Return per-category budget status for the current month."""
    preferences = _load_app_preferences()
    pay_day = preferences["pay_day"]
    current_cycle = _build_cycle_payload(pay_day)

    with _get_db() as db:
        # All expense rows for the current financial cycle
        rows = db._conn.execute(
            """
            SELECT category, SUM(amount) as total
            FROM tx_history
            WHERE amount < 0 AND date >= ? AND date < ?
            GROUP BY category
            """,
            (current_cycle["start"], current_cycle["end"]),
        ).fetchall()

        limits = db.get_budget_limits()

        # All-time categories so we can show unspent ones too
        all_cats = db._conn.execute(
            """
            SELECT DISTINCT category FROM tx_history
            WHERE category != 'Uncategorized' AND amount < 0
            ORDER BY category
            """
        ).fetchall()

    spent_by_cat: dict[str, float] = {cat: abs(total) for cat, total in rows}
    categories = sorted({row[0] for row in all_cats} | set(limits.keys()))

    items = []
    for cat in categories:
        spent = round(spent_by_cat.get(cat, 0.0), 2)
        limit = limits.get(cat)
        if limit and limit > 0:
            pct = round(spent / limit * 100, 1)
            if pct >= 100:
                status = "red"
            elif pct >= 80:
                status = "yellow"
            else:
                status = "green"
        else:
            pct = None
            status = "none"

        items.append({
            "category": cat,
            "spent": spent,
            "limit": limit,
            "pct": pct,
            "status": status,
        })

    # Sort: over-budget first, then by spent desc
    items.sort(key=lambda x: (x["status"] != "red", x["status"] != "yellow", -x["spent"]))

    on_track = sum(1 for i in items if i["status"] == "green")
    over = sum(1 for i in items if i["status"] == "red")
    no_limit = sum(1 for i in items if i["status"] == "none")

    return {
        "current_cycle": current_cycle,
        "items": items,
        "summary": {"on_track": on_track, "over": over, "no_limit": no_limit},
    }


@app.patch("/api/budget/{category}")
async def api_update_budget(category: str, request: Request):
    """Save or update a monthly budget limit for a category."""
    body = await request.json()
    limit = body.get("limit")
    if limit is None or limit < 0:
        return _api_error(
            request,
            status_code=400,
            code="validation_error",
            fallback_message="limit must be a non-negative number",
            severity="warning",
            details={"category": category, "limit": limit},
        )

    with _get_db() as db:
        db.save_budget_limit(category, float(limit))

    return {"ok": True, "category": category, "limit": limit}


# ── API: Trends ──────────────────────────────────────────────────


@app.get("/api/trends")
async def api_trends():
    """Return year-over-year financial data for the Trends page."""
    from collections import defaultdict

    preferences = _load_app_preferences()
    pay_day = preferences["pay_day"]

    with _get_db() as db:
        rows = db._conn.execute(
            "SELECT date, amount, category FROM tx_history ORDER BY date ASC"
        ).fetchall()

    if not rows:
        return {"years": [], "by_year": {}, "period_series": [], "pay_day": pay_day, "cycle_start_day": pay_day}

    # Aggregate by year and month
    by_year: dict[str, dict] = {}
    monthly_income: dict[str, float] = defaultdict(float)
    monthly_expense: dict[str, float] = defaultdict(float)
    cat_by_year: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))

    for date_str, amount, category in rows:
        tx_date = parse_iso_date(date_str)
        cycle_start = cycle_start_for(tx_date, pay_day)
        year = str(cycle_start.year)
        month = cycle_start.isoformat()

        if year not in by_year:
            by_year[year] = {"income": 0.0, "expenses": 0.0}

        if amount > 0:
            by_year[year]["income"] += amount
            monthly_income[month] += amount
        else:
            by_year[year]["expenses"] += abs(amount)
            monthly_expense[month] += abs(amount)
            if category != "Uncategorized":
                cat_by_year[year][category] += abs(amount)

    # Compute net flow and savings rate per year
    years = sorted(by_year.keys())
    year_stats = {}
    for y in years:
        inc = round(by_year[y]["income"], 2)
        exp = round(by_year[y]["expenses"], 2)
        net = round(inc - exp, 2)
        savings_rate = round((net / inc * 100), 1) if inc > 0 else 0.0
        year_stats[y] = {
            "income": inc,
            "expenses": exp,
            "net": net,
            "savings_rate": savings_rate,
            "by_category": {k: round(v, 2) for k, v in sorted(
                cat_by_year[y].items(), key=lambda x: -x[1]
            )[:8]},  # top 8 categories
        }

    # Build monthly series for charts (all years combined, keyed by YYYY-MM)
    all_months = sorted(set(monthly_income) | set(monthly_expense))
    monthly_series = [
        {
            "period_start": m,
            "period_end": next_cycle_start(parse_iso_date(m), pay_day).isoformat(),
            "income": round(monthly_income.get(m, 0), 2),
            "expenses": round(monthly_expense.get(m, 0), 2),
        }
        for m in all_months
    ]

    return {
        "years": years,
        "by_year": year_stats,
        "period_series": monthly_series,
        "pay_day": pay_day,
        "cycle_start_day": pay_day,
    }


@app.get("/api/subscriptions")
async def api_subscriptions():
    """Return recurring subscriptions with monthly and annual projections."""
    from collections import defaultdict
    from datetime import date, timedelta
    from statistics import median

    preferences = _load_app_preferences()
    pay_day = preferences["pay_day"]
    cycle_start, cycle_end = cycle_window_for(date.today(), pay_day)

    with _get_db() as db:
        rows = db._conn.execute(
            """
            SELECT date, clean_name, amount, category, COALESCE(original_description, '')
            FROM tx_history
            ORDER BY date ASC
            """
        ).fetchall()

    buckets: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "name": "",
            "dates": [],
            "amounts": [],
            "in_cycle": 0.0,
            "last_date": "",
            "category": "Digital Subscriptions",
        }
    )

    for date_str, clean_name, amount, category, original_description in rows:
        if amount >= 0:
            continue

        recurring_kind = detect_recurring_kind(clean_name, original_description, amount)
        if recurring_kind != "Subscription" and category != "Digital Subscriptions":
            continue

        tx_date = parse_iso_date(date_str)
        bucket = buckets[clean_name]
        bucket["name"] = clean_name
        bucket["category"] = category or "Digital Subscriptions"
        bucket["dates"].append(tx_date)
        bucket["amounts"].append(abs(float(amount)))
        if cycle_start <= tx_date < cycle_end:
            bucket["in_cycle"] += abs(float(amount))
        if date_str > bucket["last_date"]:
            bucket["last_date"] = date_str

    items: list[dict[str, Any]] = []
    monthly_total = 0.0
    price_change_count = 0

    for merchant, bucket in buckets.items():
        dates = sorted(set(bucket["dates"]))
        amounts = bucket["amounts"]
        if not amounts:
            continue

        avg_amount = sum(amounts) / len(amounts)
        intervals = [
            (dates[i] - dates[i - 1]).days
            for i in range(1, len(dates))
            if (dates[i] - dates[i - 1]).days > 0
        ]
        cadence_days = int(round(median(intervals))) if intervals else 30
        cadence_days = max(cadence_days, 1)

        monthly_estimate = avg_amount * (30.4375 / cadence_days)
        annual_projection = monthly_estimate * 12
        monthly_total += monthly_estimate

        last_date = parse_iso_date(bucket["last_date"]) if bucket["last_date"] else dates[-1]
        next_charge = last_date + timedelta(days=cadence_days)
        previous_amounts = amounts[:-1]
        baseline_amount = median(previous_amounts) if previous_amounts else amounts[-1]
        change_amount = round(amounts[-1] - baseline_amount, 2)
        change_pct = round((change_amount / baseline_amount) * 100, 1) if baseline_amount else 0.0
        price_change_direction = ""
        if previous_amounts and abs(change_amount) >= max(1.0, baseline_amount * 0.08):
            price_change_direction = "up" if change_amount > 0 else "down"
            price_change_count += 1

        items.append(
            {
                "merchant": merchant,
                "category": bucket["category"],
                "last_amount": round(amounts[-1], 2),
                "average_amount": round(avg_amount, 2),
                "cadence_days": cadence_days,
                "last_charge_date": last_date.isoformat(),
                "next_estimated_date": next_charge.isoformat(),
                "monthly_estimate": round(monthly_estimate, 2),
                "annual_projection": round(annual_projection, 2),
                "in_current_cycle": round(bucket["in_cycle"], 2),
                "payments_count": len(amounts),
                "price_change_direction": price_change_direction,
                "change_amount": change_amount,
                "change_pct": change_pct,
            }
        )

    items.sort(key=lambda row: row["monthly_estimate"], reverse=True)

    return {
        "items": items,
        "summary": {
            "active_count": len(items),
            "monthly_estimate": round(monthly_total, 2),
            "annual_projection": round(monthly_total * 12, 2),
            "in_current_cycle": round(sum(item["in_current_cycle"] for item in items), 2),
            "price_change_count": price_change_count,
        },
        "current_cycle": {
            "start": cycle_start.isoformat(),
            "end": cycle_end.isoformat(),
            "label": format_cycle_label(cycle_start, cycle_end),
        },
    }


# ── Launch ───────────────────────────────────────────────────────


def serve(host: str = "127.0.0.1", port: int = 8080) -> None:
    """Launch the Spectra dashboard server."""
    import uvicorn
    print(f"\n  🌟 Spectra Dashboard running at http://{host}:{port}\n")
    uvicorn.run(app, host=host, port=port, log_level="warning")
