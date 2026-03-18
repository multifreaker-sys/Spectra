"""Centralized error monitoring, alerting, and user-safe error payloads."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from fastapi import Request

from spectra.config import Settings, load_settings
from spectra.db import BookmarkDB

logger = logging.getLogger("spectra.error_watcher")

REQUEST_ID_HEADER = "X-Request-ID"
_DEFAULT_LANGUAGE = "en"
_SUPPORTED_LANGUAGES = {"en", "nl"}

_DEFAULT_ERROR_MESSAGES: dict[str, dict[str, str]] = {
    "internal_server_error": {
        "en": "An unexpected server error occurred.",
        "nl": "Er is een onverwachte serverfout opgetreden.",
    },
    "http_error": {
        "en": "The request could not be completed.",
        "nl": "De aanvraag kon niet worden verwerkt.",
    },
    "bad_request": {
        "en": "The request contains invalid input.",
        "nl": "De aanvraag bevat ongeldige invoer.",
    },
    "unsupported_file_type": {
        "en": "Unsupported file type. Please upload CSV, PDF, or OFX.",
        "nl": "Niet ondersteund bestandstype. Upload CSV, PDF of OFX.",
    },
    "upload_processing_failed": {
        "en": "Upload processing failed. Please try again.",
        "nl": "Uploadverwerking is mislukt. Probeer het opnieuw.",
    },
    "invalid_scope": {
        "en": "Invalid scope value.",
        "nl": "Ongeldige scopewaarde.",
    },
    "resource_not_found": {
        "en": "Requested resource was not found.",
        "nl": "De opgevraagde gegevens zijn niet gevonden.",
    },
    "validation_error": {
        "en": "Validation failed for the submitted data.",
        "nl": "Validatie van de aangeleverde gegevens is mislukt.",
    },
    "settings_update_failed": {
        "en": "Unable to save settings.",
        "nl": "Instellingen konden niet worden opgeslagen.",
    },
    "operation_failed": {
        "en": "The operation failed.",
        "nl": "De bewerking is mislukt.",
    },
}

_SENTRY_INITIALIZED = False
_SENTRY_ACTIVE = False


def normalize_error_code(code: str | None) -> str:
    raw = str(code or "internal_server_error").strip().lower()
    return re.sub(r"[^a-z0-9_]+", "_", raw).strip("_") or "internal_server_error"


def resolve_language(
    preference: str | None = "auto",
    accept_language: str | None = None,
) -> str:
    pref = str(preference or "auto").strip().lower()
    if pref in _SUPPORTED_LANGUAGES:
        return pref

    normalized_accept = str(accept_language or "").strip().lower()
    if normalized_accept.startswith("nl"):
        return "nl"
    return _DEFAULT_LANGUAGE


def localized_error_message(
    code: str | None,
    language: str,
    *,
    fallback_message: str | None = None,
) -> str:
    if fallback_message:
        return str(fallback_message)

    normalized_code = normalize_error_code(code)
    normalized_language = language if language in _SUPPORTED_LANGUAGES else _DEFAULT_LANGUAGE

    mapping = _DEFAULT_ERROR_MESSAGES.get(normalized_code, {})
    if mapping.get(normalized_language):
        return mapping[normalized_language]
    if mapping.get(_DEFAULT_LANGUAGE):
        return mapping[_DEFAULT_LANGUAGE]
    return str(fallback_message or normalized_code.replace("_", " ").capitalize())


def get_request_id(request: Request | None) -> str:
    if request is None:
        return ""

    state_id = getattr(getattr(request, "state", None), "request_id", "")
    if state_id:
        return str(state_id)

    header_id = request.headers.get(REQUEST_ID_HEADER, "")
    return str(header_id or "")


def build_error_payload(
    *,
    code: str,
    request_id: str,
    language: str,
    retryable: bool = False,
    fallback_message: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_code = normalize_error_code(code)
    message_nl = localized_error_message(
        normalized_code,
        "nl",
        fallback_message=fallback_message,
    )
    message_en = localized_error_message(
        normalized_code,
        "en",
        fallback_message=fallback_message,
    )
    message = message_nl if language == "nl" else message_en

    error_payload: dict[str, Any] = {
        "code": normalized_code,
        "message": message,
        "message_nl": message_nl,
        "message_en": message_en,
        "request_id": request_id,
        "retryable": bool(retryable),
    }
    if details:
        error_payload["details"] = details

    return {"ok": False, "error": error_payload}


def init_error_monitoring(settings: Settings | None = None) -> None:
    """Initialize optional Sentry integration if enabled in settings."""
    global _SENTRY_INITIALIZED, _SENTRY_ACTIVE

    if _SENTRY_INITIALIZED:
        return

    settings = settings or load_settings()
    _SENTRY_INITIALIZED = True
    _SENTRY_ACTIVE = False

    if not settings.error_watcher_enabled:
        logger.info("Error watcher is disabled via ERROR_WATCHER_ENABLED.")
        return

    if not settings.sentry_enabled or not settings.sentry_dsn.strip():
        logger.info("Sentry disabled (SENTRY_ENABLED=false or SENTRY_DSN empty).")
        return

    try:
        import sentry_sdk
    except Exception:
        logger.warning("Sentry requested but sentry-sdk is not installed.")
        return

    init_kwargs: dict[str, Any] = {
        "dsn": settings.sentry_dsn.strip(),
        "environment": settings.sentry_environment or "local",
        "release": settings.sentry_release or None,
        "traces_sample_rate": float(settings.sentry_traces_sample_rate),
        "profiles_sample_rate": float(settings.sentry_profiles_sample_rate),
        "send_default_pii": False,
    }

    try:
        from sentry_sdk.integrations.fastapi import FastApiIntegration

        init_kwargs["integrations"] = [FastApiIntegration(transaction_style="endpoint")]
    except Exception:
        logger.info("FastAPI-specific Sentry integration unavailable; continuing without it.")

    try:
        sentry_sdk.init(**init_kwargs)
        _SENTRY_ACTIVE = True
        logger.info("Sentry initialized for Spectra error monitoring.")
    except Exception:
        logger.exception("Failed to initialize Sentry.")
        _SENTRY_ACTIVE = False


def capture_error_event(
    *,
    code: str,
    status_code: int,
    message: str,
    severity: str = "error",
    retryable: bool = False,
    request: Request | None = None,
    details: dict[str, Any] | None = None,
    exc: Exception | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Persist error telemetry, send optional alert, and forward to Sentry."""
    settings = settings or load_settings()
    normalized_code = normalize_error_code(code)
    normalized_severity = str(severity or "error").strip().lower()
    if normalized_severity not in {"info", "warning", "error", "critical"}:
        normalized_severity = "error"

    request_id = get_request_id(request)
    route = str(request.url.path) if request else ""
    method = str(request.method) if request else ""
    event_id = 0

    if settings.error_watcher_enabled:
        try:
            with BookmarkDB(settings.db_path) as db:
                event_id = db.record_error_event(
                    request_id=request_id,
                    error_code=normalized_code,
                    severity=normalized_severity,
                    message=message,
                    status_code=int(status_code),
                    route=route,
                    method=method,
                    retryable=bool(retryable),
                    metadata=details or {},
                )
                _maybe_prune_events(db, settings)
                _maybe_send_alert(
                    db,
                    settings,
                    error_code=normalized_code,
                    severity=normalized_severity,
                    status_code=int(status_code),
                    request_id=request_id,
                    route=route,
                    message=message,
                )
        except Exception:
            logger.exception("Failed to persist error event telemetry.")

    if settings.sentry_enabled and (settings.sentry_dsn or "").strip():
        _capture_to_sentry(
            settings=settings,
            code=normalized_code,
            severity=normalized_severity,
            status_code=int(status_code),
            message=message,
            request_id=request_id,
            route=route,
            method=method,
            details=details or {},
            exc=exc,
        )

    return {
        "event_id": event_id,
        "request_id": request_id,
        "code": normalized_code,
    }


def _maybe_prune_events(db: BookmarkDB, settings: Settings) -> None:
    retention_days = max(1, int(settings.error_events_retention_days))
    now = datetime.now(timezone.utc)
    key = "error_events_last_prune_at"
    raw_last = db.get_app_setting(key)
    if raw_last:
        try:
            last_dt = datetime.fromisoformat(raw_last.replace("Z", "+00:00"))
            if now - last_dt < timedelta(hours=24):
                return
        except ValueError:
            pass

    deleted = db.prune_error_events(retention_days=retention_days)
    db.set_app_setting(key, now.isoformat())
    if deleted:
        logger.info("Pruned %d old error event(s).", deleted)


def _maybe_send_alert(
    db: BookmarkDB,
    settings: Settings,
    *,
    error_code: str,
    severity: str,
    status_code: int,
    request_id: str,
    route: str,
    message: str,
) -> None:
    webhook = str(settings.error_alert_webhook_url or "").strip()
    if not webhook:
        return
    if severity not in {"error", "critical"} and status_code < 500:
        return

    window_minutes = max(1, int(settings.error_alert_window_minutes))
    threshold = max(1, int(settings.error_alert_threshold_count))
    count = db.count_error_events(
        window_minutes=window_minutes,
        error_code=error_code,
    )
    if count < threshold:
        return

    now = datetime.now(timezone.utc)
    key = f"error_alert_last_sent:{error_code}"
    raw_last = db.get_app_setting(key)
    if raw_last:
        try:
            last_dt = datetime.fromisoformat(raw_last.replace("Z", "+00:00"))
            if now - last_dt < timedelta(minutes=window_minutes):
                return
        except ValueError:
            pass

    payload = {
        "text": f"Spectra alert: {error_code} reached {count} events in {window_minutes}m",
        "error_code": error_code,
        "count": count,
        "window_minutes": window_minutes,
        "severity": severity,
        "status_code": status_code,
        "request_id": request_id,
        "route": route,
        "message": message,
        "timestamp": now.isoformat(),
    }

    try:
        timeout = max(1.0, float(settings.error_alert_timeout_seconds))
        response = httpx.post(webhook, json=payload, timeout=timeout)
        response.raise_for_status()
        db.set_app_setting(key, now.isoformat())
        logger.warning(
            "Error alert sent for %s (%d events in %dm).",
            error_code,
            count,
            window_minutes,
        )
    except Exception:
        logger.exception("Failed to send error alert webhook.")


def _capture_to_sentry(
    *,
    settings: Settings,
    code: str,
    severity: str,
    status_code: int,
    message: str,
    request_id: str,
    route: str,
    method: str,
    details: dict[str, Any],
    exc: Exception | None,
) -> None:
    global _SENTRY_ACTIVE
    if not _SENTRY_ACTIVE:
        init_error_monitoring(settings)
    if not _SENTRY_ACTIVE:
        return

    try:
        import sentry_sdk

        with sentry_sdk.push_scope() as scope:
            scope.set_tag("spectra.error_code", code)
            scope.set_tag("spectra.severity", severity)
            scope.set_tag("spectra.status_code", str(status_code))
            if request_id:
                scope.set_tag("spectra.request_id", request_id)
            if route:
                scope.set_tag("spectra.route", route)
            if method:
                scope.set_tag("spectra.method", method)

            scope.set_context(
                "spectra_error",
                {
                    "code": code,
                    "severity": severity,
                    "status_code": status_code,
                    "request_id": request_id,
                    "route": route,
                    "method": method,
                    "message": message,
                    "details": details,
                },
            )

            if exc is not None:
                sentry_sdk.capture_exception(exc)
            else:
                sentry_sdk.capture_message(message, level=severity if severity in {"info", "warning", "error"} else "error")
    except Exception:
        logger.exception("Failed to capture error in Sentry.")
