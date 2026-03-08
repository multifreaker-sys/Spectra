"""Spectra Web Dashboard — FastAPI backend."""

from __future__ import annotations

import logging
import shutil
import tempfile
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from spectra.config import load_settings
from spectra.cycles import (
    DEFAULT_CYCLE_START_DAY,
    cycle_start_for,
    cycle_window_for,
    format_cycle_label,
    next_cycle_start,
    normalize_cycle_start_day,
    parse_iso_date,
)
from spectra.db import BookmarkDB

logger = logging.getLogger("spectra.web")

_HERE = Path(__file__).parent
_TEMPLATES = _HERE / "templates"
_STATIC = _HERE / "static"

app = FastAPI(title="Spectra Dashboard", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=str(_STATIC)), name="static")
templates = Jinja2Templates(directory=str(_TEMPLATES))

_THEME_SETTING_KEY = "theme_preference"
_PAY_DAY_SETTING_KEY = "cycle_start_day"
_VALID_THEME_PREFERENCES = {"auto", "light", "dark"}
_VALID_SUMMARY_SCOPES = {"cycle", "90d", "ytd"}


# ── Global error handler ─────────────────────────────────────────


from fastapi.responses import JSONResponse as _JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request, exc):
    return _JSONResponse({"error": str(exc.detail)}, status_code=exc.status_code)


@app.exception_handler(Exception)
async def generic_exception_handler(request, exc):
    logger.exception("Unhandled error: %s", exc)
    return _JSONResponse({"error": "Internal server error"}, status_code=500)


def _get_db() -> BookmarkDB:
    settings = load_settings()
    return BookmarkDB(settings.db_path)


def _load_app_preferences(db: BookmarkDB | None = None) -> dict[str, Any]:
    owns_db = db is None
    db = db or _get_db()
    try:
        raw_theme = (db.get_app_setting(_THEME_SETTING_KEY, "auto") or "auto").strip().lower()
        theme_preference = raw_theme if raw_theme in _VALID_THEME_PREFERENCES else "auto"

        raw_pay_day = db.get_app_setting(_PAY_DAY_SETTING_KEY, str(DEFAULT_CYCLE_START_DAY))
        try:
            pay_day = normalize_cycle_start_day(int(raw_pay_day or DEFAULT_CYCLE_START_DAY))
        except (TypeError, ValueError):
            pay_day = DEFAULT_CYCLE_START_DAY
    finally:
        if owns_db:
            db.close()

    return {
        "theme_preference": theme_preference,
        "pay_day": pay_day,
        # Backward compatibility for older clients.
        "cycle_start_day": pay_day,
    }


def _build_cycle_payload(pay_day: int):
    from datetime import date

    cycle_start, cycle_end = cycle_window_for(date.today(), pay_day)
    return {
        "start": cycle_start.isoformat(),
        "end": cycle_end.isoformat(),
        "label": format_cycle_label(cycle_start, cycle_end),
        "pay_day": pay_day,
        # Backward compatibility.
        "start_day": pay_day,
    }


def _template_context(request: Request) -> dict[str, Any]:
    return {"request": request, **_load_app_preferences()}


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


# ── API: Dashboard Summary ───────────────────────────────────────


@app.get("/api/summary")
async def api_summary(scope: str = Query("cycle")):
    """Return dashboard-level stats."""
    scope = (scope or "cycle").strip().lower()
    if scope not in _VALID_SUMMARY_SCOPES:
        return JSONResponse(
            {"error": "scope must be one of cycle, 90d, ytd"},
            status_code=400,
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

    with _get_db() as db:
        rows = db._conn.execute(
            "SELECT date, clean_name, amount, category FROM tx_history ORDER BY date DESC"
        ).fetchall()

    if not rows:
        return {
            "total_spent": 0, "total_income": 0, "subscriptions": 0,
            "uncategorized": 0, "by_category": {}, "monthly": {},
            "monthly_ranges": {}, "top_merchants": [], "current_cycle": _build_cycle_payload(pay_day),
            "scope": scope, "scope_label": scope_label,
            "selected_period": {"start": period_start.isoformat(), "end": period_end.isoformat(), "label": scope_label},
            "pay_day": pay_day, "cycle_start_day": pay_day, "has_data": False,
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
    }


# ── API: Transactions ────────────────────────────────────────────


@app.get("/api/transactions")
async def api_transactions(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=10, le=200),
    category: str = Query("", alias="category"),
    search: str = Query(""),
    date_from: str = Query(""),
    date_to: str = Query(""),
):
    """Return paginated transactions from history."""
    with _get_db() as db:
        query = "SELECT tx_id, date, clean_name, amount, category FROM tx_history ORDER BY date DESC"
        rows = db._conn.execute(query).fetchall()

    # Build result with categories
    results = []
    for tx_id, date, clean_name, amount, cat in rows:

        # Filters
        if category and cat.lower() != category.lower():
            continue
        if search and search.lower() not in clean_name.lower():
            continue
        if date_from and date < date_from:
            continue
        if date_to and date > date_to:
            continue

        results.append({
            "id": tx_id,
            "date": date,
            "merchant": clean_name,
            "category": cat,
            "amount": amount,
        })

    total = len(results)
    start = (page - 1) * per_page
    page_data = results[start : start + per_page]

    return {
        "transactions": page_data,
        "total": total,
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

    with _get_db() as db:
        # Get current merchant name for this transaction
        row = db._conn.execute(
            "SELECT clean_name FROM tx_history WHERE tx_id = ?", (tx_id,)
        ).fetchone()

        if not row:
            return JSONResponse({"error": "Transaction not found"}, status_code=404)

        old_name = row[0]
        merchant_name = new_merchant or old_name

        if new_category:
            db.save_merchant_category(merchant_name, new_category)
            # Also update the category directly on this transaction
            db._conn.execute(
                "UPDATE tx_history SET category = ? WHERE tx_id = ?",
                (new_category, tx_id),
            )
            db._conn.commit()

    return {"ok": True, "id": tx_id}


# ── API: Categories ──────────────────────────────────────────────


@app.get("/api/categories")
async def api_categories():
    """Return all known categories."""
    with _get_db() as db:
        cats = db._conn.execute(
            "SELECT DISTINCT category FROM tx_history WHERE category != 'Uncategorized' ORDER BY category"
        ).fetchall()
    return {"categories": [row[0] for row in cats]}


# ── API: Settings ────────────────────────────────────────────────


@app.get("/api/settings")
async def api_settings():
    """Return current config for the settings page."""
    settings = load_settings()
    with _get_db() as db:
        preferences = _load_app_preferences(db)
        tx_count = db._conn.execute("SELECT COUNT(*) FROM tx_history").fetchone()[0]
        merchant_count = db._conn.execute("SELECT COUNT(*) FROM merchant_categories").fetchone()[0]
        cats = db._conn.execute(
            "SELECT DISTINCT category FROM tx_history WHERE category != 'Uncategorized'"
        ).fetchall()

    return {
        "provider": settings.ai_provider,
        "currency": settings.base_currency,
        "tx_count": tx_count,
        "merchant_count": merchant_count,
        "category_count": len(cats),
        "sheets_connected": bool(settings.spreadsheet_id),
        **preferences,
        "current_cycle": _build_cycle_payload(preferences["pay_day"]),
    }


@app.patch("/api/settings/preferences")
async def api_update_preferences(request: Request):
    """Save dashboard-local preferences such as theme and cycle start day."""
    body = await request.json()
    updates: dict[str, str] = {}

    if "theme_preference" in body:
        theme_preference = str(body.get("theme_preference", "")).strip().lower()
        if theme_preference not in _VALID_THEME_PREFERENCES:
            return JSONResponse(
                {"error": "theme_preference must be one of auto, light, dark"},
                status_code=400,
            )
        updates[_THEME_SETTING_KEY] = theme_preference

    raw_pay_day = body.get("pay_day", body.get("cycle_start_day"))
    if raw_pay_day is not None:
        try:
            pay_day = normalize_cycle_start_day(int(raw_pay_day))
        except (TypeError, ValueError):
            return JSONResponse(
                {"error": "pay_day must be an integer between 1 and 31"},
                status_code=400,
            )
        updates[_PAY_DAY_SETTING_KEY] = str(pay_day)

    if not updates:
        return JSONResponse({"error": "No supported preference fields provided"}, status_code=400)

    with _get_db() as db:
        for key, value in updates.items():
            db.set_app_setting(key, value)
        preferences = _load_app_preferences(db)

    return {
        "ok": True,
        **preferences,
        "current_cycle": _build_cycle_payload(preferences["pay_day"]),
    }


@app.post("/api/settings/reset-db")
async def api_reset_db(request: Request):
    """Reset local SQLite data after explicit confirmation."""
    body = await request.json()
    if body.get("confirm") != "RESET":
        return JSONResponse(
            {"ok": False, "error": "Confirmation token missing"},
            status_code=400,
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
async def api_upload(file: UploadFile = File(...)):
    """Upload a CSV/PDF, parse & categorise, stream progress via SSE."""
    import json as _json
    from fastapi.responses import StreamingResponse as _SR

    settings = load_settings()

    suffix = Path(file.filename or "upload.csv").suffix.lower()
    if suffix not in (".csv", ".pdf"):
        return JSONResponse(
            {"error": f"Unsupported file type: {suffix}. Upload a .csv or .pdf"},
            status_code=400,
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
                else:
                    from spectra.csv_parser import parse_csv
                    parsed = parse_csv(tmp_path, currency=settings.base_currency)
            finally:
                Path(tmp_path).unlink(missing_ok=True)

            # ── Phase 3: dedup ─────────────────────────────────────
            yield evt(25, "Checking for duplicates...")
            await asyncio.sleep(0)
            with _get_db() as db:
                new_txns = [t for t in parsed if not db.is_seen(t.id)]
                overrides = db.get_overrides()
                merchant_db = db.get_merchant_categories()
                training_data = db.get_training_data()

            if not new_txns:
                yield evt(100, "All transactions already imported", done=True,
                          transactions=[], message="All transactions already imported")
                return

            n = len(new_txns)

            # ── Phase 4: categorise (25% → 92%, per transaction) ───
            from spectra.ai import CategorisedTransaction

            # Pre-categorise from overrides (instant)
            pre_cat = []
            to_process = []
            for t in new_txns:
                od = t.raw_description
                if od in overrides:
                    pre_cat.append(CategorisedTransaction(
                        id=t.id, original_description=od,
                        clean_name=overrides[od]["clean_name"],
                        category=overrides[od]["category"],
                        amount=t.amount, currency=t.currency, date=t.date,
                    ))
                else:
                    to_process.append(t)

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
                      transactions=preview, message=f"{len(preview)} new transactions")

        except Exception as e:
            logger.exception("Upload stream error: %s", e)
            yield f"data: {_json.dumps({'pct': 0, 'step': 'Error: ' + str(e), 'error': True})}\n\n"

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
        return {"ok": False, "message": "No transactions to save"}

    settings = load_settings()

    with _get_db() as db:
        from spectra.ai import CategorisedTransaction

        cats = []
        for t in transactions:
            ct = CategorisedTransaction(
                id=t["id"], original_description=t.get("original_description", ""),
                clean_name=t["merchant"], category=t["category"],
                amount=t["amount"], currency=t.get("currency", settings.base_currency),
                date=t["date"], recurring=t.get("recurring", ""),
            )
            cats.append(ct)

        db.save_history(cats)
        mappings = {t.clean_name: t.category for t in cats if t.category != "Uncategorized"}
        db.save_merchant_categories_batch(mappings)

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
                return {"ok": True, "message": f"Saved {len(cats)} transactions + synced to Sheets"}
            except Exception as e:
                logger.warning("Sheets sync failed: %s", e)
                return {"ok": True, "message": f"Saved {len(cats)} transactions (Sheets sync failed)"}

    return {"ok": True, "message": f"Saved {len(cats)} transactions to local DB"}



# ── Pages: Budget & Trends ───────────────────────────────────────


@app.get("/budget", response_class=HTMLResponse)
async def page_budget(request: Request):
    return templates.TemplateResponse("budget.html", {"request": request})


@app.get("/trends", response_class=HTMLResponse)
async def page_trends(request: Request):
    return templates.TemplateResponse("trends.html", {"request": request})


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
        return JSONResponse({"error": "limit must be a non-negative number"}, status_code=400)

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


# ── Launch ───────────────────────────────────────────────────────


def serve(host: str = "127.0.0.1", port: int = 8080) -> None:
    """Launch the Spectra dashboard server."""
    import uvicorn
    print(f"\n  🌟 Spectra Dashboard running at http://{host}:{port}\n")
    uvicorn.run(app, host=host, port=port, log_level="warning")
