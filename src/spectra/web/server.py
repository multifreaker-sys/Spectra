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
from spectra.db import BookmarkDB

logger = logging.getLogger("spectra.web")

_HERE = Path(__file__).parent
_TEMPLATES = _HERE / "templates"
_STATIC = _HERE / "static"

app = FastAPI(title="Spectra Dashboard", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=str(_STATIC)), name="static")
templates = Jinja2Templates(directory=str(_TEMPLATES))


def _get_db() -> BookmarkDB:
    settings = load_settings()
    return BookmarkDB(settings.db_path)


# ── Pages ────────────────────────────────────────────────────────


@app.get("/", response_class=HTMLResponse)
async def page_dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/transactions", response_class=HTMLResponse)
async def page_transactions(request: Request):
    return templates.TemplateResponse("transactions.html", {"request": request})


@app.get("/upload", response_class=HTMLResponse)
async def page_upload(request: Request):
    return templates.TemplateResponse("upload.html", {"request": request})


@app.get("/settings", response_class=HTMLResponse)
async def page_settings(request: Request):
    return templates.TemplateResponse("settings.html", {"request": request})


# ── API: Dashboard Summary ───────────────────────────────────────


@app.get("/api/summary")
async def api_summary():
    """Return dashboard-level stats."""
    with _get_db() as db:
        rows = db._conn.execute(
            "SELECT date, clean_name, amount, category FROM tx_history ORDER BY date DESC"
        ).fetchall()

    if not rows:
        return {
            "total_spent": 0, "total_income": 0, "subscriptions": 0,
            "uncategorized": 0, "by_category": {}, "monthly": {},
            "top_merchants": [],
        }

    from collections import Counter, defaultdict

    total_spent = 0.0
    total_income = 0.0
    subscriptions = 0.0
    uncategorized = 0
    by_category: dict[str, float] = defaultdict(float)
    monthly: dict[str, float] = defaultdict(float)
    merchant_totals: Counter = Counter()

    for date, clean_name, amount, cat in rows:
        month = date[:7]  # YYYY-MM

        if amount < 0:
            total_spent += abs(amount)
            by_category[cat] += abs(amount)
            monthly[month] += abs(amount)
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

    # Top 5
    top5 = [{"name": n, "total": round(t, 2)} for n, t in merchant_totals.most_common(5)]

    return {
        "total_spent": round(total_spent, 2),
        "total_income": round(total_income, 2),
        "subscriptions": round(subscriptions, 2),
        "uncategorized": uncategorized,
        "by_category": {k: round(v, 2) for k, v in sorted(by_category.items(), key=lambda x: -x[1])},
        "monthly": monthly_data,
        "top_merchants": top5,
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


# ── API: Upload & Process ────────────────────────────────────────


@app.post("/api/upload")
async def api_upload(file: UploadFile = File(...)):
    """Upload a CSV/PDF, parse & categorise, return preview."""
    settings = load_settings()

    # Save upload to temp file
    suffix = Path(file.filename or "upload.csv").suffix
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name

    try:
        # Parse
        from spectra.csv_parser import parse_csv
        if suffix.lower() == ".pdf":
            from spectra.pdf_parser import parse_pdf
            parsed = parse_pdf(tmp_path, currency=settings.base_currency)
        else:
            parsed = parse_csv(tmp_path, currency=settings.base_currency)

        # Filter duplicates
        with _get_db() as db:
            new_txns = [t for t in parsed if not db.is_seen(t.id)]
            overrides = db.get_overrides()
            merchant_db = db.get_merchant_categories()
            training_data = db.get_training_data()

        if not new_txns:
            return {"transactions": [], "message": "All transactions already imported"}

        # Categorise based on provider
        from spectra.ai import CategorisedTransaction

        to_process = []
        pre_categorised = []

        for t in new_txns:
            od = t.raw_description
            if od in overrides:
                pre_categorised.append(CategorisedTransaction(
                    id=t.id, original_description=od,
                    clean_name=overrides[od]["clean_name"],
                    category=overrides[od]["category"],
                    amount=t.amount, currency=t.currency, date=t.date,
                ))
            else:
                to_process.append(t)

        categorised = list(pre_categorised)

        if to_process:
            flat = [
                {"raw_description": t.raw_description, "amount": t.amount,
                 "currency": t.currency, "date": t.date}
                for t in to_process
            ]

            if settings.ai_provider == "local":
                from spectra.local_categorizer import categorise_local
                from spectra.ml_classifier import train_classifier
                ml_clf = train_classifier(training_data) if training_data else None
                results = categorise_local(flat, [], merchant_db=merchant_db, ml_classifier=ml_clf)
            else:
                from spectra.ai import categorise
                provider = settings.ai_provider
                if provider == "gemini":
                    api_key, model = settings.gemini_api_key, settings.gemini_model
                else:
                    api_key, model = settings.openai_api_key, settings.openai_model
                results = categorise(flat, [], provider=provider, api_key=api_key, model=model,
                                     base_currency=settings.base_currency)

            categorised.extend(results)

        # Apply recurring detection
        with _get_db() as db:
            history = db.get_merchant_history()
        from spectra.recurring import apply_recurring_tags
        apply_recurring_tags(categorised, history)

        # FX conversion
        from spectra.fx import convert_currency
        for t in categorised:
            if t.currency.upper() != settings.base_currency:
                orig_amt, orig_cur = t.amount, t.currency.upper()
                t.amount = convert_currency(orig_amt, orig_cur, settings.base_currency, t.date)
                t.original_amount, t.original_currency = orig_amt, orig_cur
                t.currency = settings.base_currency

        preview = [
            {
                "id": t.id, "date": t.date, "merchant": t.clean_name,
                "category": t.category, "amount": t.amount,
                "currency": t.currency, "recurring": t.recurring,
                "original_description": t.original_description,
            }
            for t in categorised
        ]

        return {"transactions": preview, "message": f"{len(preview)} new transactions"}

    finally:
        Path(tmp_path).unlink(missing_ok=True)


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


# ── Launch ───────────────────────────────────────────────────────


def serve(host: str = "127.0.0.1", port: int = 8080) -> None:
    """Launch the Spectra dashboard server."""
    import uvicorn
    print(f"\n  🌟 Spectra Dashboard running at http://{host}:{port}\n")
    uvicorn.run(app, host=host, port=port, log_level="warning")
