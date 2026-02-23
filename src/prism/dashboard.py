"""Dashboard — builds a summary tab with 3 charts in Google Sheets."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime
from typing import Any

logger = logging.getLogger("prism.dashboard")


def refresh_dashboard(sheets_client: Any) -> None:
    """Read Transactions sheet → compute summaries → write Dashboard tab → create charts."""

    # ── 1. Read all transactions ──────────────────────────────────
    all_rows = sheets_client.get_all_rows()
    if len(all_rows) < 2:
        logger.info("No data to build dashboard")
        return

    header = all_rows[0]
    try:
        date_idx = header.index("Date")
        cat_idx = header.index("Category")
        amt_idx = header.index("Amount")
    except ValueError:
        logger.warning("Required columns not found: %s", header)
        return

    # ── 2. Compute summaries ──────────────────────────────────────
    cat_totals: dict[str, float] = defaultdict(float)
    monthly: dict[str, dict[str, float]] = defaultdict(lambda: {"income": 0.0, "expenses": 0.0})

    for row in all_rows[1:]:
        if len(row) <= max(date_idx, cat_idx, amt_idx):
            continue
        try:
            date_str = row[date_idx].strip()
            category = row[cat_idx].strip() or "Other"
            amount = float(row[amt_idx].replace(",", "."))
        except (ValueError, IndexError):
            continue

        # Category totals (expenses only, absolute value)
        if amount < 0:
            cat_totals[category] += abs(amount)

        # Monthly breakdown
        try:
            month = datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m")
        except ValueError:
            continue
        if amount >= 0:
            monthly[month]["income"] += amount
        else:
            monthly[month]["expenses"] += abs(amount)

    # Sort by spending
    sorted_cats = sorted(cat_totals.items(), key=lambda x: x[1], reverse=True)
    sorted_months = sorted(monthly.keys())[-6:]  # Last 6 months

    # ── 3. Get or create Dashboard worksheet ──────────────────────
    ss = sheets_client._spreadsheet
    try:
        ws = ss.worksheet("Dashboard")
        ws.clear()
        logger.info("Cleared existing Dashboard tab")
    except Exception:
        ws = ss.add_worksheet(title="Dashboard", rows=200, cols=20)
        logger.info("Created Dashboard tab")

    # ── 4. Write summary tables ───────────────────────────────────

    # Table A: Spending by Category
    cat_header = [["Category", "Total Spent (€)"]]
    cat_rows = [[name, round(total, 2)] for name, total in sorted_cats]
    ws.update("A1", cat_header + cat_rows)

    # Table B: Monthly summary (starts at column D)
    month_header = [["Month", "Income (€)", "Expenses (€)", "Net (€)"]]
    month_rows = [
        [
            m,
            round(monthly[m]["income"], 2),
            round(monthly[m]["expenses"], 2),
            round(monthly[m]["income"] - monthly[m]["expenses"], 2),
        ]
        for m in sorted_months
    ]
    ws.update("D1", month_header + month_rows)

    logger.info(
        "Dashboard data written: %d categories, %d months",
        len(sorted_cats), len(sorted_months),
    )

    # ── 5. Create/refresh charts via Sheets API ───────────────────
    try:
        from googleapiclient.discovery import build  # type: ignore[import-untyped]

        service = build("sheets", "v4", credentials=sheets_client._creds)
        spreadsheet_id = sheets_client._spreadsheet_id

        # Get sheet IDs
        meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_id_map = {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta["sheets"]}
        dashboard_id = sheet_id_map.get("Dashboard")
        if dashboard_id is None:
            logger.warning("Dashboard sheet ID not found, skipping charts")
            return

        # Delete existing charts on Dashboard
        existing_charts = [
            c for s in meta["sheets"]
            if s["properties"]["sheetId"] == dashboard_id
            for c in s.get("charts", [])
        ]
        delete_requests = [
            {"deleteEmbeddedObject": {"objectId": c["chartId"]}}
            for c in existing_charts
        ]

        n_cats = len(cat_rows)
        n_months = len(month_rows)

        add_requests = []

        # Chart 1: Donut — Spending by Category
        if n_cats > 0:
            add_requests.append(_donut_chart(dashboard_id, n_cats))

        # Chart 2: Column — Monthly Expenses
        if n_months > 0:
            add_requests.append(_monthly_expenses_chart(dashboard_id, n_months))

        # Chart 3: Stacked — Income vs Expenses
        if n_months > 0:
            add_requests.append(_income_vs_expenses_chart(dashboard_id, n_months))

        if delete_requests or add_requests:
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": delete_requests + add_requests},
            ).execute()
            logger.info("Dashboard charts created/updated")

    except Exception as e:
        logger.warning("Could not create charts (googleapiclient missing?): %s", e)


# ── Chart builders ────────────────────────────────────────────────


def _donut_chart(sheet_id: int, n_rows: int) -> dict[str, Any]:
    """Donut chart: Spending by Category (col A=labels, col B=values)."""
    return {
        "addChart": {
            "chart": {
                "spec": {
                    "title": "Spending by Category",
                    "pieChart": {
                        "legendPosition": "RIGHT_LEGEND",
                        "pieHole": 0.4,  # donut
                        "domain": {
                            "sourceRange": {"sources": [_range(sheet_id, 1, 0, 1 + n_rows, 1)]}
                        },
                        "series": {
                            "sourceRange": {"sources": [_range(sheet_id, 1, 1, 1 + n_rows, 2)]}
                        },
                    },
                },
                "position": _anchor(sheet_id, row=1, col=6),
            }
        }
    }


def _monthly_expenses_chart(sheet_id: int, n_rows: int) -> dict[str, Any]:
    """Column chart: Monthly Expenses (col D=month, col F=expenses)."""
    return {
        "addChart": {
            "chart": {
                "spec": {
                    "title": "Monthly Expenses",
                    "basicChart": {
                        "chartType": "COLUMN",
                        "legendPosition": "BOTTOM_LEGEND",
                        "axis": [
                            {"position": "BOTTOM_AXIS", "title": "Month"},
                            {"position": "LEFT_AXIS", "title": "€"},
                        ],
                        "domains": [
                            {"domain": {"sourceRange": {"sources": [_range(sheet_id, 1, 3, 1 + n_rows, 4)]}}}
                        ],
                        "series": [
                            {
                                "series": {"sourceRange": {"sources": [_range(sheet_id, 1, 5, 1 + n_rows, 6)]}},
                                "targetAxis": "LEFT_AXIS",
                            }
                        ],
                    },
                },
                "position": _anchor(sheet_id, row=20, col=6),
            }
        }
    }


def _income_vs_expenses_chart(sheet_id: int, n_rows: int) -> dict[str, Any]:
    """Stacked column: Income vs Expenses per month."""
    return {
        "addChart": {
            "chart": {
                "spec": {
                    "title": "Income vs Expenses",
                    "basicChart": {
                        "chartType": "COLUMN",
                        "stackedType": "STACKED",
                        "legendPosition": "BOTTOM_LEGEND",
                        "axis": [
                            {"position": "BOTTOM_AXIS", "title": "Month"},
                            {"position": "LEFT_AXIS", "title": "€"},
                        ],
                        "domains": [
                            {"domain": {"sourceRange": {"sources": [_range(sheet_id, 1, 3, 1 + n_rows, 4)]}}}
                        ],
                        "series": [
                            {
                                "series": {"sourceRange": {"sources": [_range(sheet_id, 1, 4, 1 + n_rows, 5)]}},
                                "targetAxis": "LEFT_AXIS",
                            },
                            {
                                "series": {"sourceRange": {"sources": [_range(sheet_id, 1, 5, 1 + n_rows, 6)]}},
                                "targetAxis": "LEFT_AXIS",
                            },
                        ],
                    },
                },
                "position": _anchor(sheet_id, row=40, col=6),
            }
        }
    }


def _range(sheet_id: int, r1: int, c1: int, r2: int, c2: int) -> dict[str, Any]:
    """Build a GridRange dict (1-indexed end, exclusive)."""
    return {
        "sheetId": sheet_id,
        "startRowIndex": r1,
        "endRowIndex": r2,
        "startColumnIndex": c1,
        "endColumnIndex": c2,
    }


def _anchor(sheet_id: int, row: int, col: int) -> dict[str, Any]:
    """Overlay position anchored at a cell."""
    return {
        "overlayPosition": {
            "anchorCell": {
                "sheetId": sheet_id,
                "rowIndex": row,
                "columnIndex": col,
            },
            "widthPixels": 600,
            "heightPixels": 350,
        }
    }
