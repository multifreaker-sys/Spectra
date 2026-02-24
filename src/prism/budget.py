"""Budget management — read/write budget tab, compute status per category."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("prism.budget")

_BUDGET_SHEET = "Budget"
_HEADER = ["Category", "Monthly Budget (€)"]


# ── Read ──────────────────────────────────────────────────────────

def read_budgets(sheets_client: Any) -> dict[str, float]:
    """Read the Budget tab and return {category: budget_amount}."""
    try:
        ws = sheets_client._spreadsheet.worksheet(_BUDGET_SHEET)
    except Exception:
        return {}

    rows = ws.get_all_values()
    if len(rows) < 2:
        return {}

    budgets: dict[str, float] = {}
    for row in rows[1:]:
        if len(row) < 2 or not row[0].strip():
            continue
        try:
            amount = float(row[1].replace(",", ".").strip() or "0")
            budgets[row[0].strip()] = amount
        except ValueError:
            pass

    return budgets


# ── Write / sync ──────────────────────────────────────────────────

def sync_budget_sheet(sheets_client: Any, categories: list[str]) -> None:
    """Create the Budget tab if missing, and add any new categories with 0 budget.

    The user fills in the actual numbers — Prism never overwrites existing values.
    """
    try:
        ws = sheets_client._spreadsheet.worksheet(_BUDGET_SHEET)
        existing_rows = ws.get_all_values()
    except Exception:
        # Tab doesn't exist yet — create it
        ws = sheets_client._spreadsheet.add_worksheet(
            title=_BUDGET_SHEET, rows=100, cols=5
        )
        existing_rows = []
        logger.info("Created Budget tab")

    # Build set of categories already in the sheet
    existing_cats = {r[0].strip() for r in existing_rows[1:] if r}

    # Write header if missing
    if not existing_rows:
        ws.update("A1", [_HEADER])
        _apply_budget_header_formatting(sheets_client, ws.id)

    # Append any new categories (budget = 0, user fills them in)
    new_rows = [[cat, 0] for cat in sorted(categories) if cat not in existing_cats]
    if new_rows:
        ws.append_rows(new_rows, value_input_option="USER_ENTERED")
        logger.info("Added %d new categories to Budget tab", len(new_rows))

    # Auto-resize columns
    try:
        from googleapiclient.discovery import build  # type: ignore[import-untyped]
        svc = build("sheets", "v4", credentials=sheets_client._creds)
        svc.spreadsheets().batchUpdate(
            spreadsheetId=sheets_client._spreadsheet_id,
            body={"requests": [{"autoResizeDimensions": {"dimensions": {
                "sheetId": ws.id, "dimension": "COLUMNS",
                "startIndex": 0, "endIndex": 3,
            }}}]},
        ).execute()
    except Exception:
        pass


def _apply_budget_header_formatting(sheets_client: Any, sheet_id: int) -> None:
    """Apply navy bold header to the Budget tab."""
    try:
        from googleapiclient.discovery import build  # type: ignore[import-untyped]
        svc = build("sheets", "v4", credentials=sheets_client._creds)
        navy = {"red": 0.07, "green": 0.12, "blue": 0.22}
        white = {"red": 1.0, "green": 1.0, "blue": 1.0}
        svc.spreadsheets().batchUpdate(
            spreadsheetId=sheets_client._spreadsheet_id,
            body={"requests": [
                {
                    "repeatCell": {
                        "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                        "cell": {"userEnteredFormat": {
                            "backgroundColor": navy,
                            "textFormat": {"foregroundColor": white, "bold": True, "fontSize": 10},
                        }},
                        "fields": "userEnteredFormat(backgroundColor,textFormat)",
                    }
                },
                {
                    "updateSheetProperties": {
                        "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                        "fields": "gridProperties.frozenRowCount",
                    }
                },
            ]},
        ).execute()
    except Exception as e:
        logger.warning("Budget header formatting failed: %s", e)


# ── Compute status ────────────────────────────────────────────────

def compute_budget_status(
    cat_totals: dict[str, float],
    budgets: dict[str, float],
) -> list[dict[str, Any]]:
    """Compute budget status for all tracked categories.

    Returns a list of dicts (one per category that has a budget > 0):
      { category, spent, budget, pct, status }
    where status is 'OK', 'Warning', or 'Over budget'.
    """
    results: list[dict[str, Any]] = []

    for cat, budget in sorted(budgets.items()):
        if budget <= 0:
            continue
        spent = round(cat_totals.get(cat, 0.0), 2)
        pct = round(spent / budget * 100, 1)

        if pct >= 100:
            status = "🔴 Over budget"
        elif pct >= 80:
            status = "🟡 Warning"
        else:
            status = "🟢 OK"

        results.append({
            "category": cat,
            "spent": spent,
            "budget": budget,
            "pct": pct,
            "status": status,
        })

    # Sort: over budget first, then warning, then OK
    order = {"🔴 Over budget": 0, "🟡 Warning": 1, "🟢 OK": 2}
    results.sort(key=lambda x: (order.get(x["status"], 9), -x["pct"]))

    return results
