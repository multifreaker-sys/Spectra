"""Google Sheets integration — year-aware sheets, formatting, per-year tabs."""

from __future__ import annotations

import base64
import json
import logging
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

from prism.ai import CategorisedTransaction

logger = logging.getLogger("prism.sheets")

# Google API scopes needed
_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Default header row
_HEADER = [
    "Date",
    "Original Description",
    "Merchant",
    "Category",
    "Amount",
    "Currency",
    "Recurring",
    "Original Amt",
    "Original Cur",
]

# Header styling — deep navy
_HEADER_BG = {"red": 0.07, "green": 0.12, "blue": 0.22}
_HEADER_FG = {"red": 1.0, "green": 1.0, "blue": 1.0}

_SHEET_PREFIX = "Transactions"


def _year_title(year: int) -> str:
    return f"{_SHEET_PREFIX} {year}"


def _is_transaction_sheet(title: str) -> bool:
    return title.startswith(_SHEET_PREFIX)


class SheetsClient:
    """Read / write a Google Sheets spreadsheet."""

    def __init__(
        self,
        spreadsheet_id: str,
        credentials_b64: str = "",
        credentials_file: str = "credentials.json",
    ) -> None:
        self._spreadsheet_id = spreadsheet_id
        self._creds = self._load_credentials(credentials_b64, credentials_file)
        self._gc = gspread.authorize(self._creds)
        self._spreadsheet = self._gc.open_by_key(spreadsheet_id)
        logger.info("Connected to spreadsheet: %s", self._spreadsheet.title)

        # Rename legacy "Transactions" tab (no year) → "Transactions YYYY"
        self._migrate_legacy_sheet()

    # ── Credentials ──────────────────────────────────────────────

    @staticmethod
    def _load_credentials(b64: str, filepath: str) -> Credentials:
        """Load service-account credentials from base64 string or file."""
        if b64:
            raw = base64.b64decode(b64)
            info = json.loads(raw)
            tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
            json.dump(info, tmp)
            tmp.flush()
            filepath = tmp.name
            logger.debug("Credentials decoded from base64")

        if not Path(filepath).exists():
            raise FileNotFoundError(
                f"Google credentials file not found: {filepath}. "
                "Set GOOGLE_SHEETS_CREDENTIALS_B64 or GOOGLE_SHEETS_CREDENTIALS_FILE."
            )

        creds = Credentials.from_service_account_file(filepath, scopes=_SCOPES)
        logger.debug("Credentials loaded from %s", filepath)
        return creds

    # ── Migration ─────────────────────────────────────────────────

    def _migrate_legacy_sheet(self) -> None:
        """Rename 'Transactions' (no year) → 'Transactions YYYY' based on first data row."""
        try:
            ws = self._spreadsheet.worksheet("Transactions")
        except Exception:
            return  # Already migrated or doesn't exist

        # Detect year from first data row
        rows = ws.get_all_values()
        year = None
        for row in rows[1:]:
            if row and row[0].strip():
                try:
                    year = int(row[0].strip()[:4])
                    break
                except ValueError:
                    pass

        new_title = _year_title(year) if year else _year_title(2026)
        ws.update_title(new_title)
        logger.info("Migrated sheet 'Transactions' → '%s'", new_title)

    # ── Sheets API helper ─────────────────────────────────────────

    def _service(self) -> Any:
        """Return a Google Sheets API v4 service object."""
        from googleapiclient.discovery import build  # type: ignore[import-untyped]
        return build("sheets", "v4", credentials=self._creds)

    # ── Year-aware sheet management ───────────────────────────────

    def _get_or_create_year_sheet(self, year: int) -> gspread.Worksheet:
        """Return the worksheet for `year`, creating it if it doesn't exist."""
        title = _year_title(year)
        try:
            return self._spreadsheet.worksheet(title)
        except Exception:
            ws = self._spreadsheet.add_worksheet(title=title, rows=5000, cols=20)
            logger.info("Created new sheet: %s", title)
            return ws

    # ── Read ─────────────────────────────────────────────────────

    def get_existing_categories(self) -> list[str]:
        """Read unique categories from ALL Transactions YYYY sheets."""
        categories: set[str] = set()
        for ws in self._spreadsheet.worksheets():
            if not _is_transaction_sheet(ws.title):
                continue
            all_values = ws.get_all_values()
            if not all_values:
                continue
            header = all_values[0]
            col_name = "Category" if "Category" in header else "Categoria"
            try:
                cat_idx = header.index(col_name)
            except ValueError:
                continue
            for row in all_values[1:]:
                if cat_idx < len(row) and row[cat_idx].strip():
                    categories.add(row[cat_idx].strip())

        logger.info("Found %d existing categories", len(categories))
        return sorted(categories)

    def get_all_rows(self) -> list[list[str]]:
        """Return all transaction rows (header from first sheet) from ALL year sheets."""
        all_rows: list[list[str]] = []
        header_written = False

        for ws in sorted(self._spreadsheet.worksheets(), key=lambda w: w.title):
            if not _is_transaction_sheet(ws.title):
                continue
            rows = ws.get_all_values()
            if not rows:
                continue
            if not header_written:
                all_rows.append(rows[0])  # Header once
                header_written = True
            all_rows.extend(rows[1:])   # Data from every year sheet

        return all_rows

    # ── Write ────────────────────────────────────────────────────

    def _ensure_header_on(self, ws: gspread.Worksheet) -> None:
        """Insert header at row 1 of `ws` if not already present."""
        all_values = ws.get_all_values()
        first_row = all_values[0] if all_values else []
        has_header = "Date" in first_row or "Categoria" in first_row

        if not has_header:
            ws.insert_row(_HEADER, index=1)
            logger.info("Header row inserted on '%s'", ws.title)
            self._apply_header_formatting(ws.id, ws.title)

    # Keep old name for compatibility with tests
    def ensure_header(self) -> None:
        # NO-OP: use _ensure_header_on(ws) for year sheets
        pass

    def _apply_header_formatting(self, sheet_id: int, sheet_title: str) -> None:
        """Wipe stale formats, apply navy bold header + freeze + conditional amounts."""
        try:
            service = self._service()
            requests: list[dict] = [
                # Rename to correct title (already set, but keep idempotent)
                {
                    "updateSheetProperties": {
                        "properties": {"sheetId": sheet_id, "title": sheet_title},
                        "fields": "title",
                    }
                },
                # Clear ALL existing cell formatting first
                {
                    "repeatCell": {
                        "range": {"sheetId": sheet_id},
                        "cell": {"userEnteredFormat": {}},
                        "fields": "userEnteredFormat",
                    }
                },
                # Navy bold on header row
                {
                    "repeatCell": {
                        "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                        "cell": {"userEnteredFormat": {
                            "backgroundColor": _HEADER_BG,
                            "textFormat": {"foregroundColor": _HEADER_FG, "bold": True, "fontSize": 10},
                        }},
                        "fields": "userEnteredFormat(backgroundColor,textFormat)",
                    }
                },
                # Freeze row 1
                {
                    "updateSheetProperties": {
                        "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                        "fields": "gridProperties.frozenRowCount",
                    }
                },
                # Amount col (E = index 4): green if positive
                {
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [{"sheetId": sheet_id, "startColumnIndex": 4, "endColumnIndex": 5}],
                            "booleanRule": {
                                "condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]},
                                "format": {
                                    "textFormat": {"foregroundColor": {"red": 0.13, "green": 0.54, "blue": 0.13}},
                                    "backgroundColor": {"red": 0.90, "green": 0.97, "blue": 0.90},
                                },
                            },
                        },
                        "index": 0,
                    }
                },
                # Amount col (E = index 4): red if negative
                {
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [{"sheetId": sheet_id, "startColumnIndex": 4, "endColumnIndex": 5}],
                            "booleanRule": {
                                "condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]},
                                "format": {
                                    "textFormat": {"foregroundColor": {"red": 0.76, "green": 0.15, "blue": 0.15}},
                                    "backgroundColor": {"red": 1.0, "green": 0.90, "blue": 0.90},
                                },
                            },
                        },
                        "index": 1,
                    }
                },
            ]
            service.spreadsheets().batchUpdate(
                spreadsheetId=self._spreadsheet_id,
                body={"requests": requests},
            ).execute()
            logger.info("Header formatting applied on '%s'", sheet_title)
        except Exception as e:
            logger.warning("Could not apply header formatting: %s", e)

    def _auto_resize(self, sheet_id: int, n_cols: int = 7) -> None:
        """Auto-resize columns to fit content."""
        try:
            service = self._service()
            service.spreadsheets().batchUpdate(
                spreadsheetId=self._spreadsheet_id,
                body={"requests": [{"autoResizeDimensions": {"dimensions": {
                    "sheetId": sheet_id, "dimension": "COLUMNS",
                    "startIndex": 0, "endIndex": n_cols,
                }}}]},
            ).execute()
        except Exception as e:
            logger.warning("Auto-resize failed: %s", e)

    def append_transactions(self, transactions: list[CategorisedTransaction]) -> int:
        """Append categorised transactions to the right year sheet. Returns total count."""
        if not transactions:
            return 0

        # Group by year
        by_year: dict[int, list[CategorisedTransaction]] = defaultdict(list)
        for t in transactions:
            try:
                year = int(t.date[:4])
            except (ValueError, IndexError):
                year = 2026
            by_year[year].append(t)

        total = 0
        for year, txns in sorted(by_year.items()):
            ws = self._get_or_create_year_sheet(year)
            self._ensure_header_on(ws)

            rows: list[list[Any]] = [
                [
                    t.date, t.original_description, t.clean_name, t.category,
                    t.amount, t.currency, t.recurring,
                    t.original_amount if t.original_amount is not None else "",
                    t.original_currency if t.original_currency is not None else "",
                ]
                for t in txns
            ]
            ws.append_rows(rows, value_input_option="USER_ENTERED")
            logger.info("Appended %d row(s) to '%s'", len(rows), ws.title)

            # Resize after data is written (9 columns: index 0 to 8)
            self._auto_resize(ws.id, n_cols=9)
            total += len(rows)

        return total

    def sync_category_colors(self) -> None:
        """Apply consistent pastel background colors for categories across all sheets."""
        try:
            service = self._service()
            meta = service.spreadsheets().get(
                spreadsheetId=self._spreadsheet_id,
                fields="sheets(properties,conditionalFormats)"
            ).execute()
            
            categories = self.get_existing_categories()
            if not categories:
                return

            # Very light, professional pastel colors
            palette = [
                {"red": 0.89, "green": 0.93, "blue": 0.96},  # Soft Blue
                {"red": 0.93, "green": 0.91, "blue": 0.95},  # Soft Purple
                {"red": 0.91, "green": 0.96, "blue": 0.92},  # Soft Mint
                {"red": 0.98, "green": 0.92, "blue": 0.90},  # Soft Peach
                {"red": 0.98, "green": 0.96, "blue": 0.89},  # Soft Yellow
                {"red": 0.90, "green": 0.95, "blue": 0.95},  # Soft Teal
                {"red": 0.96, "green": 0.92, "blue": 0.94},  # Soft Pink
                {"red": 0.94, "green": 0.94, "blue": 0.94},  # Soft Gray
                {"red": 0.98, "green": 0.92, "blue": 0.85},  # Pale Orange
                {"red": 0.91, "green": 0.94, "blue": 0.91},  # Pale Sage
                {"red": 0.97, "green": 0.94, "blue": 0.96},  # Pale Lavender
                {"red": 0.96, "green": 0.96, "blue": 0.93},  # Warm Sand
            ]

            def get_color(cat: str) -> dict[str, float]:
                idx = sum(ord(c) for c in cat) % len(palette)
                return palette[idx]

            requests: list[dict[str, Any]] = []

            for sheet_meta in meta.get("sheets", []):
                props = sheet_meta.get("properties", {})
                sheet_id = props.get("sheetId")
                title = props.get("title", "")
                existing_rules = sheet_meta.get("conditionalFormats", [])
                
                is_tx = _is_transaction_sheet(title)
                is_dash = (title == "Dashboard")

                if not (is_tx or is_dash):
                    continue

                # 1. Delete all existing formatting rules to avoid duplicates
                for i in range(len(existing_rules) - 1, -1, -1):
                    requests.append({"deleteConditionalFormatRule": {"sheetId": sheet_id, "index": i}})

                # 2. Re-add Amount conditional formatting (green/red) only for Transactions sheets (Column E = index 4)
                if is_tx:
                    requests.append({
                        "addConditionalFormatRule": {
                            "rule": {
                                "ranges": [{"sheetId": sheet_id, "startColumnIndex": 4, "endColumnIndex": 5}],
                                "booleanRule": {
                                    "condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]},
                                    "format": {
                                        "textFormat": {"foregroundColor": {"red": 0.13, "green": 0.54, "blue": 0.13}},
                                        "backgroundColor": {"red": 0.90, "green": 0.97, "blue": 0.90},
                                    }
                                }
                            },
                            "index": 0
                        }
                    })
                    requests.append({
                        "addConditionalFormatRule": {
                            "rule": {
                                "ranges": [{"sheetId": sheet_id, "startColumnIndex": 4, "endColumnIndex": 5}],
                                "booleanRule": {
                                    "condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]},
                                    "format": {
                                        "textFormat": {"foregroundColor": {"red": 0.76, "green": 0.15, "blue": 0.15}},
                                        "backgroundColor": {"red": 1.0, "green": 0.90, "blue": 0.90},
                                    }
                                }
                            },
                            "index": 0
                        }
                    })

                # 3. Add Category color rules (Column D = index 3 for TX, Column A = index 0 for Dash)
                cat_col = 3 if is_tx else 0
                for cat in categories:
                    requests.append({
                        "addConditionalFormatRule": {
                            "rule": {
                                "ranges": [{"sheetId": sheet_id, "startColumnIndex": cat_col, "endColumnIndex": cat_col + 1}],
                                "booleanRule": {
                                    "condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": cat}]},
                                    "format": {
                                        "backgroundColor": get_color(cat),
                                        "textFormat": {"foregroundColor": {"red": 0.15, "green": 0.15, "blue": 0.15}}
                                    }
                                }
                            },
                            "index": 0
                        }
                    })

            if requests:
                service.spreadsheets().batchUpdate(
                    spreadsheetId=self._spreadsheet_id,
                    body={"requests": requests}
                ).execute()
                logger.info("Category colors synced across all sheets")

        except Exception as e:
            logger.warning("Failed to sync category colors: %s", e)
