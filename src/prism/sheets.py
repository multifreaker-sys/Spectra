"""Google Sheets integration — read categories, append categorised rows, apply formatting."""

from __future__ import annotations

import base64
import json
import logging
import tempfile
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
]

# Header background colour — deep navy
_HEADER_BG = {"red": 0.07, "green": 0.12, "blue": 0.22}
_HEADER_FG = {"red": 1.0, "green": 1.0, "blue": 1.0}


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
        self._sheet = self._spreadsheet.sheet1  # main worksheet
        logger.info("Connected to spreadsheet: %s", self._spreadsheet.title)

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

    # ── Sheets API helper ─────────────────────────────────────────

    def _service(self) -> Any:
        """Return a Google Sheets API v4 service object."""
        from googleapiclient.discovery import build  # type: ignore[import-untyped]
        return build("sheets", "v4", credentials=self._creds)

    # ── Read ─────────────────────────────────────────────────────

    def get_existing_categories(self) -> list[str]:
        """Read unique categories already present in the Category column."""
        all_values = self._sheet.get_all_values()
        if not all_values:
            return []

        # Support both English and legacy Italian column names
        header = all_values[0]
        col_name = "Category" if "Category" in header else "Categoria"
        try:
            cat_idx = header.index(col_name)
        except ValueError:
            logger.warning("'Category' column not found in header: %s", header)
            return []

        categories: set[str] = set()
        for row in all_values[1:]:
            if cat_idx < len(row) and row[cat_idx].strip():
                categories.add(row[cat_idx].strip())

        logger.info("Found %d existing categories", len(categories))
        return sorted(categories)

    def get_all_rows(self) -> list[list[str]]:
        """Return all rows (including header) from the sheet."""
        return self._sheet.get_all_values()

    # ── Write ────────────────────────────────────────────────────

    def ensure_header(self) -> None:
        """Insert the header at row 1 if it's not already there."""
        all_values = self._sheet.get_all_values()
        first_row = all_values[0] if all_values else []
        has_header = "Date" in first_row or "Categoria" in first_row

        if not has_header:
            # Insert header at the very top (row 1)
            self._sheet.insert_row(_HEADER, index=1)
            logger.info("Header row inserted at row 1")
            self._apply_header_formatting()

    def _apply_header_formatting(self) -> None:
        """Bold + colour the header row, freeze it, rename sheet, colour amounts."""
        try:
            sheet_id = self._sheet.id
            service = self._service()
            requests: list[dict] = [
                # Rename sheet to "Transactions"
                {
                    "updateSheetProperties": {
                        "properties": {"sheetId": sheet_id, "title": "Transactions"},
                        "fields": "title",
                    }
                },
                # Bold + navy background on header row
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 0, "endRowIndex": 1,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": _HEADER_BG,
                                "textFormat": {
                                    "foregroundColor": _HEADER_FG,
                                    "bold": True,
                                    "fontSize": 10,
                                },
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat)",
                    }
                },
                # Freeze row 1
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "gridProperties": {"frozenRowCount": 1},
                        },
                        "fields": "gridProperties.frozenRowCount",
                    }
                },
                # Auto-resize columns A–G
                {
                    "autoResizeDimensions": {
                        "dimensions": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": 0,
                            "endIndex": 7,
                        }
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
            logger.info("Header formatting applied")
        except Exception as e:
            logger.warning("Could not apply header formatting: %s", e)

    def append_transactions(
        self, transactions: list[CategorisedTransaction]
    ) -> int:
        """Append categorised transactions as new rows. Returns count."""
        if not transactions:
            return 0

        self.ensure_header()

        rows: list[list[Any]] = [
            [
                t.date, t.original_description, t.clean_name, t.category,
                t.amount, t.currency,
                "🔄" if t.recurring else "",
            ]
            for t in transactions
        ]

        self._sheet.append_rows(rows, value_input_option="USER_ENTERED")
        logger.info("Appended %d row(s) to Google Sheets", len(rows))
        return len(rows)
