"""Google Sheets integration — read categories, append categorised rows."""

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

# Default header row (created automatically if the sheet is empty)
_HEADER = [
    "Date",
    "Original Description",
    "Merchant",
    "Category",
    "Amount",
    "Currency",
]


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
    def _load_credentials(
        b64: str, filepath: str
    ) -> Credentials:
        """Load service-account credentials from base64 string or file."""
        if b64:
            raw = base64.b64decode(b64)
            info = json.loads(raw)
            # Write to a temp file for gspread compatibility
            tmp = tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False
            )
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

    # ── Read ─────────────────────────────────────────────────────

    def get_existing_categories(self) -> list[str]:
        """Read unique categories already present in the Categoria column."""
        all_values = self._sheet.get_all_values()
        if not all_values:
            return []

        # Find the "Category" column index (support both English and legacy Italian)
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
        """Create the header row if the sheet is empty."""
        all_values = self._sheet.get_all_values()
        if not all_values:
            self._sheet.append_row(_HEADER)
            logger.info("Header row created")

    def append_transactions(
        self, transactions: list[CategorisedTransaction]
    ) -> int:
        """Append categorised transactions as new rows. Returns count."""
        if not transactions:
            return 0

        self.ensure_header()

        rows: list[list[Any]] = []
        for t in transactions:
            rows.append([
                t.date,
                t.original_description,
                t.clean_name,
                t.category,
                t.amount,
                t.currency,
            ])

        # Batch append for efficiency
        self._sheet.append_rows(rows, value_input_option="USER_ENTERED")
        logger.info("Appended %d row(s) to Google Sheets", len(rows))
        return len(rows)
