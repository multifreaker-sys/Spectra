"""Universal PDF parser — extracts transactions from bank statement PDFs."""

from __future__ import annotations

import logging
import re
from pathlib import Path

from spectra.csv_parser import (
    ParsedTransaction,
    _detect_delimiter,
    _make_id,
    _map_columns,
    _parse_amount,
    _parse_date,
)

logger = logging.getLogger("spectra.pdf_parser")


def parse_pdf(
    file_path: str | Path,
    currency: str = "EUR",
) -> list[ParsedTransaction]:
    """Extract transactions from a bank PDF statement.

    Strategy:
    1. Try pdfplumber table extraction (works on most bank PDFs with clean tables)
    2. Fall back to line-by-line regex parsing (for text-based PDFs)
    """
    try:
        import pdfplumber  # type: ignore[import-untyped]
    except ImportError:
        raise ImportError(
            "pdfplumber is required for PDF support. Install it with:\n"
            "  pip install pdfplumber"
        )
    try:
        from pypdf import PdfReader  # type: ignore[import-untyped]
    except ImportError:
        raise ImportError(
            "pypdf is required for PDF text extraction fallback. Install it with:\n"
            "  pip install pypdf"
        )

    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"PDF file not found: {path}")

    logger.info("Parsing PDF: %s", path.name)

    with pdfplumber.open(path) as pdf:
        # ── Strategy 1: Table extraction ─────────────────────────
        transactions = _extract_from_tables(pdf, currency)
        if transactions:
            logger.info(
                "Table extraction: %d transactions from %s",
                len(transactions), path.name,
            )
            return transactions

        # ── Strategy 2: Text-based regex extraction ───────────────
        logger.info("No tables found, falling back to text extraction")
        transactions = _extract_from_text_with_pypdf(path, PdfReader, currency)
        logger.info(
            "Text extraction: %d transactions from %s",
            len(transactions), path.name,
        )
        return transactions


# ── Strategy 1: Table extraction ─────────────────────────────────


def _extract_from_tables(pdf: Any, currency: str) -> list[ParsedTransaction]:
    """Try to extract transaction tables from each PDF page."""
    all_rows: list[list[str]] = []
    header: list[str] | None = None

    for page in pdf.pages:
        tables = page.extract_tables()
        for table in tables:
            if not table or len(table) < 2:
                continue

            # Find the header row (first row with recognizable column names)
            potential_header = [str(c or "").strip() for c in table[0]]
            col_map = _map_columns(potential_header)

            if "date" in col_map and ("amount" in col_map or "credit" in col_map or "debit" in col_map):
                if header is None:
                    header = potential_header
                # Add data rows (skip header row of subsequent tables)
                data_start = 1 if header == potential_header or not all_rows else 0
                for row in table[data_start:]:
                    all_rows.append([str(c or "").strip() for c in row])

    if not all_rows or header is None:
        return []

    return _rows_to_transactions(header, all_rows, currency)


def _rows_to_transactions(
    header: list[str],
    rows: list[list[str]],
    currency: str,
) -> list[ParsedTransaction]:
    """Convert table rows to ParsedTransaction objects using column mapping."""
    col = _map_columns(header)
    transactions: list[ParsedTransaction] = []
    skipped = 0

    for row in rows:
        if not any(c.strip() for c in row):
            continue
        if len(row) <= max(col.values()):
            continue

        try:
            raw_date = row[col["date"]].strip()
            if not raw_date:
                continue
            date = _parse_date(raw_date)
            description = row[col.get("description", -1)].strip() if "description" in col else ""

            if "amount" in col:
                raw_amount = row[col["amount"]].strip()
                if not raw_amount:
                    continue
                amount = _parse_amount(raw_amount)
            else:
                raw_credit = row[col["credit"]].strip() if "credit" in col else ""
                raw_debit = row[col["debit"]].strip() if "debit" in col else ""
                credit = _parse_amount(raw_credit) if raw_credit else 0.0
                debit = _parse_amount(raw_debit) if raw_debit else 0.0
                amount = abs(credit) - abs(debit)

            transactions.append(ParsedTransaction(
                id=_make_id(date, description, amount),
                date=date,
                amount=amount,
                currency=currency,
                raw_description=description,
            ))
        except (ValueError, IndexError) as e:
            skipped += 1
            logger.debug("Skipping row: %s — %s", row, e)

    if skipped:
        logger.warning("Skipped %d malformed rows", skipped)

    return transactions


# ── Strategy 2: Text-based regex extraction ───────────────────────


# Pattern that matches a line starting with a date, then description, then amount(s)
# Handles most Italian/European bank statement formats
_TX_PATTERN = re.compile(
    r"(?P<date>\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4}|\d{4}[/\-\.]\d{2}[/\-\.]\d{2})"  # date
    r"\s+"
    r"(?P<desc>.+?)"  # description (lazy)
    r"\s+"
    r"(?P<amount>[+\-]?\s*[\d\.,]+)"  # amount
    r"\s*(?:EUR|USD|GBP|CHF)?\s*$",  # optional currency
    re.IGNORECASE,
)


def _extract_from_text_with_pypdf(
    file_path: Path,
    pdf_reader_cls: Any,
    currency: str,
) -> list[ParsedTransaction]:
    """Regex-based fallback: scan each line for date + description + amount."""
    transactions: list[ParsedTransaction] = []
    reader = pdf_reader_cls(str(file_path))

    for page in reader.pages:
        text = page.extract_text() or ""
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue

            m = _TX_PATTERN.match(line)
            if not m:
                continue

            try:
                date = _parse_date(m.group("date"))
                description = m.group("desc").strip()
                amount = _parse_amount(m.group("amount").replace(" ", ""))

                transactions.append(ParsedTransaction(
                    id=_make_id(date, description, amount),
                    date=date,
                    amount=amount,
                    currency=currency,
                    raw_description=description,
                ))
            except ValueError:
                continue

    return transactions


# Type alias used above (avoid circular import with typing)
from typing import Any  # noqa: E402
