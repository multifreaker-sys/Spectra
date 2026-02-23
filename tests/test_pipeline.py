"""Integration test — full pipeline with mocked AI and Sheets."""

from pathlib import Path
from textwrap import dedent
from unittest.mock import MagicMock, patch

import pytest

from prism.config import Settings
from prism.pipeline import run


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        spreadsheet_id="test-sheet-id",
        google_sheets_credentials_b64="",
        google_sheets_credentials_file="dummy.json",
        ai_provider="gemini",
        gemini_api_key="test-gemini-key",
        db_path=tmp_path / "test.db",
        log_level="DEBUG",
    )


@pytest.fixture
def csv_file(tmp_path: Path) -> str:
    csv = tmp_path / "test.csv"
    csv.write_text(dedent("""\
        Data Operazione;Descrizione;Importo;Valuta
        22/02/2026;POS STARBUCKS;-4,50;EUR
        21/02/2026;ADDEBITO SDD NETFLIX.COM;-9,99;EUR
    """))
    return str(csv)


class TestPipelineIntegration:

    @patch("prism.pipeline.SheetsClient")
    @patch("prism.pipeline.categorise")
    def test_full_flow(
        self,
        mock_categorise: MagicMock,
        mock_sheets_cls: MagicMock,
        settings: Settings,
        csv_file: str,
    ) -> None:
        from prism.ai import CategorisedTransaction

        mock_sheets = mock_sheets_cls.return_value
        mock_sheets.get_existing_categories.return_value = ["Bar & Caffè"]
        mock_sheets.append_transactions.return_value = 2

        mock_categorise.return_value = [
            CategorisedTransaction(
                original_description="POS STARBUCKS",
                clean_name="Starbucks",
                category="Bar & Caffè",
                amount=-4.50,
                currency="EUR",
                date="2026-02-22",
            ),
            CategorisedTransaction(
                original_description="ADDEBITO SDD NETFLIX.COM",
                clean_name="Netflix",
                category="Abbonamenti",
                amount=-9.99,
                currency="EUR",
                date="2026-02-21",
            ),
        ]

        run(settings, file=csv_file, currency="EUR", dry_run=False)

        mock_sheets.get_existing_categories.assert_called_once()
        mock_categorise.assert_called_once()
        mock_sheets.append_transactions.assert_called_once()

    @patch("prism.pipeline.SheetsClient")
    @patch("prism.pipeline.categorise")
    def test_dry_run_skips_sheets(
        self,
        mock_categorise: MagicMock,
        mock_sheets_cls: MagicMock,
        settings: Settings,
        csv_file: str,
    ) -> None:
        from prism.ai import CategorisedTransaction

        mock_categorise.return_value = [
            CategorisedTransaction(
                original_description="POS STARBUCKS",
                clean_name="Starbucks",
                category="Bar & Caffè",
                amount=-4.50,
                currency="EUR",
                date="2026-02-22",
            ),
        ]

        run(settings, file=csv_file, currency="EUR", dry_run=True)

        # In dry-run, Sheets should never be instantiated
        mock_sheets_cls.assert_not_called()

    def test_no_duplicate_imports(self, settings: Settings, csv_file: str) -> None:
        """Second import should skip already-seen transactions."""
        from prism.db import BookmarkDB
        from prism.csv_parser import parse_csv

        txns = parse_csv(csv_file)
        with BookmarkDB(settings.db_path) as db:
            db.mark_seen_batch([t.id for t in txns])

        # Now run — should detect all as seen and exit early
        with patch("prism.pipeline.categorise") as mock_cat:
            run(settings, file=csv_file, currency="EUR", dry_run=True)
            mock_cat.assert_not_called()  # AI never called
