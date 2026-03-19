"""Unit tests for the CSV parser."""

from pathlib import Path
from textwrap import dedent

import pytest

from spectra.csv_parser import ParsedTransaction, parse_csv, _parse_amount, _parse_date


class TestParseAmount:
    """Test Italian and English amount parsing."""

    def test_italian_negative(self) -> None:
        assert _parse_amount("-4,50") == -4.5

    def test_italian_thousands(self) -> None:
        assert _parse_amount("1.500,00") == 1500.0

    def test_italian_positive_sign(self) -> None:
        assert _parse_amount("+1.500,00") == 1500.0

    def test_english(self) -> None:
        assert _parse_amount("1,500.00") == 1500.0

    def test_negative_english(self) -> None:
        assert _parse_amount("-1,234.56") == -1234.56

    def test_simple_decimal(self) -> None:
        assert _parse_amount("9.99") == 9.99

    def test_with_euro_symbol(self) -> None:
        assert _parse_amount("€ 42,00") == 42.0

    def test_parentheses_negative(self) -> None:
        assert _parse_amount("(100.00)") == -100.0


class TestParseDate:
    """Test date format detection."""

    def test_iso(self) -> None:
        assert _parse_date("2026-02-22") == "2026-02-22"

    def test_eu_slash(self) -> None:
        assert _parse_date("22/02/2026") == "2026-02-22"

    def test_eu_dash(self) -> None:
        assert _parse_date("22-02-2026") == "2026-02-22"

    def test_eu_dot(self) -> None:
        assert _parse_date("22.02.2026") == "2026-02-22"

    def test_compact(self) -> None:
        assert _parse_date("20260222") == "2026-02-22"


class TestParseCsv:
    """Test full CSV parsing with different bank formats."""

    def test_isybank_format(self, tmp_path: Path) -> None:
        """ISyBank / Intesa format: semicolon, Italian amounts."""
        csv = tmp_path / "test.csv"
        csv.write_text(dedent("""\
            Data Operazione;Descrizione;Importo;Valuta
            22/02/2026;POS STARBUCKS;-4,50;EUR
            21/02/2026;STIPENDIO ACME SRL;+1.500,00;EUR
        """))
        txns = parse_csv(csv)
        assert len(txns) == 2
        assert txns[0].amount == -4.5
        assert txns[0].date == "2026-02-22"
        assert txns[1].amount == 1500.0

    def test_english_format(self, tmp_path: Path) -> None:
        """English format: comma-separated, English amounts."""
        csv = tmp_path / "test.csv"
        csv.write_text(dedent("""\
            Date,Description,Amount,Currency
            2026-02-22,STARBUCKS,-4.50,EUR
            2026-02-21,SALARY,1500.00,EUR
        """))
        txns = parse_csv(csv)
        assert len(txns) == 2
        assert txns[0].amount == -4.5
        assert txns[1].amount == 1500.0

    def test_split_debit_credit(self, tmp_path: Path) -> None:
        """Banks that split debit/credit into two columns."""
        csv = tmp_path / "test.csv"
        csv.write_text(dedent("""\
            Data;Descrizione;Addebito;Accredito
            22/02/2026;POS STARBUCKS;4,50;
            21/02/2026;STIPENDIO;;1500,00
        """))
        txns = parse_csv(csv)
        assert len(txns) == 2
        assert txns[0].amount == -4.5  # debit
        assert txns[1].amount == 1500.0  # credit

    def test_dutch_ing_format_with_af_bij(self, tmp_path: Path) -> None:
        """ING NL format: 'Naam / Omschrijving' + 'Af Bij' + 'Bedrag (EUR)'."""
        csv = tmp_path / "test.csv"
        csv.write_text(dedent("""\
            Datum;Naam / Omschrijving;Rekening;Tegenrekening;Code;Af Bij;Bedrag (EUR);Mutatiesoort;Mededelingen;Saldo na mutatie;Tag
            22-02-2026;JUMBO SUPERMARKT;NL00INGB0000000000;;;Af;4,50;Betaalpas;;1000,00;
            21-02-2026;SALARY ACME;NL00INGB0000000000;;;Bij;1.500,00;Overboeking;;2500,00;
        """))
        txns = parse_csv(csv)
        assert len(txns) == 2
        assert txns[0].date == "2026-02-22"
        assert txns[0].raw_description.startswith("JUMBO SUPERMARKT")
        assert "Rekening: NL00INGB0000000000" in txns[0].raw_description
        assert "Mutatiesoort: Betaalpas" in txns[0].raw_description
        assert txns[0].amount == -4.5
        assert "Mutatiesoort: Overboeking" in txns[1].raw_description
        assert txns[1].amount == 1500.0

    def test_long_merchant_name_not_cleaned_to_empty(self, tmp_path: Path) -> None:
        """Long alphabetic merchant names (15+ chars) must be preserved."""
        csv = tmp_path / "test.csv"
        csv.write_text(dedent("""\
            Date;Description;Amount
            2026-02-22;BELASTINGDIENST;-12.00
        """))
        txns = parse_csv(csv)
        assert len(txns) == 1
        assert txns[0].raw_description == "BELASTINGDIENST"

    def test_duplicate_same_day_amount_gets_disambiguated_id(self, tmp_path: Path) -> None:
        """Distinct rows with same date/description/amount must not collapse to one ID."""
        csv = tmp_path / "test.csv"
        csv.write_text(dedent("""\
            Date;Description;Amount;Mededelingen
            2026-02-22;PAYPAL;-2.99;Ref A
            2026-02-22;PAYPAL;-2.99;Ref B
        """))
        txns = parse_csv(csv)
        assert len(txns) == 2
        assert txns[0].id != txns[1].id

    def test_dedup_ids_are_stable(self, tmp_path: Path) -> None:
        """Same data should produce the same IDs."""
        csv = tmp_path / "test.csv"
        csv.write_text(dedent("""\
            Date,Description,Amount
            2026-02-22,STARBUCKS,-4.50
        """))
        txns1 = parse_csv(csv)
        txns2 = parse_csv(csv)
        assert txns1[0].id == txns2[0].id

    def test_empty_rows_skipped(self, tmp_path: Path) -> None:
        """Empty rows should be silently skipped."""
        csv = tmp_path / "test.csv"
        csv.write_text(dedent("""\
            Date,Description,Amount
            2026-02-22,STARBUCKS,-4.50

            2026-02-21,NETFLIX,-9.99
        """))
        txns = parse_csv(csv)
        assert len(txns) == 2

    def test_file_not_found(self) -> None:
        with pytest.raises(FileNotFoundError):
            parse_csv("/nonexistent/file.csv")

    def test_metadata_columns_enrich_description_without_changing_id(self, tmp_path: Path) -> None:
        """Optional metadata should enrich context while preserving dedup ID stability."""
        plain_csv = tmp_path / "plain.csv"
        plain_csv.write_text(dedent("""\
            Date,Description,Amount
            2026-02-22,CONSUMER BOND,-7.50
        """))

        rich_csv = tmp_path / "rich.csv"
        rich_csv.write_text(dedent("""\
            Datum;Naam / Omschrijving;Rekening;Tegenrekening;Code;Af Bij;Bedrag (EUR);Mutatiesoort;Mededelingen;Tag
            22-02-2026;CONSUMER BOND;NL11INGB0000000000;NL22INGB0000000000;SEPA;Af;7,50;Incasso;Contributie;Vast
        """))

        plain_tx = parse_csv(plain_csv)[0]
        rich_tx = parse_csv(rich_csv)[0]

        assert plain_tx.id == rich_tx.id
        assert "Tegenrekening: NL22INGB0000000000" in rich_tx.raw_description
        assert "Code: SEPA" in rich_tx.raw_description
        assert "Mededelingen: Contributie" in rich_tx.raw_description
