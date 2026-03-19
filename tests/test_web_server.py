from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from spectra.config import Settings
from spectra.db import BookmarkDB
from spectra.web import server


@pytest.fixture
def web_settings(tmp_path: Path) -> Settings:
    creds = tmp_path / "dummy.json"
    creds.write_text("{}")
    return Settings(
        ai_provider="local",
        spreadsheet_id="",
        google_sheets_credentials_file=str(creds),
        db_path=tmp_path / "web.db",
        log_level="DEBUG",
    )


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, web_settings: Settings) -> TestClient:
    monkeypatch.setattr(server, "load_settings", lambda: web_settings)
    return TestClient(server.app)


def seed_tx(
    db: BookmarkDB,
    *,
    tx_id: str,
    tx_date: str,
    merchant: str,
    amount: float,
    category: str,
    original_description: str,
) -> None:
    db.save_history(
        [
            SimpleNamespace(
                id=tx_id,
                date=tx_date,
                clean_name=merchant,
                amount=amount,
                category=category,
                original_description=original_description,
            )
        ]
    )


def test_patch_transaction_persists_learning(client: TestClient, web_settings: Settings) -> None:
    with BookmarkDB(web_settings.db_path) as db:
        seed_tx(
            db,
            tx_id="tx-1",
            tx_date="2026-03-10",
            merchant="Netflix.Com",
            amount=-12.99,
            category="Uncategorized",
            original_description="ADDEBITO SDD NETFLIX.COM",
        )

    response = client.patch(
        "/api/transactions/tx-1",
        json={"merchant": "Netflix", "category": "Digital Subscriptions", "apply_to_future": True},
    )
    assert response.status_code == 200
    assert response.json()["ok"] is True

    with BookmarkDB(web_settings.db_path) as db:
        row = db._conn.execute(
            "SELECT clean_name, category FROM tx_history WHERE tx_id = 'tx-1'"
        ).fetchone()
        assert row == ("Netflix", "Digital Subscriptions")
        assert db.get_merchant_categories()["Netflix"] == "Digital Subscriptions"
        assert db.get_overrides()["ADDEBITO SDD NETFLIX.COM"]["category"] == "Digital Subscriptions"

        learning = db.get_recent_learning_feedback(limit=5)
        assert learning[0]["source"] == "manual_edit"
        assert learning[0]["apply_to_future"] is True


def test_rule_lifecycle_and_reapply_history(client: TestClient, web_settings: Settings) -> None:
    with BookmarkDB(web_settings.db_path) as db:
        seed_tx(
            db,
            tx_id="tx-rule",
            tx_date="2026-03-09",
            merchant="Amzn Mktp",
            amount=-45.0,
            category="Uncategorized",
            original_description="AMZN MKTP DIGITAL",
        )

    create_response = client.post(
        "/api/settings/rules",
        json={"rule_type": "contains", "pattern": "amzn", "category": "Shopping"},
    )
    assert create_response.status_code == 200
    rule_id = create_response.json()["rule"]["id"]

    test_response = client.post(
        "/api/settings/rules/test",
        json={"rule_type": "contains", "pattern": "amzn", "sample_text": "AMZN MKTP DIGITAL"},
    )
    assert test_response.status_code == 200
    preview = test_response.json()
    assert preview["matches_sample"] is True
    assert preview["impact_count"] >= 1

    disable_response = client.patch(f"/api/settings/rules/{rule_id}", json={"is_active": False})
    assert disable_response.status_code == 200
    assert disable_response.json()["rule"]["is_active"] is False

    enable_response = client.patch(f"/api/settings/rules/{rule_id}", json={"is_active": True})
    assert enable_response.status_code == 200
    assert enable_response.json()["rule"]["is_active"] is True

    reapply_response = client.post("/api/settings/learning/reapply")
    assert reapply_response.status_code == 200
    assert reapply_response.json()["updated"] >= 1

    with BookmarkDB(web_settings.db_path) as db:
        category = db._conn.execute(
            "SELECT category FROM tx_history WHERE tx_id = 'tx-rule'"
        ).fetchone()[0]
        assert category == "Shopping"


def test_summary_and_subscriptions_surface_signals(client: TestClient, web_settings: Settings) -> None:
    today = date.today()
    current_day = today.isoformat()
    prior_cycle_day = (today - timedelta(days=32)).isoformat()
    two_cycles_back_day = (today - timedelta(days=64)).isoformat()

    with BookmarkDB(web_settings.db_path) as db:
        db.save_budget_limit("Food & Dining", 100.0)
        seed_tx(
            db,
            tx_id="tx-food-current",
            tx_date=current_day,
            merchant="Starbucks",
            amount=-80.0,
            category="Food & Dining",
            original_description="POS STARBUCKS",
        )
        seed_tx(
            db,
            tx_id="tx-food-prev",
            tx_date=prior_cycle_day,
            merchant="Starbucks",
            amount=-20.0,
            category="Food & Dining",
            original_description="POS STARBUCKS",
        )
        seed_tx(
            db,
            tx_id="tx-uncat",
            tx_date=current_day,
            merchant="Unknown Merchant",
            amount=-12.0,
            category="Uncategorized",
            original_description="RANDOM UNKNOWN PURCHASE",
        )
        seed_tx(
            db,
            tx_id="sub-old",
            tx_date=two_cycles_back_day,
            merchant="Netflix",
            amount=-9.99,
            category="Digital Subscriptions",
            original_description="NETFLIX.COM",
        )
        seed_tx(
            db,
            tx_id="sub-prev",
            tx_date=prior_cycle_day,
            merchant="Netflix",
            amount=-9.99,
            category="Digital Subscriptions",
            original_description="NETFLIX.COM",
        )
        seed_tx(
            db,
            tx_id="sub-current",
            tx_date=current_day,
            merchant="Netflix",
            amount=-14.99,
            category="Digital Subscriptions",
            original_description="NETFLIX.COM",
        )

    summary_response = client.get("/api/summary?scope=cycle")
    assert summary_response.status_code == 200
    summary = summary_response.json()
    assert isinstance(summary["insights"], list)
    insight_types = {item["type"] for item in summary["insights"]}
    assert "uncategorized" in insight_types

    subscriptions_response = client.get("/api/subscriptions")
    assert subscriptions_response.status_code == 200
    subscriptions = subscriptions_response.json()
    assert subscriptions["summary"]["price_change_count"] >= 1
    netflix = next(item for item in subscriptions["items"] if item["merchant"] == "Netflix")
    assert netflix["price_change_direction"] == "up"
    assert netflix["change_amount"] > 0


def test_confirm_respects_apply_to_future(client: TestClient, web_settings: Settings) -> None:
    payload = {
        "transactions": [
            {
                "id": "upload-1",
                "date": "2026-03-11",
                "merchant": "Spotify",
                "category": "Digital Subscriptions",
                "amount": -9.99,
                "currency": "EUR",
                "recurring": "Subscription",
                "original_description": "SPOTIFY AB",
                "apply_to_future": True,
            },
            {
                "id": "upload-2",
                "date": "2026-03-11",
                "merchant": "One-off Store",
                "category": "Shopping",
                "amount": -49.0,
                "currency": "EUR",
                "recurring": "",
                "original_description": "ONE OFF STORE",
                "apply_to_future": False,
            },
        ]
    }

    response = client.post("/api/confirm", json=payload)
    assert response.status_code == 200
    assert response.json()["ok"] is True

    with BookmarkDB(web_settings.db_path) as db:
        assert db._conn.execute("SELECT COUNT(*) FROM tx_history").fetchone()[0] == 2
        merchant_categories = db.get_merchant_categories()
        assert merchant_categories["Spotify"] == "Digital Subscriptions"
        assert "One-off Store" not in merchant_categories

        overrides = db.get_overrides()
        assert overrides["SPOTIFY AB"]["category"] == "Digital Subscriptions"
        assert "ONE OFF STORE" not in overrides

        learning = db.get_recent_learning_feedback(limit=10)
        assert len(learning) >= 2
        assert any(event["clean_name"] == "One-off Store" and event["apply_to_future"] is False for event in learning)


def test_invalid_scope_returns_structured_error_with_request_id(client: TestClient) -> None:
    response = client.get("/api/summary?scope=invalid")
    assert response.status_code == 400

    payload = response.json()
    assert payload["ok"] is False
    assert payload["error"]["code"] == "invalid_scope"
    assert payload["error"]["request_id"]
    assert response.headers["X-Request-ID"] == payload["error"]["request_id"]


def test_upload_rejects_unsupported_file_type_with_structured_error(client: TestClient) -> None:
    response = client.post(
        "/api/upload",
        files={"file": ("notes.txt", b"hello", "text/plain")},
    )
    assert response.status_code == 400
    payload = response.json()
    assert payload["ok"] is False
    assert payload["error"]["code"] == "unsupported_file_type"


def test_upload_stream_reports_parsed_and_duplicate_counts(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    web_settings: Settings,
) -> None:
    with BookmarkDB(web_settings.db_path) as db:
        seed_tx(
            db,
            tx_id="dup-1",
            tx_date="2026-03-01",
            merchant="Known Merchant",
            amount=-10.0,
            category="Shopping",
            original_description="KNOWN DUPLICATE",
        )

    monkeypatch.setattr(
        "spectra.csv_parser.parse_csv",
        lambda *args, **kwargs: [
            SimpleNamespace(
                id="dup-1",
                date="2026-03-01",
                amount=-10.0,
                currency="EUR",
                raw_description="KNOWN DUPLICATE",
            )
        ],
    )

    response = client.post(
        "/api/upload",
        files={"file": ("test.csv", b"date,desc,amount\n2026-03-01,KNOWN DUPLICATE,-10.0\n", "text/csv")},
    )
    assert response.status_code == 200

    events = [
        json.loads(line[6:])
        for line in response.text.splitlines()
        if line.startswith("data: ")
    ]
    assert events
    final = events[-1]
    assert final["done"] is True
    assert final["parsed_total"] == 1
    assert final["new_total"] == 0
    assert final["duplicate_total"] == 1
    assert final["transactions"] == []


def test_error_watcher_endpoint_surfaces_recent_errors(client: TestClient) -> None:
    # Trigger a validation error event first.
    invalid = client.post(
        "/api/settings/rules",
        json={"rule_type": "contains", "pattern": "", "category": ""},
    )
    assert invalid.status_code == 400

    watcher = client.get("/api/settings/errors?window_hours=24&limit=20")
    assert watcher.status_code == 200
    data = watcher.json()
    assert data["ok"] is True
    assert data["watcher_enabled"] is True
    assert data["total_events"] >= 1
    assert any(event["error_code"] == "validation_error" for event in data["recent_events"])


def test_can_store_language_preference(client: TestClient) -> None:
    response = client.patch(
        "/api/settings/preferences",
        json={"language_preference": "nl"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["language_preference"] == "nl"


def test_transactions_include_booking_time_hint(client: TestClient, web_settings: Settings) -> None:
    with BookmarkDB(web_settings.db_path) as db:
        seed_tx(
            db,
            tx_id="tx-time-hint",
            tx_date="2026-03-12",
            merchant="Cafe Test",
            amount=-8.5,
            category="Food & Dining",
            original_description=(
                "Online bankieren | Naam: Cafe Test | Omschrijving: Lunch | "
                "Tegenrekening: NL11TEST0000000001 | Code: SEPA | "
                "Mededelingen: Factuur 123 | Tag: Zakelijk | "
                "Datum/Tijd: 12-03-2026 14:37:12 | Valutadatum: 12-03-2026"
            ),
        )

    response = client.get("/api/transactions")
    assert response.status_code == 200
    payload = response.json()
    row = next(item for item in payload["transactions"] if item["id"] == "tx-time-hint")
    assert row["booking_time"] == "14:37"
    assert row["details"]["payment_method"] == "Online bankieren"
    assert row["details"]["counterparty"] == "Cafe Test"
    assert row["details"]["counter_account"] == "NL11TEST0000000001"
    assert row["details"]["transaction_code"] == "SEPA"
    assert row["details"]["message"] == "Factuur 123"
    assert row["details"]["tag"] == "Zakelijk"
    assert row["details"]["value_date"] == "2026-03-12"
    assert "NL11TEST0000000001" in row["details"]["iban_candidates"]
    assert row["details"]["structured_fields"]["Naam"] == "Cafe Test"


def test_transactions_use_counterparty_for_generic_merchant_name(
    client: TestClient,
    web_settings: Settings,
) -> None:
    with BookmarkDB(web_settings.db_path) as db:
        seed_tx(
            db,
            tx_id="tx-incasso-counterparty",
            tx_date="2026-03-11",
            merchant="Incasso",
            amount=-7.5,
            category="Education",
            original_description="Consumentenbond Incasso",
        )

    response = client.get("/api/transactions?search=consumentenbond")
    assert response.status_code == 200
    payload = response.json()
    row = next(item for item in payload["transactions"] if item["id"] == "tx-incasso-counterparty")
    assert row["merchant"] == "Consumentenbond"
    assert row["details"]["payment_method"] == "Incasso"
    assert row["details"]["counterparty"] == "Consumentenbond"
