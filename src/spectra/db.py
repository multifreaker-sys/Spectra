"""SQLite bookmark — tracks which transactions have already been imported."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger("spectra.db")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS seen_transactions (
    tx_id       TEXT PRIMARY KEY,
    source      TEXT NOT NULL,          -- e.g. "CSV"
    seen_at     TEXT NOT NULL           -- ISO-8601 UTC timestamp
);

CREATE TABLE IF NOT EXISTS tx_history (
    tx_id       TEXT PRIMARY KEY,
    date        TEXT NOT NULL,
    clean_name  TEXT NOT NULL,
    amount      REAL NOT NULL,
    category    TEXT NOT NULL DEFAULT 'Uncategorized'
);

CREATE TABLE IF NOT EXISTS user_overrides (
    original_description TEXT PRIMARY KEY,
    category             TEXT NOT NULL,
    clean_name           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS merchant_categories (
    clean_name  TEXT PRIMARY KEY,
    category    TEXT NOT NULL
);
"""


class BookmarkDB:
    """Thin wrapper around a SQLite database for dedup tracking."""

    def __init__(self, db_path: Path | str) -> None:
        self._path = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._path))
        self._conn.executescript(_SCHEMA)
        self._conn.commit()
        self._migrate()
        logger.info("Bookmark DB ready at %s", self._path)

    def _migrate(self) -> None:
        """Apply backwards-compatible schema migrations."""
        # Add category column to existing tx_history tables
        try:
            self._conn.execute("ALTER TABLE tx_history ADD COLUMN category TEXT NOT NULL DEFAULT 'Uncategorized'")
            self._conn.commit()
        except sqlite3.OperationalError:
            pass  # Column already exists

    # ── Transaction dedup ────────────────────────────────────────

    def is_seen(self, tx_id: str) -> bool:
        """Return True if this transaction ID was already processed."""
        row = self._conn.execute(
            "SELECT 1 FROM seen_transactions WHERE tx_id = ?", (tx_id,)
        ).fetchone()
        return row is not None

    def mark_seen(self, tx_id: str, source: str = "CSV") -> None:
        """Record that *tx_id* has been processed."""
        from datetime import datetime, timezone

        self._conn.execute(
            """
            INSERT OR IGNORE INTO seen_transactions (tx_id, source, seen_at)
            VALUES (?, ?, ?)
            """,
            (tx_id, source, datetime.now(timezone.utc).isoformat()),
        )
        self._conn.commit()

    def mark_seen_batch(self, tx_ids: list[str], source: str = "CSV") -> None:
        """Record a batch of transaction IDs as processed."""
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).isoformat()
        self._conn.executemany(
            """
            INSERT OR IGNORE INTO seen_transactions (tx_id, source, seen_at)
            VALUES (?, ?, ?)
            """,
            [(tx_id, source, now) for tx_id in tx_ids],
        )
        self._conn.commit()

    # ── History tracking for Recurring Detection ─────────────────
    
    def save_history(self, transactions: list[Any]) -> None:
        """Save a batch of parsed and ML-categorised transactions to history."""
        self._conn.executemany(
            """
            INSERT OR REPLACE INTO tx_history (tx_id, date, clean_name, amount, category)
            VALUES (?, ?, ?, ?, ?)
            """,
            [(t.id, t.date, t.clean_name, t.amount, getattr(t, 'category', 'Uncategorized')) for t in transactions],
        )
        # Also mark them as seen
        self.mark_seen_batch([t.id for t in transactions])
        
    def get_merchant_history(self) -> dict[str, list[tuple[str, float]]]:
        """Fetch all historical transactions grouped by merchant clean_name."""
        rows = self._conn.execute(
            """
            SELECT clean_name, date, amount
            FROM tx_history
            ORDER BY clean_name, date ASC
            """
        ).fetchall()
        
        history: dict[str, list[tuple[str, float]]] = {}
        for clean_name, date, amount in rows:
            history.setdefault(clean_name, []).append((date, amount))
            
        return history

    # ── Merchant Categories (for local mode) ──────────────────────

    def save_merchant_category(self, clean_name: str, category: str) -> None:
        """Save a merchant→category mapping for future local categorisation."""
        self._conn.execute(
            "INSERT OR REPLACE INTO merchant_categories (clean_name, category) VALUES (?, ?)",
            (clean_name, category),
        )
        self._conn.commit()

    def save_merchant_categories_batch(self, mappings: dict[str, str]) -> None:
        """Save multiple merchant→category mappings at once."""
        if not mappings:
            return
        self._conn.executemany(
            "INSERT OR REPLACE INTO merchant_categories (clean_name, category) VALUES (?, ?)",
            list(mappings.items()),
        )
        self._conn.commit()

    def get_merchant_categories(self) -> dict[str, str]:
        """Fetch all known merchant→category mappings."""
        rows = self._conn.execute("SELECT clean_name, category FROM merchant_categories").fetchall()
        return {name: cat for name, cat in rows}

    def get_training_data(self) -> list[tuple[str, str]]:
        """Return (raw_description, category) pairs for ML training.

        Joins tx_history with merchant_categories to associate descriptions with categories.
        """
        rows = self._conn.execute(
            """
            SELECT h.clean_name, m.category
            FROM tx_history h
            INNER JOIN merchant_categories m ON h.clean_name = m.clean_name
            """
        ).fetchall()
        return [(desc, cat) for desc, cat in rows]

    # ── LLM Feedback Overrides ───────────────────────────────────

    def save_overrides(self, overrides: dict[str, dict[str, str]]) -> None:
        """Save a dictionary of user-defined overrides (original_description -> {category, clean_name})."""
        if not overrides:
            return
            
        rows_to_insert = [
            (orig_desc, data.get("category", ""), data.get("clean_name", ""))
            for orig_desc, data in overrides.items()
        ]
        
        self._conn.executemany(
            """
            INSERT OR REPLACE INTO user_overrides (original_description, category, clean_name)
            VALUES (?, ?, ?)
            """,
            rows_to_insert,
        )
        self._conn.commit()

    def get_overrides(self) -> dict[str, dict[str, str]]:
        """Fetch all manual overrides applied by the user in Google Sheets."""
        rows = self._conn.execute(
            """
            SELECT original_description, category, clean_name
            FROM user_overrides
            """
        ).fetchall()
        
        return {
            orig_desc: {"category": cat, "clean_name": name}
            for orig_desc, cat, name in rows
        }

    def count(self) -> int:
        """Return total number of seen transactions."""
        row = self._conn.execute("SELECT COUNT(*) FROM seen_transactions").fetchone()
        return row[0] if row else 0

    # ── Housekeeping ─────────────────────────────────────────────

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "BookmarkDB":
        return self

    def __exit__(self, *exc) -> None:  # noqa: ANN002
        self.close()
