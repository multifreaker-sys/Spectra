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

CREATE TABLE IF NOT EXISTS budget_limits (
    category    TEXT PRIMARY KEY,
    monthly_limit REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS app_settings (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL
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
        # Add original_description column to tx_history (for ML training on raw text)
        try:
            self._conn.execute("ALTER TABLE tx_history ADD COLUMN original_description TEXT NOT NULL DEFAULT ''")
            self._conn.commit()
        except sqlite3.OperationalError:
            pass  # Column already exists
        # budget_limits table (for existing DBs pre-budget feature)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS budget_limits (
                category      TEXT PRIMARY KEY,
                monthly_limit REAL NOT NULL
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS app_settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        self._conn.commit()

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
            INSERT OR REPLACE INTO tx_history (tx_id, date, clean_name, amount, category, original_description)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    t.id, t.date, t.clean_name, t.amount,
                    getattr(t, 'category', 'Uncategorized'),
                    getattr(t, 'original_description', ''),
                )
                for t in transactions
            ],
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

    # ── Budget Limits ───────────────────────────────────────────

    def get_budget_limits(self) -> dict[str, float]:
        """Return all category→monthly_limit mappings."""
        rows = self._conn.execute("SELECT category, monthly_limit FROM budget_limits").fetchall()
        return {cat: lim for cat, lim in rows}

    def save_budget_limit(self, category: str, monthly_limit: float) -> None:
        """Save or update the monthly budget limit for a category."""
        self._conn.execute(
            "INSERT OR REPLACE INTO budget_limits (category, monthly_limit) VALUES (?, ?)",
            (category, monthly_limit),
        )
        self._conn.commit()

    def get_app_setting(self, key: str, default: str | None = None) -> str | None:
        """Return a persisted app setting by key."""
        row = self._conn.execute(
            "SELECT value FROM app_settings WHERE key = ?",
            (key,),
        ).fetchone()
        return row[0] if row else default

    def set_app_setting(self, key: str, value: str) -> None:
        """Persist a simple app setting."""
        self._conn.execute(
            "INSERT OR REPLACE INTO app_settings (key, value) VALUES (?, ?)",
            (key, value),
        )
        self._conn.commit()

    def get_training_data(self) -> list[tuple[str, str]]:
        """Return (description, category) pairs for ML training.

        Sources (in priority order):
        1. User overrides — the gold-standard corrections by the user.
        2. Transaction history — raw banking descriptions with their assigned category.
        3. Merchant memory fallback — clean_name→category for old rows without raw descriptions.
        """
        pairs: list[tuple[str, str]] = []
        seen: set[str] = set()

        # 1. User overrides (highest quality: the user explicitly corrected these)
        for row in self._conn.execute(
            "SELECT original_description, category FROM user_overrides WHERE category != ''"
        ).fetchall():
            desc, cat = row
            if desc and cat and desc not in seen:
                pairs.append((desc, cat))
                seen.add(desc)

        # 2. History rows that have a raw original_description
        for row in self._conn.execute(
            """
            SELECT original_description, category
            FROM tx_history
            WHERE original_description != '' AND category != 'Uncategorized'
            """
        ).fetchall():
            desc, cat = row
            if desc and desc not in seen:
                pairs.append((desc, cat))
                seen.add(desc)

        # 3. Fallback: old history rows without original_description — use clean_name
        for row in self._conn.execute(
            """
            SELECT h.clean_name, m.category
            FROM tx_history h
            INNER JOIN merchant_categories m ON h.clean_name = m.clean_name
            WHERE (h.original_description IS NULL OR h.original_description = '')
            """
        ).fetchall():
            desc, cat = row
            if desc and cat and desc not in seen:
                pairs.append((desc, cat))
                seen.add(desc)

        return pairs

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

    def reset_all_data(self) -> dict[str, int]:
        """Delete all user data from the local DB while keeping schema intact."""
        tables = [
            "tx_history",
            "seen_transactions",
            "merchant_categories",
            "user_overrides",
            "budget_limits",
        ]

        deleted_counts: dict[str, int] = {}
        for table in tables:
            row = self._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            deleted_counts[table] = int(row[0] if row else 0)

        try:
            self._conn.execute("BEGIN")
            for table in tables:
                self._conn.execute(f"DELETE FROM {table}")
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

        # Reclaim free pages after mass delete.
        self._conn.execute("VACUUM")
        self._conn.commit()
        return deleted_counts

    # ── Housekeeping ─────────────────────────────────────────────

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "BookmarkDB":
        return self

    def __exit__(self, *exc) -> None:  # noqa: ANN002
        self.close()
