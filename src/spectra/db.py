"""SQLite bookmark — tracks which transactions have already been imported."""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
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
    category    TEXT NOT NULL DEFAULT 'Uncategorized',
    original_description TEXT NOT NULL DEFAULT ''
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

CREATE TABLE IF NOT EXISTS category_rules (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_type   TEXT NOT NULL,     -- contains | regex
    pattern     TEXT NOT NULL,
    category    TEXT NOT NULL,
    priority    INTEGER NOT NULL DEFAULT 100,
    is_active   INTEGER NOT NULL DEFAULT 1,
    created_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS learning_feedback (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    tx_id               TEXT,
    original_description TEXT NOT NULL DEFAULT '',
    clean_name          TEXT NOT NULL,
    category            TEXT NOT NULL,
    source              TEXT NOT NULL,
    apply_to_future     INTEGER NOT NULL DEFAULT 1,
    created_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS error_events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at    TEXT NOT NULL,
    request_id    TEXT NOT NULL DEFAULT '',
    error_code    TEXT NOT NULL,
    severity      TEXT NOT NULL,
    message       TEXT NOT NULL,
    status_code   INTEGER NOT NULL,
    route         TEXT NOT NULL DEFAULT '',
    method        TEXT NOT NULL DEFAULT '',
    retryable     INTEGER NOT NULL DEFAULT 0,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_error_events_created_at
ON error_events(created_at);

CREATE INDEX IF NOT EXISTS idx_error_events_code_created_at
ON error_events(error_code, created_at);
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
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS category_rules (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_type  TEXT NOT NULL,
                pattern    TEXT NOT NULL,
                category   TEXT NOT NULL,
                priority   INTEGER NOT NULL DEFAULT 100,
                is_active  INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS learning_feedback (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                tx_id                TEXT,
                original_description TEXT NOT NULL DEFAULT '',
                clean_name           TEXT NOT NULL,
                category             TEXT NOT NULL,
                source               TEXT NOT NULL,
                apply_to_future      INTEGER NOT NULL DEFAULT 1,
                created_at           TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS error_events (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at    TEXT NOT NULL,
                request_id    TEXT NOT NULL DEFAULT '',
                error_code    TEXT NOT NULL,
                severity      TEXT NOT NULL,
                message       TEXT NOT NULL,
                status_code   INTEGER NOT NULL,
                route         TEXT NOT NULL DEFAULT '',
                method        TEXT NOT NULL DEFAULT '',
                retryable     INTEGER NOT NULL DEFAULT 0,
                metadata_json TEXT NOT NULL DEFAULT '{}'
            )
        """)
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_error_events_created_at
            ON error_events(created_at)
        """)
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_error_events_code_created_at
            ON error_events(error_code, created_at)
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

    # ── Category Rules ─────────────────────────────────────────

    def get_category_rules(self) -> list[dict[str, object]]:
        """Return all category rules sorted by priority then id."""
        rows = self._conn.execute(
            """
            SELECT id, rule_type, pattern, category, priority, is_active
            FROM category_rules
            ORDER BY priority ASC, id ASC
            """
        ).fetchall()
        return [
            {
                "id": int(rule_id),
                "rule_type": str(rule_type),
                "pattern": str(pattern),
                "category": str(category),
                "priority": int(priority),
                "is_active": bool(is_active),
            }
            for rule_id, rule_type, pattern, category, priority, is_active in rows
        ]

    def get_category_rule(self, rule_id: int) -> dict[str, object] | None:
        """Return a single category rule by id."""
        row = self._conn.execute(
            """
            SELECT id, rule_type, pattern, category, priority, is_active
            FROM category_rules
            WHERE id = ?
            """,
            (int(rule_id),),
        ).fetchone()
        if not row:
            return None

        return {
            "id": int(row[0]),
            "rule_type": str(row[1]),
            "pattern": str(row[2]),
            "category": str(row[3]),
            "priority": int(row[4]),
            "is_active": bool(row[5]),
        }

    def add_category_rule(self, rule_type: str, pattern: str, category: str) -> dict[str, object]:
        """Insert a new category rule and return it."""
        next_priority_row = self._conn.execute(
            "SELECT COALESCE(MAX(priority), 0) + 1 FROM category_rules"
        ).fetchone()
        priority = int(next_priority_row[0] if next_priority_row else 1)

        cur = self._conn.execute(
            """
            INSERT INTO category_rules (rule_type, pattern, category, priority, is_active)
            VALUES (?, ?, ?, ?, 1)
            """,
            (rule_type, pattern, category, priority),
        )
        self._conn.commit()
        return {
            "id": int(cur.lastrowid),
            "rule_type": rule_type,
            "pattern": pattern,
            "category": category,
            "priority": priority,
            "is_active": True,
        }

    def _normalize_rule_priorities(self) -> None:
        """Keep priorities contiguous after mutations."""
        rows = self._conn.execute(
            "SELECT id FROM category_rules ORDER BY priority ASC, id ASC"
        ).fetchall()
        for index, (rule_id,) in enumerate(rows, start=1):
            self._conn.execute(
                "UPDATE category_rules SET priority = ? WHERE id = ?",
                (index, int(rule_id)),
            )
        self._conn.commit()

    def move_category_rule(self, rule_id: int, direction: str) -> list[dict[str, object]]:
        """Move a category rule one step up or down in priority order."""
        normalized_direction = str(direction or "").strip().lower()
        if normalized_direction not in {"up", "down"}:
            raise ValueError("direction must be 'up' or 'down'")

        rules = self.get_category_rules()
        idx = next((i for i, rule in enumerate(rules) if int(rule["id"]) == int(rule_id)), None)
        if idx is None:
            raise KeyError(rule_id)

        target_idx = idx - 1 if normalized_direction == "up" else idx + 1
        if target_idx < 0 or target_idx >= len(rules):
            return rules

        rules[idx], rules[target_idx] = rules[target_idx], rules[idx]
        for priority, rule in enumerate(rules, start=1):
            self._conn.execute(
                "UPDATE category_rules SET priority = ? WHERE id = ?",
                (priority, int(rule["id"])),
            )
        self._conn.commit()
        return self.get_category_rules()

    def update_category_rule(
        self,
        rule_id: int,
        *,
        is_active: bool | None = None,
    ) -> dict[str, object] | None:
        """Update mutable category rule fields and return the fresh row."""
        updates: list[str] = []
        params: list[object] = []

        if is_active is not None:
            updates.append("is_active = ?")
            params.append(1 if is_active else 0)

        if not updates:
            return self.get_category_rule(rule_id)

        params.append(int(rule_id))
        cur = self._conn.execute(
            f"UPDATE category_rules SET {', '.join(updates)} WHERE id = ?",
            params,
        )
        self._conn.commit()
        if cur.rowcount <= 0:
            return None
        self._normalize_rule_priorities()
        return self.get_category_rule(rule_id)

    def delete_category_rule(self, rule_id: int) -> bool:
        """Delete a category rule by id. Returns True if deleted."""
        cur = self._conn.execute("DELETE FROM category_rules WHERE id = ?", (int(rule_id),))
        self._conn.commit()
        if cur.rowcount > 0:
            self._normalize_rule_priorities()
        return cur.rowcount > 0

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

    def record_learning_feedback(
        self,
        *,
        tx_id: str | None,
        original_description: str,
        clean_name: str,
        category: str,
        source: str,
        apply_to_future: bool,
    ) -> int:
        """Persist a user learning event for auditability and later review."""
        cur = self._conn.execute(
            """
            INSERT INTO learning_feedback (
                tx_id,
                original_description,
                clean_name,
                category,
                source,
                apply_to_future
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                tx_id,
                original_description,
                clean_name,
                category,
                source,
                1 if apply_to_future else 0,
            ),
        )
        self._conn.commit()
        return int(cur.lastrowid)

    def get_recent_learning_feedback(self, limit: int = 40) -> list[dict[str, object]]:
        """Return the most recent user learning events."""
        rows = self._conn.execute(
            """
            SELECT id, tx_id, original_description, clean_name, category, source, apply_to_future, created_at
            FROM learning_feedback
            ORDER BY id DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
        return [
            {
                "id": int(row_id),
                "tx_id": tx_id,
                "original_description": str(original_description),
                "clean_name": str(clean_name),
                "category": str(category),
                "source": str(source),
                "apply_to_future": bool(apply_to_future),
                "created_at": str(created_at),
            }
            for row_id, tx_id, original_description, clean_name, category, source, apply_to_future, created_at in rows
        ]

    def reapply_learning_to_history(self) -> dict[str, int]:
        """Re-apply overrides, rules, and merchant mappings to historical rows."""
        from spectra.rules import first_matching_rule

        overrides = self.get_overrides()
        rules = self.get_category_rules()
        merchant_categories = self.get_merchant_categories()
        rows = self._conn.execute(
            """
            SELECT tx_id, original_description, clean_name, category
            FROM tx_history
            ORDER BY date ASC, tx_id ASC
            """
        ).fetchall()

        updated = 0
        override_updates = 0
        rule_updates = 0
        merchant_updates = 0

        for tx_id, original_description, clean_name, current_category in rows:
            next_name = str(clean_name)
            next_category = str(current_category)
            applied_source = ""

            override = overrides.get(str(original_description or ""))
            if override:
                next_name = str(override.get("clean_name") or next_name)
                next_category = str(override.get("category") or next_category)
                applied_source = "override"
            else:
                matched_rule = first_matching_rule(
                    rules,
                    clean_name=str(clean_name),
                    raw_description=str(original_description or clean_name),
                )
                if matched_rule:
                    next_category = str(matched_rule["category"])
                    applied_source = "rule"
                elif str(clean_name) in merchant_categories:
                    next_category = str(merchant_categories[str(clean_name)])
                    applied_source = "merchant"

            if next_name == str(clean_name) and next_category == str(current_category):
                continue

            self._conn.execute(
                "UPDATE tx_history SET clean_name = ?, category = ? WHERE tx_id = ?",
                (next_name, next_category, str(tx_id)),
            )
            updated += 1
            if applied_source == "override":
                override_updates += 1
            elif applied_source == "rule":
                rule_updates += 1
            elif applied_source == "merchant":
                merchant_updates += 1

        self._conn.commit()
        return {
            "updated": updated,
            "override_updates": override_updates,
            "rule_updates": rule_updates,
            "merchant_updates": merchant_updates,
        }

    # ── Error Watcher ─────────────────────────────────────────

    def record_error_event(
        self,
        *,
        request_id: str,
        error_code: str,
        severity: str,
        message: str,
        status_code: int,
        route: str,
        method: str,
        retryable: bool,
        metadata: dict[str, object] | None = None,
    ) -> int:
        """Persist an application error event for later analytics/alerting."""
        created_at = (
            datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )
        metadata_json = json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True)
        cur = self._conn.execute(
            """
            INSERT INTO error_events (
                created_at,
                request_id,
                error_code,
                severity,
                message,
                status_code,
                route,
                method,
                retryable,
                metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                created_at,
                request_id or "",
                error_code,
                severity,
                message,
                int(status_code),
                route or "",
                method or "",
                1 if retryable else 0,
                metadata_json,
            ),
        )
        self._conn.commit()
        return int(cur.lastrowid)

    def count_error_events(
        self,
        *,
        window_minutes: int,
        error_code: str | None = None,
        severity: str | None = None,
    ) -> int:
        """Count error events in the trailing window (in minutes)."""
        cutoff = (
            datetime.now(timezone.utc) - timedelta(minutes=max(1, int(window_minutes)))
        ).replace(microsecond=0).isoformat().replace("+00:00", "Z")

        query = "SELECT COUNT(*) FROM error_events WHERE created_at >= ?"
        params: list[object] = [cutoff]

        if error_code:
            query += " AND error_code = ?"
            params.append(str(error_code))
        if severity:
            query += " AND severity = ?"
            params.append(str(severity))

        row = self._conn.execute(query, params).fetchone()
        return int(row[0] if row else 0)

    def prune_error_events(self, *, retention_days: int) -> int:
        """Delete error events older than *retention_days* days."""
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=max(1, int(retention_days)))
        ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        cur = self._conn.execute(
            "DELETE FROM error_events WHERE created_at < ?",
            (cutoff,),
        )
        self._conn.commit()
        return max(0, int(cur.rowcount or 0))

    def get_recent_error_events(self, limit: int = 25) -> list[dict[str, object]]:
        """Return latest error events in descending order."""
        rows = self._conn.execute(
            """
            SELECT id, created_at, request_id, error_code, severity, message,
                   status_code, route, method, retryable, metadata_json
            FROM error_events
            ORDER BY id DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()

        events: list[dict[str, object]] = []
        for (
            row_id,
            created_at,
            request_id,
            error_code,
            severity,
            message,
            status_code,
            route,
            method,
            retryable,
            metadata_json,
        ) in rows:
            try:
                metadata = json.loads(str(metadata_json or "{}"))
                if not isinstance(metadata, dict):
                    metadata = {}
            except json.JSONDecodeError:
                metadata = {}

            events.append(
                {
                    "id": int(row_id),
                    "created_at": str(created_at),
                    "request_id": str(request_id or ""),
                    "error_code": str(error_code),
                    "severity": str(severity),
                    "message": str(message),
                    "status_code": int(status_code),
                    "route": str(route or ""),
                    "method": str(method or ""),
                    "retryable": bool(retryable),
                    "metadata": metadata,
                }
            )
        return events

    def get_error_overview(
        self,
        *,
        window_hours: int = 24 * 7,
        recent_limit: int = 25,
    ) -> dict[str, object]:
        """Aggregate error telemetry for watcher dashboards."""
        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=max(1, int(window_hours)))
        ).replace(microsecond=0).isoformat().replace("+00:00", "Z")

        total_row = self._conn.execute(
            "SELECT COUNT(*) FROM error_events WHERE created_at >= ?",
            (cutoff,),
        ).fetchone()
        total_events = int(total_row[0] if total_row else 0)

        unique_req_row = self._conn.execute(
            """
            SELECT COUNT(DISTINCT request_id)
            FROM error_events
            WHERE created_at >= ? AND request_id != ''
            """,
            (cutoff,),
        ).fetchone()
        unique_requests = int(unique_req_row[0] if unique_req_row else 0)

        severity_rows = self._conn.execute(
            """
            SELECT severity, COUNT(*)
            FROM error_events
            WHERE created_at >= ?
            GROUP BY severity
            ORDER BY COUNT(*) DESC
            """,
            (cutoff,),
        ).fetchall()
        severities = {str(name): int(count) for name, count in severity_rows}

        top_codes_rows = self._conn.execute(
            """
            SELECT error_code, COUNT(*) as cnt, MAX(created_at) as last_seen
            FROM error_events
            WHERE created_at >= ?
            GROUP BY error_code
            ORDER BY cnt DESC, last_seen DESC
            LIMIT 10
            """,
            (cutoff,),
        ).fetchall()
        top_codes = [
            {
                "error_code": str(error_code),
                "count": int(count),
                "last_seen": str(last_seen),
            }
            for error_code, count, last_seen in top_codes_rows
        ]

        top_routes_rows = self._conn.execute(
            """
            SELECT route, method, COUNT(*) as cnt
            FROM error_events
            WHERE created_at >= ?
            GROUP BY route, method
            ORDER BY cnt DESC
            LIMIT 10
            """,
            (cutoff,),
        ).fetchall()
        top_routes = [
            {
                "route": str(route or ""),
                "method": str(method or ""),
                "count": int(count),
            }
            for route, method, count in top_routes_rows
        ]

        timeline_rows = self._conn.execute(
            """
            SELECT substr(created_at, 1, 10) as day, COUNT(*) as cnt
            FROM error_events
            WHERE created_at >= ?
            GROUP BY day
            ORDER BY day ASC
            """,
            (cutoff,),
        ).fetchall()
        timeline = [
            {"day": str(day), "count": int(count)}
            for day, count in timeline_rows
        ]

        server_error_row = self._conn.execute(
            """
            SELECT COUNT(*)
            FROM error_events
            WHERE created_at >= ? AND status_code >= 500
            """,
            (cutoff,),
        ).fetchone()
        server_errors = int(server_error_row[0] if server_error_row else 0)

        return {
            "window_hours": int(window_hours),
            "total_events": total_events,
            "unique_requests": unique_requests,
            "server_errors": server_errors,
            "severities": severities,
            "top_codes": top_codes,
            "top_routes": top_routes,
            "timeline": timeline,
            "recent_events": self.get_recent_error_events(limit=recent_limit),
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
            "learning_feedback",
            "error_events",
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
