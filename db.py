# db.py
import json
import os
import sqlite3
from typing import Dict, List


DATABASE_FILE = "expenses.db"
EXPENSES_JSON = "expenses.json"
LIMITS_JSON = "budget_limits.json"


# ---------- Core helpers ----------


def get_conn(db_path: str = DATABASE_FILE) -> sqlite3.Connection:
    """Return SQLite connection with reasonable defaults."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def initialize_db(db_path: str = DATABASE_FILE) -> None:
    """Create tables if not exist."""
    with get_conn(db_path) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS expenses (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                date     TEXT    NOT NULL,           -- YYYY-MM-DD
                category TEXT    NOT NULL,
                amount   REAL    NOT NULL,
                note     TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS budget_limits (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                month    TEXT    NOT NULL,           -- YYYY-MM
                category TEXT    NOT NULL,
                limit    REAL    NOT NULL,
                UNIQUE (month, category)
            )
            """
        )

        # Useful indices
        cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_date ON expenses(date)")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_expenses_cat  ON expenses(category)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_limits_month_cat ON budget_limits(month, category)"
        )


# ---------- Expenses I/O ----------


def add_expense_to_db(date: str, category: str, amount: float, note: str = "") -> None:
    """Insert a single expense row."""
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO expenses (date, category, amount, note) VALUES (?, ?, ?, ?)",
            (date, category, float(amount), note or ""),
        )


def get_all_expenses() -> List[Dict]:
    """Return all expenses as list of dicts."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT date, category, amount, note FROM expenses ORDER BY date ASC, category ASC"
        ).fetchall()

    return [
        {
            "date": row["date"],
            "category": row["category"],
            "amount": float(row["amount"]),
            "note": (row["note"] or ""),
        }
        for row in rows
    ]


# ---------- Limits I/O ----------


def upsert_monthly_limit(month: str, category: str, limit_value: float) -> None:
    """Insert or update a monthly limit for (month, category)."""
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO budget_limits (month, category, limit)
            VALUES (?, ?, ?)
            ON CONFLICT(month, category) DO UPDATE SET limit=excluded.limit
            """,
            (month, category, float(limit_value)),
        )


def get_monthly_limits() -> Dict[str, Dict[str, float]]:
    """
    Return limits in the form:
      { "YYYY-MM": { "food": 200.0, "transport": 50.0, ... }, ... }
    """
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT month, category, limit FROM budget_limits ORDER BY month, category"
        ).fetchall()

    result: Dict[str, Dict[str, float]] = {}
    for row in rows:
        month = row["month"]
        cat = row["category"]
        val = float(row["limit"])
        if month not in result:
            result[month] = {}
        result[month][cat] = val
    return result


# ---------- One-off migration from JSON ----------


def migrate_json_to_sqlite(
    expenses_json: str = EXPENSES_JSON,
    limits_json: str = LIMITS_JSON,
    db_path: str = DATABASE_FILE,
) -> None:
    """
    One-time migration helper:
      - loads expenses.json and budget_limits.json (if present)
      - inserts data into SQLite (skipping if already present)
    Safe to run multiple times.
    """
    initialize_db(db_path)

    # Expenses
    if os.path.exists(expenses_json):
        try:
            with open(expenses_json, "r", encoding="utf-8") as f:
                data = json.load(f)
            # Accept both list-of-dicts and empty
            if isinstance(data, list):
                with get_conn(db_path) as conn:
                    cur = conn.cursor()
                    for e in data:
                        cur.execute(
                            """
                            INSERT INTO expenses (date, category, amount, note)
                            VALUES (?, ?, ?, ?)
                            """,
                            (
                                e.get("date", ""),
                                e.get("category", ""),
                                float(e.get("amount", 0.0)),
                                e.get("note", "") or e.get("description", "") or "",
                            ),
                        )
        except Exception:
            # don't fail the app if old JSON is malformed
            pass

    # Limits
    if os.path.exists(limits_json):
        try:
            with open(limits_json, "r", encoding="utf-8") as f:
                limits = json.load(f) or {}
            if isinstance(limits, dict):
                for month, cats in limits.items():
                    if isinstance(cats, dict):
                        for cat, val in cats.items():
                            upsert_monthly_limit(month, cat, float(val))
        except Exception:
            pass
