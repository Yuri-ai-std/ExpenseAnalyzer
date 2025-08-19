# db.py — SQLite backend for ExpenseAnalyzer

from __future__ import annotations
import json
import os
import sqlite3
import pandas as pd
from typing import Dict, List, Tuple
from typing import Optional, Sequence

DB_PATH = "expenses.db"
EXPENSES_JSON = "expenses.json"
BUDGET_LIMITS_JSON = "budget_limits.json"


# ——— Low-level helpers ———
def get_conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Return a SQLite connection with sensible defaults."""
    conn = sqlite3.connect(db_path)
    # Make returned rows behave like tuples (simple & fast)
    return conn


def initialize_db(db_path: str = DB_PATH) -> None:
    """Create required tables if they don't exist."""
    conn = get_conn(db_path)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS expenses (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            date     TEXT    NOT NULL,     -- YYYY-MM-DD
            category TEXT    NOT NULL,
            amount   REAL    NOT NULL,
            note     TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS budget_limits (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            month        TEXT    NOT NULL,    -- YYYY-MM
            category     TEXT    NOT NULL,
            limit_amount REAL    NOT NULL,
            UNIQUE(month, category)
        )
        """
    )

    conn.commit()
    conn.close()


# ——— Import/migration from legacy JSON files ———
def migrate_json_to_sqlite(
    expenses_json: str = EXPENSES_JSON,
    limits_json: str = BUDGET_LIMITS_JSON,
    db_path: str = DB_PATH,
) -> None:
    """
    One-time import from JSON files into SQLite (idempotent):
    - Appends expenses that are not already present by (date, category, amount, note)
    - Upserts monthly limits
    """
    initialize_db(db_path)
    conn = get_conn(db_path)
    cur = conn.cursor()

    # Expenses
    if os.path.exists(expenses_json):
        try:
            with open(expenses_json, "r", encoding="utf-8") as f:
                items = json.load(f)
        except Exception:
            items = []

        for e in items or []:
            cur.execute(
                """
                INSERT INTO expenses (date, category, amount, note)
                VALUES (?, ?, ?, ?)
                """,
                (
                    e.get("date"),
                    e.get("category"),
                    float(e.get("amount", 0.0)),
                    e.get("note") or e.get("description") or "",
                ),
            )

    # Limits: JSON shape expected { "YYYY-MM": { "food": 100.0, ... }, ... }
    if os.path.exists(limits_json):
        try:
            with open(limits_json, "r", encoding="utf-8") as f:
                limits = json.load(f) or {}
        except Exception:
            limits = {}

        for month, cats in limits.items():
            for category, limit_amount in (cats or {}).items():
                # Use UPSERT (SQLite 3.24+). If unavailable, REPLACE INTO also works.
                cur.execute(
                    """
                    INSERT INTO budget_limits (month, category, limit_amount)
                    VALUES (?, ?, ?)
                    ON CONFLICT(month, category)
                    DO UPDATE SET limit_amount=excluded.limit_amount
                    """,
                    (month, category, float(limit_amount)),
                )

    conn.commit()
    conn.close()


# ——— Public API used by project.py ———
def add_expense_to_db(
    date: str, category: str, amount: float, note: str = "", db_path: str = DB_PATH
) -> None:
    """Insert a single expense row."""
    initialize_db(db_path)
    conn = get_conn(db_path)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO expenses (date, category, amount, note) VALUES (?, ?, ?, ?)",
        (date, category, float(amount), note or ""),
    )
    conn.commit()
    conn.close()


def get_all_expenses(db_path: str = DB_PATH) -> List[Dict]:
    """
    Return all expenses as a list of dicts:
    [{date, category, amount, note}, ...]
    """
    initialize_db(db_path)
    conn = get_conn(db_path)
    cur = conn.cursor()
    cur.execute(
        "SELECT date, category, amount, note FROM expenses ORDER BY date ASC, id ASC"
    )
    rows = cur.fetchall()
    conn.close()

    return [
        {"date": d, "category": c, "amount": float(a), "note": (n or "")}
        for (d, c, a, n) in rows
    ]


def get_monthly_limits(db_path: str = DB_PATH) -> Dict[str, Dict[str, float]]:
    """
    Return limits as nested dict: { "YYYY-MM": { "food": 100.0, ... }, ... }
    """
    initialize_db(db_path)
    conn = get_conn(db_path)
    cur = conn.cursor()
    cur.execute("SELECT month, category, limit_amount FROM budget_limits")
    rows = cur.fetchall()
    conn.close()

    result: Dict[str, Dict[str, float]] = {}
    for month, category, limit_amount in rows:
        result.setdefault(month, {})[category] = float(limit_amount)
    return result


def save_monthly_limits(
    limits: Dict[str, Dict[str, float]], db_path: str = DB_PATH
) -> None:
    """
    Upsert all limits from nested dict {month: {category: limit, ...}, ...}
    """
    if not limits:
        return

    initialize_db(db_path)
    conn = get_conn(db_path)
    cur = conn.cursor()

    for month, cats in limits.items():
        for category, limit_amount in (cats or {}).items():
            cur.execute(
                """
                INSERT INTO budget_limits (month, category, limit_amount)
                VALUES (?, ?, ?)
                ON CONFLICT(month, category)
                DO UPDATE SET limit_amount=excluded.limit_amount
                """,
                (month, category, float(limit_amount)),
            )

    conn.commit()
    conn.close()


# ---------- DataFrame helpers (date, category, amount, description) ----------


def get_expenses_df(
    db_path: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category: Optional[str] = None,
) -> pd.DataFrame:
    """Возвращает DataFrame с колонками date/category/amount (+description, если она есть)."""
    path = db_path or DB_PATH

    with sqlite3.connect(path) as conn:
        # Узнаём реальные колонки таблицы
        table_info = pd.read_sql_query("PRAGMA table_info(expenses);", conn)
        existing_cols = set(table_info["name"].tolist())

        # Минимально нужные; description добавляем только если есть в схеме
        cols = ["date", "category", "amount"]
        if "description" in existing_cols:
            cols.append("description")

        # Собираем запрос с фильтрами
        q = f"SELECT {', '.join(cols)} FROM expenses WHERE 1=1"
        params = {}
        if start_date:
            q += " AND date >= :start"
            params["start"] = start_date
        if end_date:
            q += " AND date <= :end"
            params["end"] = end_date
        if category:
            q += " AND category = :category"
            params["category"] = category
        q += " ORDER BY date ASC"

        df = pd.read_sql_query(q, conn, params=params)

    return df


def totals_by_month(df: pd.DataFrame) -> pd.DataFrame:
    """Суммы по месяцам (YYYY-MM)."""
    if df.empty:
        return pd.DataFrame(columns=["month", "amount"])

    out = (
        df.assign(month=df["date"].dt.to_period("M").astype(str))
        .groupby("month", as_index=False)[["amount"]]
        .sum()
        .sort_values(by=["month"])
        .reset_index(drop=True)
    )
    return out


def totals_by_category(df: pd.DataFrame) -> pd.DataFrame:
    """Суммы по категориям за выбранный диапазон."""
    if df.empty:
        return pd.DataFrame(columns=["category", "amount"])

    out = (
        df.groupby("category", as_index=False)[["amount"]]
        .sum()
        .sort_values(by=["amount"], ascending=False)
        .reset_index(drop=True)
    )
    return out
