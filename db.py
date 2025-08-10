import os
import json
import sqlite3
from typing import List, Dict, Any

DB_PATH = "expenses.db"
EXP_JSON = "expenses.json"
LIM_JSON = "budget_limits.json"


def get_conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,                 -- YYYY-MM-DD
            category TEXT NOT NULL,
            amount REAL NOT NULL CHECK(amount >= 0),
            note TEXT DEFAULT ''
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS limits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month TEXT NOT NULL,                -- YYYY-MM
            category TEXT NOT NULL,
            limit REAL NOT NULL CHECK(limit >= 0),
            UNIQUE(month, category)
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_expenses_date ON expenses(date);")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_expenses_category ON expenses(category);"
    )
    conn.commit()


def migrate_json_to_sqlite(
    expenses_json: str = EXP_JSON,
    limits_json: str = LIM_JSON,
    db_path: str = DB_PATH,
) -> bool:
    """
    Переносит данные из JSON в SQLite, если таблица expenses ещё пустая.
    Возвращает True, если миграция выполнялась; False — если в БД уже были данные.
    """
    conn = get_conn(db_path)
    init_db(conn)

    cur = conn.execute("SELECT COUNT(*) FROM expenses")
    count = cur.fetchone()[0]
    if count > 0:
        conn.close()
        return False  # уже мигрировано/данные есть

    # 1) расходы
    if os.path.exists(expenses_json):
        try:
            with open(expenses_json, "r", encoding="utf-8") as f:
                expenses = json.load(f)
        except json.JSONDecodeError:
            expenses = []
        for e in expenses or []:
            conn.execute(
                "INSERT INTO expenses(date, category, amount, note) VALUES (?, ?, ?, ?)",
                (
                    e.get("date"),
                    e.get("category"),
                    float(e.get("amount", 0) or 0),
                    e.get("note") or e.get("description", "") or "",
                ),
            )

    # 2) лимиты
    if os.path.exists(limits_json):
        try:
            with open(limits_json, "r", encoding="utf-8") as f:
                limits = json.load(f)
        except json.JSONDecodeError:
            limits = {}
        for month, cats in (limits or {}).items():
            for cat, lim in (cats or {}).items():
                conn.execute(
                    "INSERT OR IGNORE INTO limits(month, category, limit) VALUES (?, ?, ?)",
                    (month, cat, float(lim or 0)),
                )

    conn.commit()
    conn.close()
    return True


def add_expense_to_db(
    date: str, category: str, amount: float, note: str = "", db_path: str = DB_PATH
) -> None:
    conn = get_conn(db_path)
    init_db(conn)
    conn.execute(
        "INSERT INTO expenses(date, category, amount, note) VALUES (?, ?, ?, ?)",
        (date, category, float(amount), note or ""),
    )
    conn.commit()
    conn.close()


def get_all_expenses(db_path: str = DB_PATH) -> List[Dict[str, Any]]:
    """
    Возвращает список словарей: [{date, category, amount, note}, ...]
    """
    conn = get_conn(db_path)
    init_db(conn)
    cur = conn.execute(
        "SELECT date, category, amount, note FROM expenses ORDER BY date"
    )
    rows = cur.fetchall()
    conn.close()
    return [
        {"date": d, "category": c, "amount": float(a), "note": (n or "")}
        for (d, c, a, n) in rows
    ]


def get_monthly_limits(db_path: str = DB_PATH) -> Dict[str, Dict[str, float]]:
    """
    Возвращает лимиты в виде словаря: {"YYYY-MM": {"food": 100.0, ...}, ...}
    """
    conn = get_conn(db_path)
    init_db(conn)
    cur = conn.execute("SELECT month, category, limit FROM limits")
    result: Dict[str, Dict[str, float]] = {}
    for month, cat, lim in cur.fetchall():
        result.setdefault(month, {})[cat] = float(lim)
    conn.close()
    return result
