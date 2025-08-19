from __future__ import annotations

from pathlib import Path
from typing import Optional
import sqlite3
import pandas as pd

# Путь к БД по умолчанию
DB_PATH = "expenses.db"


def ensure_schema(db_path: str = DB_PATH) -> None:
    """
    Создаёт необходимые таблицы/индексы, если их ещё нет.
    """
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Таблица расходов
    cur.execute("""
        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,        -- YYYY-MM-DD
            category TEXT NOT NULL,
            amount REAL NOT NULL,
            description TEXT
        )
    """)

    # Индексы для скорости выборок
    cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_date ON expenses(date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_cat  ON expenses(category)")

    # Таблица месячных лимитов
    cur.execute("""
        CREATE TABLE IF NOT EXISTS monthly_limits (
            month TEXT NOT NULL,       -- YYYY-MM
            category TEXT NOT NULL,
            "limit" REAL NOT NULL,
            PRIMARY KEY (month, category)
        )
    """)

    conn.commit()
    conn.close()


def add_expense(
    date: str,
    category: str,
    amount: float,
    description: str = "",
    db_path: str = DB_PATH,
) -> None:
    """
    Добавляет расход ТОЛЬКО в SQLite (никакого JSON).
    """
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO expenses(date, category, amount, description) VALUES (?,?,?,?)",
            (date, category, float(amount), description or None),
        )


def get_expenses_df(
    db_path: str = DB_PATH,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category: Optional[str] = None,
) -> pd.DataFrame:
    """
    Универсальная выборка для экранов/графиков/экспорта.
    """
    q = (
        "SELECT date, category, amount, COALESCE(description,'') AS description "
        "FROM expenses WHERE 1=1"
    )
    params: list = []
    if start_date:
        q += " AND date >= ?"
        params.append(start_date)
    if end_date:
        q += " AND date <= ?"
        params.append(end_date)
    if category:
        q += " AND category = ?"
        params.append(category)
    q += " ORDER BY date ASC"

    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query(q, conn, params=params)
