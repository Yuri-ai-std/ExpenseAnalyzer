from __future__ import annotations

from pathlib import Path
from typing import Optional, List, Dict, Any
import sqlite3
import pandas as pd

# Путь к БД по умолчанию
DB_PATH = "data/expenses.db"


def ensure_schema(db_path: str = DB_PATH) -> None:
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS expenses(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                category TEXT NOT NULL,
                amount REAL NOT NULL,
                description TEXT
            )
        """
        )
        # индекс(ы) оставьте как есть, если они уже есть в файле
        conn.commit()


def get_expenses_df(
    db_path: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category: Optional[str] = None,
) -> pd.DataFrame:
    # ленивый дефолт
    if db_path is None:
        db_path = DB_PATH

    ensure_schema(db_path)

    where_parts: list[str] = ["WHERE 1=1"]
    params: list[str] = []

    if start_date:
        where_parts.append("AND date >= ?")
        params.append(start_date)
    if end_date:
        where_parts.append("AND date <= ?")
        params.append(end_date)
    if category:
        where_parts.append("AND category = ?")
        params.append(category)

    query = f"""
        SELECT date, category, amount, COALESCE(description, '') AS description
        FROM expenses
        {' '.join(where_parts)}
        ORDER BY date
    """

    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query(
            query,
            conn,
            params=tuple(params),  # 👈 кортеж вместо list
        )


def load_expenses(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category: Optional[str] = None,
) -> List[Dict]:
    """
    Универсальная выборка в виде списка словарей для меню/печати.
    """
    # если у вас уже есть фильтры внутри get_expenses_df — просто прокиньте
    df = get_expenses_df(start_date=start_date, end_date=end_date, category=category)
    if df is None or df.empty:
        return []
    # гарантируем одинаковые ключи
    out = []
    for r in df.to_dict(orient="records"):
        out.append(
            {
                "date": str(r.get("date", "")),
                "category": str(r.get("category", "")),
                "amount": float(r.get("amount") or 0.0),
                "description": r.get("description") or r.get("note") or None,
            }
        )
    return out


def add_expense(
    date: str,
    category: str,
    amount: float,
    description: Optional[str] = None,
    db_path: Optional[str] = None,
) -> None:
    if db_path is None:
        db_path = DB_PATH

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO expenses(date, category, amount, description) VALUES (?, ?, ?, ?)",
            (date, category, float(amount), description),
        )
        conn.commit()


# --- Совместимость со старым API (тонкие обёртки) ---


def get_conn(db_path: str = DB_PATH):
    """
    Старое имя: вернуть соединение. Перед отдачей гарантируем схему.
    """
    ensure_schema(db_path)
    return sqlite3.connect(db_path)


def get_all_expenses(
    start_date: str | None = None,
    end_date: str | None = None,
    category: str | None = None,
    db_path: str = DB_PATH,
):
    """
    Старое имя: получить все расходы (как DataFrame) с фильтрами.
    """
    return get_expenses_df(
        db_path=db_path, start_date=start_date, end_date=end_date, category=category
    )


def list_categories() -> list[str]:
    """
    Вернёт отсортированный список уникальных категорий из БД.
    """
    import sqlite3

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT category FROM expenses ORDER BY category ASC")
        return [r[0] for r in cur.fetchall() if r and r[0]]


def migrate_json_to_sqlite(
    json_path: str = "expenses.json",
    db_path: str = DB_PATH,
) -> int:
    """
    Перенос из JSON в SQLite. Дубликаты игнорируются.
    Возвращает число реально добавленных строк.
    Формат JSON: список словарей с ключами date, category, amount, description (опц.).
    """
    import json

    ensure_schema(db_path)

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return 0

    inserted = 0
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        # На всякий случай – уникальность
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_expenses_uniq "
            "ON expenses(date, category, amount, COALESCE(description, ''))"
        )
        for row in data:
            date = str(row.get("date", "")).strip()
            category = str(row.get("category", "")).strip()
            # аккуратно приводим сумму
            try:
                amount = float(row.get("amount", 0) or 0)
            except Exception:
                amount = 0.0
            desc = row.get("description") or row.get("note") or ""

            if not date or not category:
                continue

            try:
                cur.execute(
                    "INSERT OR IGNORE INTO expenses(date, category, amount, description) VALUES (?,?,?,?)",
                    (date, category, amount, desc),
                )
                inserted += cur.rowcount  # будет 1 если реально вставили, 0 если дубль
            except Exception:
                # не падаем на мусорных строках
                pass

        conn.commit()

    return inserted
