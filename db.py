from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from utils import DATA_DIR


# 🔹 Хелпер для получения пути к БД пользователя
def get_db_path(user: str = "default") -> str:
    """Возвращает путь к БД конкретного пользователя"""
    user_dir = DATA_DIR / user
    user_dir.mkdir(exist_ok=True)
    return str(user_dir / "expenses.db")


# 🔹 Путь по умолчанию (для старта без логина)
DB_PATH = get_db_path("default")


# === Пример использования в функциях ===


def ensure_schema(db_path: Optional[str] = None):
    path = db_path or DB_PATH
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            category TEXT,
            amount REAL,
            description TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def get_expenses_df(
    db_path: Optional[str] = None,
    *,  # делаем параметры ниже только именованными
    # “правильные” имена:
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category: Optional[str] = None,
    # алиасы для обратной совместимости:
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> pd.DataFrame:
    """
    Возвращает DataFrame с расходами. Фильтры по дате и категории — опциональны.

    Поддерживаются оба набора имён:
    - start_date / end_date / category
    - start / end / category
    """
    # Сводим алиасы к основным именам
    if start_date is None:
        start_date = start
    if end_date is None:
        end_date = end

    # ---- дальше ваша текущая реализация ----
    if db_path is None:
        db_path = DB_PATH  # если у вас есть глобальный путь

    where_parts: list[str] = ["WHERE 1=1"]
    params: list[Any] = []

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
        return pd.read_sql_query(query, conn, params=params)


def add_expense(
    date: str,
    category: str,
    amount: float,
    description: str = "",
    db_path: Optional[str] = None,
):
    path = db_path or DB_PATH
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO expenses (date, category, amount, description) VALUES (?, ?, ?, ?)",
        (date, category, amount, description),
    )
    conn.commit()
    conn.close()


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
