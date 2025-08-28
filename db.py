from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Iterable, Union
from os import PathLike

import pandas as pd

from utils import DATA_DIR


# --- Универсальный резолвер пути БД: явный путь > session_state > дефолт ---
def _resolve_db_path(db_path: Optional[str] = None) -> str:
    """
    Возвращает путь к БД в следующем приоритете:
    1) явный аргумент db_path, если передан;
    2) st.session_state['ACTIVE_DB_PATH'] (если streamlit доступен);
    3) путь по умолчанию для пользователя 'default'.
    """
    if db_path:
        return db_path
    try:
        # Локальный импорт, чтобы модуль работал и вне Streamlit (в тестах/скриптах)
        import streamlit as st  # type: ignore

        p = st.session_state.get("ACTIVE_DB_PATH")
        if p:
            return str(p)
    except Exception:
        pass
    # Фолбэк на дефолтного пользователя (и одновременно гарантия существования data/)
    return get_db_path("default")


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
    """
    Создаёт таблицы в базе данных, если их ещё нет.
    """
    # Унифицированное определение пути
    path = _resolve_db_path(db_path)

    with sqlite3.connect(path) as conn:
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


def get_expenses_df(
    db_path: Optional[str] = None,
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category: Optional[str] = None,
) -> pd.DataFrame:
    """Читает таблицу expenses как DataFrame c фильтрами."""
    db_path = _resolve_db_path(db_path)

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

    sql = f"""
        SELECT date, category, amount, COALESCE(description, '') AS description
        FROM expenses
        {' '.join(where_parts)}
        ORDER BY date
    """
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(sql, conn, params=params)
    return df


def add_expense(
    *,
    date: str,
    category: str,
    amount: float,
    description: Optional[str] = None,
    db_path: Optional[str] = None,
) -> None:
    db_path = _resolve_db_path(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO expenses (date, category, amount, description) VALUES (?, ?, ?, ?)",
            (date, category, amount, description or None),
        )
        conn.commit()


def load_expenses(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category: Optional[str] = None,
    db_path: Optional[str] = None,
) -> List[Dict]:
    """
    Универсальная выборка расходов как список словарей (для меню/печати).
    Путь к БД: явный аргумент > session_state > дефолт.
    """
    path = _resolve_db_path(db_path)

    df = get_expenses_df(
        db_path=path,
        start_date=start_date,
        end_date=end_date,
        category=category,
    )
    if df is None or df.empty:
        return []

    out: List[Dict] = []
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


def get_conn(db_path: Optional[str] = None):
    """
    Старое имя: вернуть соединение. Перед отдачей гарантируем схему.
    Если путь не передан — берём активный (через session_state).
    """
    path = _resolve_db_path(db_path)
    ensure_schema(path)
    return sqlite3.connect(path)


def get_all_expenses(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category: Optional[str] = None,
    db_path: Optional[str] = None,
):
    """
    Старое имя: получить все расходы (как DataFrame) с фильтрами.
    """
    path = _resolve_db_path(db_path)
    return get_expenses_df(
        db_path=path, start_date=start_date, end_date=end_date, category=category
    )


def list_categories(db_path: Optional[str] = None) -> list[str]:
    """
    Список категорий по алфавиту.
    """
    path = _resolve_db_path(db_path)
    sql = "SELECT DISTINCT category FROM expenses ORDER BY category ASC"
    with sqlite3.connect(path) as conn:
        cur = conn.execute(sql)
        return [str(r[0]) for r in cur.fetchall()]


def migrate_json_to_sqlite(
    json_path: Union[str, PathLike] = "expenses.json",
    db_path: Optional[str] = None,
) -> int:
    """
    Перенос из JSON в SQLite. Дубликаты игнорируются.
    Формат JSON: [{date, category, amount, description?}, ...]
    """
    import json

    path = _resolve_db_path(db_path)
    ensure_schema(path)

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return 0

    inserted = 0
    with sqlite3.connect(path) as conn:
        cur = conn.cursor()
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_expenses_uniq "
            "ON expenses(date, category, amount, COALESCE(description, ''))"
        )
        for row in data:
            date = str(row.get("date", "")).strip()
            category = str(row.get("category", "")).strip()
            try:
                amount = float(row.get("amount", 0) or 0)
            except Exception:
                amount = 0.0
            desc = row.get("description") or row.get("note") or ""

            if not date or not category:
                continue

            try:
                cur.execute(
                    "INSERT OR IGNORE INTO expenses(date, category, amount, description) "
                    "VALUES (?,?,?,?)",
                    (date, category, amount, desc),
                )
                inserted += cur.rowcount
            except Exception:
                # пропускаем мусорные строки
                pass

        conn.commit()

    return inserted
