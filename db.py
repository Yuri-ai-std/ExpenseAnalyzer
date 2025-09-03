# db.py — ExpenseAnalyzer (каноника 2025-09-02)

from __future__ import annotations

from pathlib import Path
from typing import Optional, Union, List, Dict

import json
import sqlite3
import pandas as pd

# Если у вас уже есть DATA_DIR в utils — импортируйте оттуда.
# from utils import DATA_DIR
DATA_DIR = Path("data")

# Универсальный тип для путей
PathLike = Union[str, Path]

# ---- Схема и инициализация БД ---------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS expenses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    category TEXT NOT NULL,
    amount REAL NOT NULL,
    description TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_expenses_date ON expenses(date);
CREATE INDEX IF NOT EXISTS idx_expenses_category ON expenses(category);
"""


def ensure_db(db_path: PathLike) -> None:
    """Гарантированно создаёт файл БД и таблицы, если их нет."""
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(p) as conn:
        conn.executescript(SCHEMA_SQL)


# ---- Резолверы путей -------------------------------------------------------


def get_db_path(user: str = "default") -> str:
    """Путь к БД пользователя в корне data/: data/<user>_expenses.db"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return str(DATA_DIR / f"{user}_expenses.db")


DB_PATH: str = get_db_path("default")  # совместимость со старым кодом/тестами


def _resolve_db_path(db_path: Optional[PathLike] = None) -> str:
    """Возвращает str-путь: явный аргумент > session_state.current_user > default."""
    if db_path is not None:
        return str(Path(db_path))
    try:
        import streamlit as st  # локальный импорт, чтобы не тянуть Streamlit вне приложения

        user = st.session_state.get("current_user", "default")
    except Exception:
        user = "default"
    return get_db_path(user)


# ---- Соединение и справочные функции --------------------------------------


def get_conn(db_path: Optional[PathLike] = None):
    """Открывает sqlite3.Connection, перед этим гарантируя схему."""
    path = _resolve_db_path(db_path)
    ensure_db(path)
    return sqlite3.connect(path)


def list_categories(db_path: Optional[PathLike] = None) -> List[str]:
    """Список уникальных категорий расходов (для меню/фильтров)."""
    path = _resolve_db_path(db_path)
    ensure_db(path)
    with sqlite3.connect(path) as conn:
        rows = conn.execute(
            "SELECT DISTINCT category FROM expenses ORDER BY category"
        ).fetchall()
    return [r[0] for r in rows]


# ---- CRUD / выборки --------------------------------------------------------


def get_expenses_df(
    db_path: Optional[PathLike] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category: Optional[str] = None,
) -> pd.DataFrame:
    """
    Возвращает DataFrame из таблицы expenses с опциональными фильтрами.
    Всегда гарантирует существование схемы.
    """
    path = _resolve_db_path(db_path)
    ensure_db(path)

    where = ["1=1"]
    params: List[Union[str, float]] = []

    if start_date:
        where.append("date >= ?")
        params.append(start_date)
    if end_date:
        where.append("date <= ?")
        params.append(end_date)
    if category:
        where.append("category = ?")
        params.append(category)

    sql = f"""
        SELECT id, date, category, amount, COALESCE(description,'') AS description
        FROM expenses
        WHERE {' AND '.join(where)}
        ORDER BY date DESC, id DESC
    """
    with sqlite3.connect(path) as conn:
        return pd.read_sql_query(sql, conn, params=tuple(params))


def get_all_expenses(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category: Optional[str] = None,
    db_path: Optional[PathLike] = None,
) -> pd.DataFrame:
    """Старое имя: вернуть расходы как DataFrame (обёртка над get_expenses_df)."""
    path = _resolve_db_path(db_path)
    return get_expenses_df(
        db_path=path, start_date=start_date, end_date=end_date, category=category
    )


def load_expenses(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category: Optional[str] = None,
    db_path: Optional[PathLike] = None,
) -> List[Dict]:
    """
    Универсальная выборка расходов как список словарей (для меню/печати).
    Безопасно работает с пустыми БД.
    """
    path = _resolve_db_path(db_path)
    ensure_db(path)
    try:
        df = get_expenses_df(
            db_path=path, start_date=start_date, end_date=end_date, category=category
        )
    except Exception:
        return []
    if df is None or df.empty:
        return []
    out: List[Dict] = []
    for r in df.to_dict(orient="records"):
        out.append(
            {
                "date": str(r.get("date", "")),
                "category": str(r.get("category", "")),
                "amount": float(r.get("amount") or 0.0),
                "description": (r.get("description") or r.get("note") or "") or "",
            }
        )
    return out


def add_expense(
    date: str,
    category: str,
    amount: float,
    description: str = "",
    db_path: Optional[PathLike] = None,
) -> int:
    path = _resolve_db_path(db_path)
    ensure_db(path)
    with sqlite3.connect(path) as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO expenses(date, category, amount, description) VALUES (?, ?, ?, ?)",
            (date, category, amount, description or ""),
        )
        conn.commit()
        last_id = cur.lastrowid  # int | None
        if last_id is None:
            return 0
        return int(last_id)


def update_expense(
    expense_id: int,
    *,
    date: Optional[str] = None,
    category: Optional[str] = None,
    amount: Optional[float] = None,
    description: Optional[str] = None,
    db_path: Optional[PathLike] = None,
) -> int:
    """Частичное обновление записи. Возвращает число затронутых строк."""
    path = _resolve_db_path(db_path)
    ensure_db(path)

    sets: List[str] = []
    params: List[Union[str, float, int]] = []
    if date is not None:
        sets.append("date = ?")
        params.append(date)
    if category is not None:
        sets.append("category = ?")
        params.append(category)
    if amount is not None:
        sets.append("amount = ?")
        params.append(float(amount))
    if description is not None:
        sets.append("description = ?")
        params.append(description)

    if not sets:
        return 0

    if expense_id is None:
        return 0  # защита от Optional[int]

    params.append(int(expense_id))

    sql = f"UPDATE expenses SET {', '.join(sets)} WHERE id = ?"
    with sqlite3.connect(path) as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
        rc = cur.rowcount or 0  # rowcount может быть None по типам
        return int(rc)


def delete_expense(expense_id: int | None, db_path: Optional[PathLike] = None) -> int:
    path = _resolve_db_path(db_path)
    ensure_db(path)

    if expense_id is None:
        return 0

    with sqlite3.connect(path) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM expenses WHERE id = ?", (int(expense_id),))
        conn.commit()
        rc = cur.rowcount or 0
        return int(rc)


# ---- Опционально: миграция из JSON ----------------------------------------


def migrate_json_to_sqlite(
    json_path: PathLike, db_path: Optional[PathLike] = None
) -> int:
    """
    Импортирует список записей из JSON [ {date, category, amount, description?}, ... ]
    Возвращает количество вставленных строк.
    """
    path = _resolve_db_path(db_path)
    ensure_db(path)

    items = json.loads(Path(json_path).read_text(encoding="utf-8"))
    if not isinstance(items, list):
        return 0

    inserted = 0
    with sqlite3.connect(path) as conn:
        cur = conn.cursor()
        for r in items:
            cur.execute(
                "INSERT INTO expenses(date, category, amount, description) VALUES (?, ?, ?, ?)",
                (
                    r.get("date"),
                    r.get("category"),
                    float(r.get("amount") or 0.0),
                    r.get("description") or r.get("note") or "",
                ),
            )
            inserted += 1
        conn.commit()
    return inserted


# ---- Backward compatibility shim (tests) ----
def ensure_schema(db_path: Optional[PathLike] = None) -> None:
    """Старое имя для ensure_db — оставлено для обратной совместимости тестов."""
    ensure_db(_resolve_db_path(db_path))
