# db.py — ExpenseAnalyzer (каноника 2025-09-02)

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import List, Optional, Union

import pandas as pd

# Если у вас уже есть DATA_DIR в utils — импортируйте оттуда.
# from utils import DATA_DIR
DATA_DIR = Path("data")

# Стартовый набор категорий для новых профилей
DEFAULT_CATEGORIES: list[str] = [
    "food",
    "transport",
    "groceries",
    "utilities",
    "entertainment",
    "other",
]

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


def ensure_limits_file(limits_path: Path, seed: list[str] | None = None) -> None:
    """
    Создаёт JSON лимитов, если его ещё нет. Инициализирует нулевыми значениями по seed-категориям.
    Структура:
    {
      "limits": { "food": 0, ... },
      "months": { "YYYY-MM": { ... } }   # если в будущем захотите помесячно
    }
    """
    if limits_path.exists():
        return
    limits_path.parent.mkdir(parents=True, exist_ok=True)
    cats = seed or DEFAULT_CATEGORIES
    payload = {"limits": {c: 0 for c in cats}, "months": {}}
    limits_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )


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


def _limits_path_for_db(db_path: PathLike) -> Path:
    """
    По пути БД data/<user>_expenses.db -> вернуть Path для лимитов: data/<user>_budget_limits.json.
    Именование выдержано по текущей конвенции проекта.
    """
    p = Path(_resolve_db_path(db_path))
    # извлечём <user> из "<user>_expenses.db"
    m = re.match(r"(.+?)_expenses\.db$", p.name)
    user = m.group(1) if m else "default"
    return DATA_DIR / f"{user}_budget_limits.json"


# ---- Соединение и справочные функции --------------------------------------


def list_categories(db_path: Optional[PathLike] = None) -> List[str]:
    """
    Возвращает список категорий пользователя:
    объединение ключей из лимитов и уникальных категорий из таблицы expenses.
    Гарантирует существование файла лимитов для нового профиля.
    """
    path = _resolve_db_path(db_path)
    ensure_db(path)

    # 1) категории из БД
    db_cats: set[str] = set()
    try:
        with sqlite3.connect(path) as conn:
            rows = conn.execute(
                "SELECT DISTINCT category FROM expenses WHERE category IS NOT NULL AND TRIM(category) <> ''"
            ).fetchall()
            db_cats.update(str(r[0]).strip() for r in rows if r and r[0])
    except Exception:
        pass

    # 2) категории из лимитов (создадим файл при необходимости)
    lim_path = _limits_path_for_db(path)
    ensure_limits_file(lim_path)  # ← важный шаг — создаст JSON с базовыми категориями
    limit_cats: set[str] = set()
    try:
        data = json.loads(lim_path.read_text(encoding="utf-8"))
        base = data.get("limits", {}) or {}
        limit_cats.update(k.strip() for k in base.keys() if k and str(k).strip())
    except Exception:
        pass

    cats = sorted((db_cats | limit_cats) - {""})
    return cats


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


# ---- Опционально: миграция из JSON ----------------------------------------


# ---- Backward compatibility shim (tests) ----
def ensure_schema(db_path: Optional[PathLike] = None) -> None:
    """Старое имя для ensure_db — оставлено для обратной совместимости тестов."""
    ensure_db(_resolve_db_path(db_path))
