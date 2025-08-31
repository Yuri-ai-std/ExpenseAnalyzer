# test_project.py
from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

# — тестируемые модули/функции
from db import add_expense, ensure_schema, get_expenses_df
from messages import messages as ALL_MESSAGES
from project import check_budget_limits, suggest_limits_for_month
from utils import month_key

# ---------- фикстуры ----------


@pytest.fixture()
def tmp_db(tmp_path: Path) -> str:
    """Создаёт пустую временную БД и возвращает её путь (str)."""
    db_path = tmp_path / "test_expenses.db"
    ensure_schema(str(db_path))
    return str(db_path)


@pytest.fixture()
def sample_data(tmp_db: str):
    """
    Наполняем БД минимальным набором: три месяца истории
    для 2–3 категорий. Возвращаем путь к БД.
    """
    today = date.today()
    # три последних месяца
    m0 = date(today.year, today.month, 1)
    m1 = (m0 - timedelta(days=1)).replace(day=1)
    m2 = (m1 - timedelta(days=1)).replace(day=1)

    # Фиктивные траты
    rows = [
        # m2
        (m2.replace(day=3), "food", 25.0, "groceries"),
        (m2.replace(day=9), "food", 30.0, "groceries"),
        (m2.replace(day=5), "transport", 10.0, "metro"),
        # m1
        (m1.replace(day=8), "food", 40.0, "groceries"),
        (m1.replace(day=2), "transport", 15.0, "metro"),
        (m1.replace(day=18), "other", 20.0, "misc"),
        # m0 (текущий)
        (m0.replace(day=1), "food", 12.0, "start of month"),
        (m0.replace(day=2), "transport", 5.0, "bus"),
    ]

    for d, cat, amt, desc in rows:
        add_expense(
            date=d.isoformat(),
            category=cat,
            amount=amt,
            description=desc,
            db_path=tmp_db,
        )

    return tmp_db


# ---------- тесты ----------


def test_get_expenses_df_reads_from_given_db_path(sample_data: str):
    """get_expenses_df должен брать данные именно из переданного db_path."""
    df = get_expenses_df(db_path=sample_data)
    assert isinstance(df, pd.DataFrame)
    # Вставлено 8 строк
    assert len(df) >= 8
    # Набор ключевых колонок
    for col in ("date", "category", "amount", "description"):
        assert col in df.columns


def test_suggest_limits_for_month_produces_values(sample_data: str):
    """
    На основе истории за прошлые месяцы должны появляться предложения лимитов
    для текущего месяца (не все нули).
    """
    mk = month_key(date.today())
    sugg = suggest_limits_for_month(user="default", month_key=mk)

    # словарь вида {category: float}
    assert isinstance(sugg, dict)
    assert "food" in sugg
    # как минимум для food получим положительный прогноз
    assert sugg["food"] > 0.0


def test_check_budget_limits_detects_overspend(sample_data: str):
    """
    Если задать низкий лимит на 'food', функция должна вернуть предупреждение.
    """
    msgs = ALL_MESSAGES["en"]  # словарь строк локали

    # Явно задаём низкие лимиты (структура {user: {category: limit}})
    limits = {"default": {"food": 10.0, "transport": 100.0, "other": 100.0}}

    with sqlite3.connect(sample_data) as conn:
        issues = check_budget_limits(
            conn,
            messages=msgs,
            budget_limits=limits,
        )

    assert isinstance(issues, list)
    assert any("food" in str(item).lower() for item in issues)
