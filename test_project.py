# test_project.py
import sqlite3
from pathlib import Path
from typing import Any, List, Dict

import pandas as pd
import pytest
import inspect

import project as prj
import db
import db as dbmod
from charts import show_charts

import os
from project import add_expense, DB_PATH


# ---------- маленькие утилиты для тестов ----------


def _df_records(df_or_items: Any) -> List[Dict]:
    """Принимает DataFrame или уже list[dict] и всегда возвращает list[dict]."""
    if isinstance(df_or_items, pd.DataFrame):
        return df_or_items.to_dict(orient="records")
    return list(df_or_items)  # на случай генератора/итератора


def _total_from_any(df_or_items: Any) -> float:
    """Считает сумму по полю 'amount' для DataFrame или list[dict]."""
    items = _df_records(df_or_items)
    return sum(float(e.get("amount", 0.0)) for e in items)


# ---------- фикстуры ----------


@pytest.fixture
def isolate_db(tmp_path, monkeypatch):
    test_db = tmp_path / "test_expenses.db"
    # Используем временный файл БД
    monkeypatch.setattr(db, "DB_PATH", str(test_db), raising=False)
    # Отключаем любые автоматические миграции в тестах
    monkeypatch.setattr(
        db, "migrate_json_to_sqlite", lambda *a, **k: None, raising=False
    )
    # Создаём/обновляем схему
    db.ensure_schema(str(test_db))
    # На всякий случай чистим таблицу
    import sqlite3

    with sqlite3.connect(str(test_db)) as conn:
        conn.execute("DELETE FROM expenses")
        conn.commit()
    yield


# ---------- сами тесты ----------


def test_add_expense_and_calculate_total(isolate_db):
    dbmod.add_expense("2025-07-01", "food", 100.0, "")

    # ВАЖНО: указываем путь явным параметром
    df = dbmod.get_expenses_df(db_path=dbmod.DB_PATH)

    total = _total_from_any(df)
    assert total == pytest.approx(100.0)


def test_check_budget_limits(isolate_db, capsys):

    # 1) локализация и лимиты
    msgs = prj.messages["en"]
    limits = {"2025-07": {"food": 70.0, "transport": 50.0}}

    # 2) три операции в июле (суммарно food = 90 > 70)
    dbmod.add_expense("2025-07-01", "food", 60.0, "")
    dbmod.add_expense("2025-07-02", "transport", 40.0, "")
    dbmod.add_expense("2025-07-03", "food", 30.0, "")

    # 3) вызов новой фасадной сигнатуры (все аргументы по именам)
    with sqlite3.connect(dbmod.DB_PATH) as conn:
        res = prj.check_budget_limits(
            conn=conn,
            messages=msgs,
            start_date="2025-07-01",
            end_date="2025-07-31",
            budget_limits=limits,
        )

    # 4) нормализуем результат: либо список строк, либо печать в stdout
    if isinstance(res, list):
        text = " ".join(map(str, res)).lower()
    else:
        text = (capsys.readouterr().out or "").lower()

    # 5) проверки: упоминание категории и факта превышения
    assert "food" in text
    assert ("over" in text) or ("limit" in text)


def test_summarize_expenses(isolate_db, capsys):
    # данные: food=80, transport=30
    for d, c, a in [
        ("2025-07-01", "food", 40.0),
        ("2025-07-02", "food", 40.0),
        ("2025-07-03", "transport", 30.0),
    ]:
        db.add_expense(d, c, a, "")

    df = db.get_expenses_df()
    records = _df_records(df)
    msgs = prj.messages["en"]

    # просим вернуть сводку по категориям
    result = prj.summarize_expenses(messages=msgs, lang="en", expenses=records)

    if isinstance(result, dict):
        # если вернули map категорий — проверяем сумму
        assert result.get("food") == pytest.approx(80.0)
    else:
        # иначе допускаем текст/список/печать
        if isinstance(result, str):
            text = result
        elif isinstance(result, list):
            text = " ".join(map(str, result))
        else:
            text = capsys.readouterr().out
        assert "food" in text.lower()


def test_get_expenses_df_returns_dataframe(isolate_db):
    df = db.get_expenses_df()
    # простая «проверка на DataFrame»
    assert hasattr(df, "to_dict")
    assert isinstance(df.to_dict(), dict)


def test_filter_expenses_by_date(isolate_db):
    # пара записей за июль
    db.add_expense("2025-07-05", "food", 10.0, "")
    db.add_expense("2025-07-10", "transport", 20.0, "")

    rows = prj.filter_expenses_by_date_db(
        start_date="2025-07-01",
        end_date="2025-07-31",
        messages=prj.messages["en"],
        db_path=db.DB_PATH,
    )
    assert isinstance(rows, list)
    assert all(isinstance(r, dict) for r in rows)


@pytest.mark.parametrize("lang", ["en", "fr", "es"])
def test_message_keys_exist(lang):
    msgs = prj.messages[lang]
    # базовый набор ключей локализаций, которые используются в проекте
    required = [
        "expense_added",
        "invalid_category",
        "enter_amount",
        "enter_date",
        "invalid_amount",
    ]
    for k in required:
        assert k in msgs


def test_list_categories_returns_list(isolate_db):
    # хотя бы одна категория должна появиться после добавления
    db.add_expense("2025-07-11", "food", 5.0, "")
    cats = prj.list_categories()
    assert isinstance(cats, list)
    assert "food" in {str(c).lower() for c in cats}


def test_charts_facade_runs_and_saves(isolate_db, tmp_path):
    # немного данных
    db.add_expense("2025-07-01", "food", 10.0, "")
    db.add_expense("2025-07-02", "transport", 20.0, "")
    db.add_expense("2025-07-03", "food", 30.0, "")

    out_dir = tmp_path / "reports" / "plots"
    paths = show_charts(out_dir=out_dir, lang="en")

    assert isinstance(paths, list) and paths
    for p in paths:
        assert isinstance(p, (str, Path))
        p = Path(p)
        assert p.exists() and p.stat().st_size > 0


def _call_db_add_expense(*, date, category, amount, description, db_path):
    """
    Универсальный вызов db.add_expense:
    – если в сигнатуре есть db_path — передаем его;
    – если нет — вызываем без него.
    """
    fn = db.add_expense
    params = inspect.signature(fn).parameters

    if "db_path" in params:
        return fn(
            date=date,
            category=category,
            amount=float(amount),
            description=description,
            db_path=str(db_path),
        )
    else:
        return fn(
            date=date,
            category=category,
            amount=float(amount),
            description=description,
        )


def test_add_expense_with_none_description(tmp_path):
    """Проверяем, что можно добавить запись с None в поле description."""
    db_file = tmp_path / "test_expenses.db"

    # минимальная схема для теста
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE expenses (
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

    # вызываем добавление (описание None)
    _call_db_add_expense(
        date="2025-08-21",
        category="Food",
        amount=20.5,
        description=None,
        db_path=db_file,  # будет проигнорировано, если параметра нет
    )

    # проверяем, что строка записалась и description = NULL
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()
    cur.execute("SELECT date, category, amount, description FROM expenses")
    row = cur.fetchone()
    conn.close()

    assert row is not None
    date, category, amount, description = row
    assert date == "2025-08-21"
    assert category == "Food"
    assert amount == 20.5
    assert description is None
