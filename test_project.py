# test_project.py

import io
import os
import sys
import json
import pytest
import tempfile
import sqlite3
from messages import messages
from project import (
    add_expense,
    calculate_total_expenses,
    check_budget_limits,
    summarize_expenses,
    filter_expenses_by_date,
    load_monthly_limits,
    save_monthly_limits,
)

def test_add_expense_and_calculate_total(tmp_path):
    # 1. Создаём путь к временному файлу
    temp_file = tmp_path / "expenses.json"

    # 2. Подготавливаем тестовые расходы
    expenses = [
        {"date": "2025-07-23", "category": "food", "amount": 25.5, "description": "groceries"},
        {"date": "2025-07-24", "category": "transport", "amount": 10.0, "description": "bus ticket"},
        {"date": "2025-07-25", "category": "food", "amount": 14.5, "description": "snack"},
    ]

    # 3. Сохраняем их в JSON-файл
    temp_file.write_text(json.dumps(expenses, ensure_ascii=False, indent=2), encoding="utf-8")

    # 4. Читаем из файла и считаем сумму
    loaded_expenses = json.loads(temp_file.read_text(encoding="utf-8"))
    total = calculate_total_expenses(loaded_expenses)

    # 5. Проверяем сумму
    assert total == 50.0

def test_check_budget_limits_exceeded(tmp_path, capsys):
    import sqlite3
    from project import check_budget_limits

    # 1. Подготовка временной базы данных
    db_path = tmp_path / "test_expenses.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            category TEXT,
            amount REAL,
            note TEXT
        )
    """)

    # 2. Вставка фиктивных расходов
    expenses_data = [
        ("2025-07-23", "food", 60.0, "groceries"),
        ("2025-07-24", "transport", 20.0, "bus"),
    ]
    cursor.executemany("""
        INSERT INTO expenses (date, category, amount, note)
        VALUES (?, ?, ?, ?)
    """, expenses_data)
    conn.commit()

    # 3. Определение лимитов
    budget_limits = {
        "2025-07": {
            "food": 50.0,
            "transport": 100.0
        }
    }

    # 4. Сообщения
    messages = {
        "over_limit": "{category} ❌ ${total:.2f} > ${limit:.2f}",
        "no_limits_defined": "No limits defined for {month}",
    }

    # 5. Вызов тестируемой функции
    check_budget_limits(conn, budget_limits, messages)

    # 6. Проверка вывода
    captured = capsys.readouterr()
    assert "food ❌ $60.00 > $50.00" in captured.out
    assert "transport" not in captured.out

    conn.close()

def test_summarize_expenses(capsys):
    # 1. Фиктивные расходы
    expenses = [
        {"date": "2025-07-21", "category": "food", "amount": 20.0, "description": "groceries"},
        {"date": "2025-07-22", "category": "food", "amount": 10.0, "description": ""},
        {"date": "2025-07-22", "category": "transport", "amount": 15.0, "description": "bus"},
    ]

    # 2. Сообщения
    messages = {
        "expense_summary": "📊 Expense Summary",
        "category_total": "🧾 ",
        "note": "📝 Note:"
    }

    # 3. Запуск функции
    summarize_expenses(expenses, messages, "en")
    output = capsys.readouterr().out

    # 4. Проверка
    assert "📊 Expense Summary" in output
    assert "food: $30.00" in output
    assert "transport: $15.00" in output

def test_filter_expenses_by_date():
    expenses = [
        {"date": "2025-07-20", "category": "food", "amount": 10.0},
        {"date": "2025-07-22", "category": "transport", "amount": 5.0},
        {"date": "2025-07-25", "category": "food", "amount": 7.0},
        {"date": "2025-08-01", "category": "groceries", "amount": 12.0}
    ]

    start_date = "2025-07-21"
    end_date = "2025-07-31"

    messages = {
        "filter_prompt": "🗂️ Filtering by date range...",
        "expense_summary": "📊 Expense Summary",
        "category_total": "🧾 ",
        "note": "📝 Note:"
    }

    from project import filter_expenses_by_date
    filtered = filter_expenses_by_date(expenses, messages, start_date, end_date)

    expected = [
        {"date": "2025-07-22", "category": "transport", "amount": 5.0},
        {"date": "2025-07-25", "category": "food", "amount": 7.0}
    ]

    assert filtered == expected

def test_save_and_load_expenses(tmp_path):
    test_expenses = [
        {"date": "2025-07-20", "category": "food", "amount": 10.0, "note": "test1"},
        {"date": "2025-07-21", "category": "transport", "amount": 5.5, "note": "test2"}
    ]

    # Путь к временной директории
    file_path = tmp_path / "expenses_test.json"

    # Импорт с заменой пути
    import project
    original_path = project.EXPENSES_FILE if hasattr(project, "EXPENSES_FILE") else "expenses.json"
    project.save_expenses(test_expenses, str(file_path))
    loaded = project.load_expenses(str(file_path))

    assert loaded == test_expenses

    # Восстановление переменной (на всякий случай)
    project.EXPENSES_FILE = original_path

def test_load_and_save_monthly_limits():
    # 📦 Подготовка тестовых данных
    test_data = {
        "2025-07": {
            "food": 250.0,
            "transport": 100.0,
            "utilities": 150.0
        }
    }

    # 🗂 Создаём временный файл
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        temp_filename = tmp.name

    try:
        # 💾 Сохраняем данные
        save_monthly_limits(test_data, filename=temp_filename)

        # 📥 Загружаем обратно
        loaded_data = load_monthly_limits(filename=temp_filename)

        # ✅ Проверяем соответствие
        assert loaded_data == test_data

    finally:
        # 🧹 Удаляем временный файл после теста
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

@pytest.mark.parametrize("lang", ["en", "fr", "es"])
def test_message_keys_exist(lang):
    required_keys = [
        "filter_prompt", "expense_summary", "budget_limit_updated",
        "prompt_budget_limit_for_category", "invalid_amount", "enter_month"
    ]
    for key in required_keys:
        assert key in messages[lang], f"Missing '{key}' in language: {lang}"


