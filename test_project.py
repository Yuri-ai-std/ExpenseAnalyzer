# test_project.py

import io
import os
import sys
import json
import pytest
import tempfile
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

def test_check_budget_limits_exceeded(monkeypatch, capsys):
    # 1. Фиктивные расходы
    expenses = [
        {"date": "2025-07-23", "category": "food", "amount": 60.0},
        {"date": "2025-07-24", "category": "transport", "amount": 20.0}
    ]

    # 2. Лимиты: food ограничен 50
    monthly_limits = {
        "2025-07": {"food": 50.0, "transport": 100.0}
    }

    # 3. Сообщения
    messages = {
        "budget_check_header": "\n=== Budget check for all time ===",
        "budget_exceeded": "⚠️ Over budget for",
        "budget_within_limits": "✅ Budget within limits.",
        "category_total": "🔸 Category:",
        "limit": "Limit:"
    }

    # 4. Запуск функции
    check_budget_limits(expenses, monthly_limits, messages)

    # 5. Захватываем вывод
    output = capsys.readouterr().out

    # 6. Проверка наличия ожидаемого вывода
    assert "Over budget for food" in output
    assert "60.00 > 50.00" in output

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


