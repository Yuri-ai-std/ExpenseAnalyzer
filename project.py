# project.py - Expense Analyzer with multi-language support 🌎

import json
import os
import sqlite3
from datetime import datetime
from messages import messages as project_messages
from collections import defaultdict
from utils import load_monthly_limits, save_monthly_limits
from db import add_expense_to_db, get_all_expenses, get_monthly_limits

EXPENSES_FILE = "expenses.json"
BUDGET_LIMITS_FILE = "budget_limits.json"
USE_SQLITE = True
DATABASE_FILE = "expenses.db"

def calculate_total_expenses(expenses):
    return sum(expense["amount"] for expense in expenses)

def load_expenses():
    conn = sqlite3.connect("expenses.db")
    cursor = conn.cursor()
    cursor.execute("SELECT date, category, amount, note FROM expenses")
    rows = cursor.fetchall()
    conn.close()

    expenses = []
    for row in rows:
        expense = {
            "date": row[0],
            "category": row[1],
            "amount": row[2],
            "note": row[3]
        }
        expenses.append(expense)
    return expenses
    
def save_expenses(expenses, file_path=EXPENSES_FILE):
    if USE_SQLITE:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS expenses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                category TEXT,
                amount REAL,
                note TEXT
            )
        """)
        cursor.execute("DELETE FROM expenses")  # очищаем таблицу перед новой вставкой
        for exp in expenses:
            cursor.execute("INSERT INTO expenses (date, category, amount, note) VALUES (?, ?, ?, ?)",
                           (exp['date'], exp['category'], exp['amount'], exp['note']))
        conn.commit()
        conn.close()
    else:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(expenses, f, indent=2, ensure_ascii=False)
            
def add_expense(messages, lang, budget_limits, categories):
    print(messages[lang]["enter_category"])
    for i, cat in enumerate(categories):
        print(f"{i + 1}. {cat}")
    
    try:
        cat_choice = int(input("> ")) - 1
        if cat_choice not in range(len(categories)):
            print(messages[lang]["invalid_category"])
            return
        category = categories[cat_choice]
    except ValueError:
        print(messages[lang]["invalid_category"])
        return

    try:
        amount = float(input(messages[lang]["enter_amount"] + " "))
    except ValueError:
        print(messages[lang]["invalid_amount"])
        return

    description = input(messages[lang]["enter_description"] + " ")
    date_str = input(messages[lang]["enter_date"] + " ")
    try:
        date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        print(messages[lang]["invalid_date"])
        return

    # 💾 Сохраняем в SQLite, вместо списка
    add_expense_to_db(str(date), category, amount, description)

    print(messages[lang]["expense_added"])
    
def summarize_expenses(db_path, messages, lang, budget_limits=None, by_date=False):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    if by_date:
        cursor.execute("""
            SELECT date, category, SUM(amount) 
            FROM expenses 
            GROUP BY date, category
            ORDER BY date
        """)
        daily_totals = defaultdict(lambda: defaultdict(float))
        for date, category, amount in cursor.fetchall():
            daily_totals[date][category] += amount

        for date in sorted(daily_totals):
            print(f"\n{date}:")
            for category, total in daily_totals[date].items():
                print(f"  {category}: ${total:.2f}")

    else:
        cursor.execute("""
            SELECT substr(date, 1, 7) as month, category, SUM(amount)
            FROM expenses
            GROUP BY month, category
            ORDER BY month
        """)
        monthly_totals = defaultdict(lambda: defaultdict(float))
        for month, category, amount in cursor.fetchall():
            monthly_totals[month][category] += amount

        for month in sorted(monthly_totals):
            print(f"\n{month}:")
            for category, total in monthly_totals[month].items():
                line = f"  {category}: ${total:.2f}"
                if budget_limits:
                    month_limits = budget_limits.get(month, {})
                    limit = month_limits.get(category)
                    if limit is not None:
                        status = (messages["over_limit"][lang]
                                  if total > limit
                                  else messages["within_limit"][lang])
                        line += f" → {status} (Limit: ${limit:.2f})"
                print(line)

    conn.close()
    
def get_budget_tips(messages):
    """
    Display financial tips for the user.
    Currently uses static messages, but can be extended to dynamic tips based on user's data.

    Args:
        messages (dict): Dictionary of localized messages and tips.

    Returns:
        None
    """

    print(messages["tips_header"])
    tips = [
        messages.get("tip_1", "Track your spending regularly."),
        messages.get("tip_2", "Set realistic monthly budgets."),
        messages.get("tip_3", "Avoid impulse purchases.")
    ]
    for tip in tips:
        print("- " + tip)
        
def get_valid_date(prompt, messages):
    while True:
        date_str = input(prompt).strip()
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return date_str
        except ValueError:
            print(messages["invalid_date_format"])

def filter_expenses_by_date(start_date=None, end_date=None):
    print(messages["filter_prompt"])

    if start_date is None:
        start_date = get_valid_date(messages["start_date"], messages)
    if end_date is None:
        end_date = get_valid_date(messages["end_date"], messages)

    conn = sqlite3.connect("expenses.db")
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT date, category, amount, note
        FROM expenses
        WHERE date BETWEEN ? AND ?
        ORDER BY date ASC
        """,
        (start_date, end_date)
    )

    rows = cursor.fetchall()
    conn.close()

    if not rows:
        print(messages["no_expenses_found"])
        return []

    expenses = []
    for row in rows:
        date, category, amount, note = row
        expenses.append({
            "date": date,
            "category": category,
            "amount": amount,
            "note": note
        })

    summarize_expenses(expenses, messages, "en")
    return expenses
    
def show_summary(expenses, messages):
    # Заголовок
    print("\n" + messages["expense_summary"])

    # Выбор фильтрации по дате
    filter_range = input(messages["filter_by_date"]).strip().lower()
    filtered_expenses = expenses

    if filter_range == "yes":
        # Запрос и проверка start_date
        while True:
            start_date = input(messages["start_date"])
            try:
                datetime.strptime(start_date, "%Y-%m-%d")
                break
            except ValueError:
                print(messages["invalid_date_format"])

        # Запрос и проверка end_date
        while True:
            end_date = input(messages["end_date"])
            try:
                datetime.strptime(end_date, "%Y-%m-%d")
                break
            except ValueError:
                print(messages["invalid_date_format"])

        # Фильтрация по диапазону дат
        filtered_expenses = [
            exp for exp in expenses
            if start_date <= exp["date"] <= end_date
        ]

    # Суммируем и выводим все подходящие расходы
    total = 0
    for exp in filtered_expenses:
        print(f"{exp['date']} - {exp['category'].capitalize()}: ${exp['amount']:.2f} ({exp['description']})")
        total += exp["amount"]

    print(messages["total_expenses"].format(total=total))
    input(messages["press_enter_to_continue"])

def load_monthly_limits(filename=BUDGET_LIMITS_FILE):
    if os.path.exists(filename):
        with open(filename, "r") as f:
            return json.load(f)
    else:
        return {}

def save_monthly_limits(budget_limits, filename=BUDGET_LIMITS_FILE):
    with open(filename, "w") as file:
        json.dump(budget_limits, file, indent=4)

def show_monthly_summary(expenses, messages):
    print("\n" + messages["expense_summary"])  # ✅ уже должен быть в messages
    monthly_data = defaultdict(lambda: defaultdict(float))

    for expense in expenses:
        date = expense["date"]
        if isinstance(date, str):
            date = datetime.strptime(date, "%Y-%m-%d").date()
        month_key = date.strftime("%Y-%m")
        monthly_data[month_key][expense["category"]] += expense["amount"]

    for month in sorted(monthly_data):
        print(f"\n{month}:")
        for category, amount in monthly_data[month].items():
            print(f"  {category.capitalize()}: ${amount:.2f}")

def check_budget_limits(conn, budget_limits, messages, start_date=None, end_date=None):
    cursor = conn.cursor()

    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    if start_date and end_date:
        # 🔹 Анализ по месяцам в заданном диапазоне
        cursor.execute("""
            SELECT date, category, amount FROM expenses
            WHERE date BETWEEN ? AND ?
        """, (start_date, end_date))
    else:
        # 🔸 Анализ по всем записям
        cursor.execute("SELECT date, category, amount FROM expenses")

    rows = cursor.fetchall()
    expenses_by_month = {}

    for row in rows:
        date_str, category, amount = row
        date = datetime.strptime(date_str, "%Y-%m-%d").date()
        month = date.strftime("%Y-%m")

        if month not in expenses_by_month:
            expenses_by_month[month] = []
        expenses_by_month[month].append((category, amount))

    for month, month_expenses in expenses_by_month.items():
        print(f"\n=== Budget check for {month} ===")
        totals = {}

        for category, amount in month_expenses:
            totals[category] = totals.get(category, 0) + amount

        if month in budget_limits:
            for category, total in totals.items():
                limit = budget_limits[month].get(category)
                if limit is not None and total > limit:
                    print(messages["over_limit"].format(
                        category=category,
                        total=total,
                        limit=limit
                    ))
        else:
            print(messages["no_limits_defined"].format(month=month))
            
def update_budget_limits(budget_limits, categories, lang):
    messages = project_messages[lang]

    # 1. Запрос месяца
    month_str = input(messages["enter_month"])  # Пример: "2025-07"

    # 2. Проверка и инициализация словаря для месяца
    if month_str not in budget_limits:
        budget_limits[month_str] = {}

    # 3. Показываем текущие лимиты (если есть)
    print(messages["current_limits"].format(month=month_str))
    for cat in categories:
        limit = budget_limits[month_str].get(cat, "not set")
        print(f"  {cat}: {limit}")

    # 4. Обновляем лимиты
    for cat in categories:
        while True:
            try:
                limit_input = input(messages["prompt_budget_limit_for_category"].format(cat) + " (Press Enter to skip): ")
                if limit_input.strip() == "":
                    break  # Skip this category
                limit = float(limit_input)
                budget_limits[month_str][cat] = limit
                break
            except ValueError:
                print(messages["invalid_amount"])

    # 5. Подтверждение
    print(messages["budget_limit_updated"])

    # 6. Сохраняем лимиты в JSON
    save_monthly_limits(budget_limits)

def main():
    lang = input("Choose your language / Choisissez votre langue / Elige tu idioma (en/fr/es): ").lower()
    if lang not in project_messages:
        print("Language not supported. Defaulting to English.")
        lang = "en"

    messages = project_messages[lang]
    expenses = load_expenses()

    # Лимиты бюджета по месяцам (пример: июль 2025)
    budget_limits = load_monthly_limits()

    categories = [
        "food", "transport", "entertainment",
        "utilities", "rent", "groceries", "other"
    ]

    while True:
        print("\n" + messages["menu"])
        choice = input(messages["select_option"])

        if choice == "1":
            add_expense(expenses, messages, lang, budget_limits, categories)

        elif choice == "2":
            filter_choice = input(messages["filter_prompt"]).lower()
            if filter_choice in ("yes", "oui", "sí", "si"):
                start_date = get_valid_date(messages["start_date"], messages)
                end_date = get_valid_date(messages["end_date"], messages)
                filtered = [e for e in expenses if start_date <= e["date"] <= end_date]
                summarize_expenses(filtered, messages, lang, by_date=True)
                check_budget_limits(filtered, budget_limits, messages, start_date, end_date)
            else:
                show_monthly_summary(expenses, messages)  # 👉 ваша функция
                check_budget_limits(expenses, budget_limits, messages)

        elif choice == "3":
            get_budget_tips(messages)

        elif choice == "4":
            print(messages["goodbye"])
            save_monthly_limits(budget_limits)
            break

        elif choice == "5":
            update_budget_limits(budget_limits, categories, lang)

        else:
            print(messages["invalid_option"])

        save_expenses(expenses)

if __name__ == "__main__":
    main()

