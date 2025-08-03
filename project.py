# project.py - Expense Analyzer with multi-language support 🌎

import json
import os
from datetime import datetime
from messages import messages as project_messages
from collections import defaultdict
from utils import load_monthly_limits, save_monthly_limits

EXPENSES_FILE = "expenses.json"
BUDGET_LIMITS_FILE = "budget_limits.json"

def calculate_total_expenses(expenses):
    return sum(expense["amount"] for expense in expenses)

def load_expenses(file_path=EXPENSES_FILE):
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def save_expenses(expenses, file_path=EXPENSES_FILE):
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(expenses, f, indent=2, ensure_ascii=False)

def add_expense(expenses, messages, lang, budget_limits, categories):

    print(messages["add_expense_header"])
    print(f'{messages["valid_categories"]}: {", ".join(categories)}')

    while True:
        category = input(messages["enter_category"]).strip().lower()

        if category in [c.lower() for c in categories]:
            break
        elif category in ["education", "subscriptions", "stationery"]:
            print(messages["auto_assign_other"])
            category = "other"
            break
        else:
            print(messages["invalid_category"])
            print(f'{messages["valid_categories"]}: {", ".join(categories)}')

    try:
        amount = float(input(messages["enter_amount"]))
    except ValueError:
        print(messages["invalid_amount"])
        return

    description = input(messages["enter_description"])
    date = datetime.today().strftime("%Y-%m-%d")

    expenses.append({
        "category": category,
        "amount": amount,
        "description": description,
        "date": date
    })

    print(messages["expense_added"])
    input(messages["press_enter_to_continue"])

    # 🔽 Определяем текущий месяц
    current_month = date[:7]  # формат YYYY-MM

    # 🔽 Проверка лимитов по категориям за этот месяц
    category_totals = {}
    for exp in expenses:
        exp_month = exp["date"][:7]
        if exp_month == current_month:
            cat = exp["category"].lower()
            amt = exp["amount"]
            category_totals[cat] = category_totals.get(cat, 0) + amt

    print(messages["budget_check_header"])
    for cat, total in category_totals.items():
        limit = budget_limits.get(current_month, {}).get(cat, 0)
        if total > limit:
            print(messages["category_over_limit"].format(
                category=cat.capitalize(), total=total, limit=limit))
        elif total >= limit * 0.9 and limit > 0:
            print(messages["category_near_limit"].format(
                category=cat.capitalize(), total=total, limit=limit))
        else:
            print(messages["category_within_limit"].format(
                category=cat.capitalize(), total=total, limit=limit))

def summarize_expenses(expenses, messages, lang, budget_limits=None, by_date=False):
    print(messages["expense_summary"])
    if by_date:
        daily_totals = {}
        for e in expenses:
            date = e["date"]
            if date not in daily_totals:
                daily_totals[date] = {}
            category = e["category"]
            amount = e["amount"]
            daily_totals[date][category] = daily_totals[date].get(category, 0) + amount

        for date in sorted(daily_totals.keys()):
            print(f"\nDate: {date}")
            for category, total in daily_totals[date].items():
                print(f"  {category}: ${total:.2f}")
    else:
        monthly_totals = {}
        for e in expenses:
            month = e["date"][:7]
            category = e["category"]
            amount = e["amount"]
            if month not in monthly_totals:
                monthly_totals[month] = {}
            monthly_totals[month][category] = monthly_totals[month].get(category, 0) + amount

        for month in sorted(monthly_totals.keys()):
            print(f"\nMonth: {month}")
            for category, total in monthly_totals[month].items():
                print(f"  {category}: ${total:.2f}")

            if budget_limits and month in budget_limits:
                print(messages["current_limits"].format(month=month))
                for category, limit in budget_limits[month].items():
                    spent = monthly_totals[month].get(category, 0)
                    status = messages["limit_exceeded"] if spent > limit else messages["within_limit"]
                    print(f"  {category}: ${spent:.2f} / ${limit:.2f} → {status}")

def get_budget_tips(messages):
    print(messages["tips_header"])
    print("- " + messages["tip_1"])
    print("- " + messages["tip_2"])
    print("- " + messages["tip_3"])

def get_valid_date(prompt, messages):
    while True:
        date_str = input(prompt).strip()
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return date_str
        except ValueError:
            print(messages["invalid_date_format"])

def filter_expenses_by_date(expenses, messages, start_date=None, end_date=None):
    print(messages["filter_prompt"])

    if start_date is None:
        start_date = get_valid_date(messages["start_date"], messages)
    if end_date is None:
        end_date = get_valid_date(messages["end_date"], messages)

    filtered = [
        expense for expense in expenses
        if start_date <= expense["date"] <= end_date
    ]

    summarize_expenses(filtered, messages, "en")
    return filtered

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

def check_budget_limits(expenses, budget_limits, messages, start_date=None, end_date=None):
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    if start_date and end_date:
        # 🔹 Группируем расходы по месяцам
        expenses_by_month = {}
        for expense in expenses:
            date = datetime.strptime(expense["date"], "%Y-%m-%d").date()
            if start_date <= date <= end_date:
                month_str = date.strftime("%Y-%m")
                if month_str not in expenses_by_month:
                    expenses_by_month[month_str] = []
                expenses_by_month[month_str].append(expense)

        # 🔍 Анализируем каждый месяц отдельно
        for month, month_expenses in expenses_by_month.items():
            print(f"\n=== Budget check for {month} ===")
            monthly_totals = {}
            for exp in month_expenses:
                cat = exp["category"]
                monthly_totals[cat] = monthly_totals.get(cat, 0) + exp["amount"]

            if month in budget_limits:
                for cat, total in monthly_totals.items():
                    limit = budget_limits[month].get(cat)
                    if limit is not None and total > limit:
                        print(f"⚠️  Over budget for {cat}: {total:.2f} > {limit:.2f}")
            else:
                print("ℹ️  No budget limits defined for this month.")
    else:
        # 🔸 Стандартная логика, как раньше
        print("\n=== Budget check for all time ===")
        total_by_category = {}
        for expense in expenses:
            cat = expense["category"]
            total_by_category[cat] = total_by_category.get(cat, 0) + expense["amount"]

        for cat, total in total_by_category.items():
            limit = 0
            # Складываем лимиты по всем месяцам
            for month_limits in budget_limits.values():
                limit += month_limits.get(cat, 0)
            if limit and total > limit:
                print(f"⚠️  Over budget for {cat}: {total:.2f} > {limit:.2f}")

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

