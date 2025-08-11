# project.py — Expense Analyzer with multi-language support + SQLite

import os
import csv
import sqlite3
import json
from datetime import datetime
from collections import defaultdict
from typing import List, Dict, Optional
from messages import messages as project_messages
from utils import load_monthly_limits, save_monthly_limits
from db import (
    get_conn,
    migrate_json_to_sqlite,
    add_expense_to_db,
    get_all_expenses,
)

# Старые имена файлов — оставлены только для обратной совместимости/экспорта
EXPENSES_FILE = "expenses.json"
BUDGET_LIMITS_FILE = "budget_limits.json"
DATABASE_FILE = "expenses.db"


def calculate_total_expenses(expenses):
    return sum(float(e.get("amount", 0)) for e in expenses)


# --------------------------- утилиты вывода ---------------------------


def load_expenses(file_path=EXPENSES_FILE):
    """
    Для тестов: грузит расходы из JSON, если файл существует.
    Если файла нет — возвращает [] (а не лезет в БД).
    """
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_expenses(expenses, file_path=EXPENSES_FILE):
    """
    Для тестов: сохраняет список расходов в JSON по указанному пути.
    """
    os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(expenses, f, ensure_ascii=False, indent=2)


def summarize_expenses(expenses, messages, lang, budget_limits=None, by_date=False):
    """
    Принимает ИЛИ список расходов (list[dict]),
    ИЛИ путь к sqlite-файлу с таблицей expenses.
    """
    # если передали путь к БД — читаем из нее и конвертируем в list[dict]
    if isinstance(expenses, (str, os.PathLike)):
        import sqlite3

        items = []
        conn = sqlite3.connect(str(expenses))
        cur = conn.cursor()
        cur.execute("SELECT date, category, amount, note FROM expenses ORDER BY date")
        for d, c, a, n in cur.fetchall():
            items.append({"date": d, "category": c, "amount": float(a), "note": n})
        conn.close()
        expenses = items

    # дальше работаем со списком словарей
    from collections import defaultdict

    if by_date:
        daily = defaultdict(lambda: defaultdict(float))
        for e in expenses:
            daily[e["date"]][e["category"]] += float(e["amount"])
        for day in sorted(daily):
            print(f"\n{day}:")
            for cat, total in daily[day].items():
                print(f"  {cat}: ${total:.2f}")
        return

    monthly = defaultdict(lambda: defaultdict(float))
    for e in expenses:
        month = e["date"][:7]
        monthly[month][e["category"]] += float(e["amount"])

    for month in sorted(monthly):
        print(f"\n{month}:")
        for category, total in monthly[month].items():
            line = f"  {category}: ${total:.2f}"
            if budget_limits:
                limit = budget_limits.get(month, {}).get(category)
                if limit is not None:
                    status = (
                        messages.get("over_limit", "Over").format(category=category)
                        if total > limit
                        else messages.get("within_limit", "Within")
                    )
                    line += f" → {status} (Limit: ${float(limit):.2f})"
            print(line)


def show_monthly_summary(expenses, messages):
    print("\n" + messages["expense_summary"])
    monthly = defaultdict(lambda: defaultdict(float))
    for e in expenses:
        d = e["date"]
        if isinstance(d, str):
            d = datetime.strptime(d, "%Y-%m-%d").date()
        key = d.strftime("%Y-%m")
        monthly[key][e["category"]] += float(e["amount"])
    for month in sorted(monthly):
        print(f"\n{month}:")
        for cat, total in monthly[month].items():
            print(f"  {cat.capitalize()}: ${total:.2f}")


def get_valid_date(prompt, messages):
    while True:
        s = input(prompt).strip()
        try:
            datetime.strptime(s, "%Y-%m-%d")
            return s
        except ValueError:
            print(messages["invalid_date_format"])


# --------------------------- работа с БД ---------------------------


def filter_expenses_by_date_db(
    start_date: str,
    end_date: str,
    messages: Optional[Dict] = None,
    db_path: str = "expenses.db",
) -> List[Dict]:
    """
    Реальная (SQLite) версия: возвращает список расходов из БД в диапазоне дат.
    Возвращает список словарей: {date, category, amount, note}.
    Ничего не печатает (чтобы не мешать тестам).
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT date, category, amount, note
        FROM expenses
        WHERE date BETWEEN ? AND ?
        ORDER BY date ASC
        """,
        (start_date, end_date),
    )
    rows = cur.fetchall()
    conn.close()

    expenses = [
        {"date": d, "category": c, "amount": float(a), "note": n} for d, c, a, n in rows
    ]

    if not expenses and messages:
        msg = messages.get("no_expenses_found")
        if msg:
            print(msg)

    return expenses


def filter_expenses_by_date(
    start_date: str,
    end_date: str,
    messages: Optional[Dict] = None,
) -> List[Dict]:
    """
    Обёртка с сигнатурой, которую ожидают тесты.
    Делегирует реальной DB-версии, открывая expenses.db из текущей директории.
    """
    db_path = os.path.join(os.getcwd(), "expenses.db")
    return filter_expenses_by_date_db(start_date, end_date, messages, db_path=db_path)


def check_budget_limits(conn, budget_limits, messages, start_date=None, end_date=None):
    """
    Считает суммы по месяцам/категориям из БД и сравнивает с лимитами.
    Печатает только превышения.
    """
    cur = conn.cursor()
    params = ()
    where = ""
    if start_date and end_date:
        # сравнение строк YYYY-MM-DD в SQLite корректно
        where = "WHERE date BETWEEN ? AND ?"
        params = (str(start_date), str(end_date))

    cur.execute(
        f"""
        SELECT substr(date, 1, 7) AS month, category, SUM(amount) AS total
        FROM expenses
        {where}
        GROUP BY month, category
        ORDER BY month
        """,
        params,
    )
    for month, category, total in cur.fetchall():
        if month in budget_limits:
            limit = budget_limits[month].get(category)
            if limit is not None and float(total) > limit:
                print(
                    messages["over_limit"].format(
                        category=category, month=month, total=float(total), limit=limit
                    )
                )
        else:
            # если нет лимитов для месяца
            if "no_limits_defined" in messages:
                print(messages["no_limits_defined"].format(month=month))


def export_to_csv(db_path, out_path, start_date=None, end_date=None, category=None):
    """
    Экспортирует расходы из БД в CSV с опциональными фильтрами.
    Колонки: date, category, amount, note. Сортировка по date ASC.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    query = """
        SELECT date, category, amount, note
        FROM expenses
        WHERE 1=1
    """
    params = []

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)
    if category:
        query += " AND category = ?"
        params.append(category)

    query += " ORDER BY date ASC"
    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "category", "amount", "note"])
        for r in rows:
            w.writerow(r)
    print(f"CSV exported → {out_path}")


# --------------------------- ввод расходов ---------------------------


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

    # сохраняем сразу в SQLite
    add_expense_to_db(str(date), category, amount, description)
    print(messages[lang]["expense_added"])


# --------------------------- main меню ---------------------------


def main():
    # миграция старого expenses.json → expenses.db (безопасна при повторном вызове)
    migrate_json_to_sqlite(EXPENSES_FILE, DATABASE_FILE)

    lang = input(
        "Choose your language / Choisissez votre langue / Elige tu idioma (en/fr/es): "
    ).lower()
    if lang not in project_messages:
        print("Language not supported. Defaulting to English.")
        lang = "en"
    messages = project_messages[lang]

    # лимиты остаются в JSON (совместимость с тестами и простота редактирования)
    budget_limits = load_monthly_limits(BUDGET_LIMITS_FILE)

    categories = [
        "food",
        "transport",
        "entertainment",
        "utilities",
        "rent",
        "groceries",
        "other",
    ]

    while True:
        print("\n" + messages["menu"])
        print("(6) Export to CSV")  # временно жёстко в меню
        choice = input(messages["select_option"])

        if choice == "1":
            add_expense(messages, lang, budget_limits, categories)

        elif choice == "2":
            filter_choice = input(messages["filter_prompt"]).lower()
            if filter_choice in ("yes", "oui", "sí", "si"):
                start_date = get_valid_date(messages["start_date"], messages)
                end_date = get_valid_date(messages["end_date"], messages)
                filtered = filter_expenses_by_date_db(start_date, end_date, messages)
                summarize_expenses(filtered, messages, lang, by_date=True)
                with get_conn(DATABASE_FILE) as conn:
                    check_budget_limits(
                        conn, budget_limits, messages, start_date, end_date
                    )
            else:
                expenses = get_all_expenses()
                show_monthly_summary(expenses, messages)
                with get_conn(DATABASE_FILE) as conn:
                    check_budget_limits(conn, budget_limits, messages)

        elif choice == "3":
            print(messages["tips_header"])
            for k in ("tip_1", "tip_2", "tip_3"):
                if k in messages:
                    print("- " + messages[k])

        elif choice == "4":
            print(messages["goodbye"])
            save_monthly_limits(budget_limits, BUDGET_LIMITS_FILE)
            break

        elif choice == "5":
            # обновление лимитов (осталось JSON-ориентированным)
            month = input(messages["enter_month"])
            budget_limits.setdefault(month, {})
            print(messages["current_limits"].format(month=month))
            for cat in categories:
                cur = budget_limits[month].get(cat, "not set")
                print(f"  {cat}: {cur}")
            for cat in categories:
                s = input(
                    messages["prompt_budget_limit_for_category"].format(cat)
                    + " (Press Enter to skip): "
                ).strip()
                if not s:
                    continue
                try:
                    budget_limits[month][cat] = float(s)
                except ValueError:
                    print(messages["invalid_amount"])
            print(messages["budget_limit_updated"])
            save_monthly_limits(budget_limits, BUDGET_LIMITS_FILE)

        elif choice == "6":
            # простой экспорт в CSV
            out_path = input("CSV path (e.g. export.csv): ").strip() or "export.csv"
            sd = input("Start date YYYY-MM-DD (optional): ").strip() or None
            ed = input("End date YYYY-MM-DD (optional): ").strip() or None
            cat = input("Category (optional): ").strip() or None
            export_to_csv(DATABASE_FILE, out_path, sd, ed, cat)

        else:
            print(messages["invalid_option"])


if __name__ == "__main__":
    main()
