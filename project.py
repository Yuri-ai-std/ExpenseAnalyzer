# project.py — Expense Analyzer with multi-language support + SQLite

import os
import csv
import sqlite3
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import List, Dict, Optional, Any
from messages import messages
from collections import Counter
from utils import load_monthly_limits, save_monthly_limits
from db import (
    get_conn,
    migrate_json_to_sqlite,
    add_expense_to_db,
    get_all_expenses,
)
from charts import show_charts

# Старые имена файлов — оставлены только для обратной совместимости/экспорта
EXPENSES_FILE = "expenses.json"
BUDGET_LIMITS_FILE = "budget_limits.json"
DATABASE_FILE = "expenses.db"
LANG = "en"
# en / fr / es - можешь переключать


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
    print(messages["enter_category"])
    for i, cat in enumerate(categories):
        print(f"{i + 1}. {cat}")

    try:
        cat_choice = int(input("> ")) - 1
        if cat_choice not in range(len(categories)):
            print(messages["invalid_category"])
            return
        category = categories[cat_choice]
    except ValueError:
        print(messages["invalid_category"])
        return

    try:
        amount = float(input(messages["enter_amount"] + " "))
    except ValueError:
        print(messages["invalid_amount"])
        return

    description = input(messages["enter_description"] + " ")
    date_str = input(messages["enter_date"] + " ")
    try:
        date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        print(messages["invalid_date"])
        return

    # сохраняем сразу в SQLite
    add_expense_to_db(str(date), category, amount, description)
    print(messages["expense_added"])


def _t(key: str, default: str = "") -> str:
    """i18n: возьми ключ из messages[LANG], иначе из en, иначе default."""
    return messages.get(LANG, messages.get("en", {})).get(
        key, messages.get("en", {}).get(key, default)
    )


def _safe_call(fn_name: str, *args, **kwargs):
    """Вызывает функцию по имени, если она существует; иначе ничего не делает."""
    fn = globals().get(fn_name)
    if callable(fn):
        return fn(*args, **kwargs)
    return None


def _call_any(*fn_names):
    """Вызывает по очереди функции по именам; возвращает результат первой, что нашлась."""
    for name in fn_names:
        res = _safe_call(name)
        if res is not None:
            return res
    return None


def _coerce_date_iso(s: Any) -> str:
    if s is None:
        return ""
    txt = str(s).strip()
    if not txt:
        return ""
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d.%m.%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(txt, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return txt  # уже похоже на дату — вернём как есть


def _coerce_float(x: Any) -> float:
    if isinstance(x, (int, float)):
        return float(x)
    if x is None:
        return 0.0
    s = str(x).replace(",", ".").strip()
    try:
        return float(s)
    except ValueError:
        return 0.0


def _get_current_expenses():
    """Вернуть текущие расходы: сначала из БД (если есть), иначе из JSON/функций."""
    # 1) из БД -> список словарей
    try:
        import db

        df = db.get_expenses_df()
        if df is not None and hasattr(df, "empty") and not df.empty:
            df = df.copy()
            # приведение типов
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime(
                    "%Y-%m-%d"
                )
            if "amount" in df.columns:
                df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
            rows = []
            for _, r in df.iterrows():
                row = {
                    "date": str(r.get("date", "")),
                    "category": str(r.get("category", "")),
                    "amount": float(r.get("amount", 0.0)),
                }
                if "description" in df.columns and pd.notna(r.get("description")):
                    row["description"] = str(r.get("description"))
                rows.append(row)
            if rows:
                return rows
    except Exception:
        pass

    # 2) из функций/JSON
    res = _safe_call("get_expenses")
    if res is None:
        res = _safe_call("load_expenses")
    return res if isinstance(res, list) else []


def _save_and_exit():
    """Сохранить данные, используя доступные функции, и красиво выйти."""
    # 1) Если есть save_data() без аргументов — используем её
    if _safe_call("save_data") is None:
        # 2) Иначе пробуем классическую связку: load/get -> save_expenses(expenses)
        expenses = _get_current_expenses()
        _safe_call("save_expenses", expenses)

    print(_t("saving_data", "Saving data..."))
    print(_t("goodbye", "Goodbye!"))


def _ask(prompt_key: str, fallback: str) -> str:
    return input(_t(prompt_key, fallback)).strip()


def _generate_charts():
    import charts
    from pathlib import Path
    from datetime import datetime

    start = _ask("filter_start_date", "Enter start date (YYYY-MM-DD): ")
    end = _ask("filter_end_date", "Enter end date (YYYY-MM-DD): ")
    category = _ask("enter_category", "Enter category (optional): ")

    start = start or None
    end = end or None
    category = category or None

    out_dir = Path("reports/plots") / datetime.now().strftime("%Y-%m-%d")
    try:
        saved = charts.show_charts(
            out_dir=out_dir, start=start, end=end, category=category, lang=LANG
        )
    except ValueError as e:
        print("No data for plots with given filters.", str(e))
        retry = input("Try without filters? (y/N): ").strip().lower()
        if retry == "y":
            saved = charts.show_charts(out_dir=out_dir, lang=LANG)
        else:
            return

    print(_t("charts_saved_to", "Saved charts:"))
    for p in saved:
        print("  ", p)


def _render_menu():
    print()
    print(_t("menu_header", "=== Expense Analyzer Menu ==="))
    print(_t("menu_options", ""))


def _set_language():
    global LANG
    lang = input("Language (en/fr/es): ").strip().lower()
    if lang in ("en", "fr", "es"):
        LANG = lang
        print(f"Language set to: {LANG}")
    else:
        print("Unsupported language. Keeping current.")


# --- адаптеры под функции, которым нужны аргументы ---------------------------


def _load_json_if_exists(path: str):
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return None


def _get_budget_limits():
    """Вернём словарь лимитов из функции или из budget_limits.json."""
    # 1) если есть функция загрузки — используем её
    res = _safe_call("load_monthly_limits")
    if isinstance(res, dict):
        return res
    # 2) иначе пробуем файл
    data = _load_json_if_exists("budget_limits.json")
    return data if isinstance(data, dict) else {}


def _get_categories():
    """Список категорий: из функции, из БД, либо из expenses.json."""
    # 1) явная функция
    cats = _safe_call("get_categories")
    if isinstance(cats, (list, tuple)) and cats:
        return list(cats)

    # 2) попробуем БД, если есть модуль db
    try:
        import db  # noqa

        df = db.get_expenses_df()
        if "category" in df.columns and not df.empty:
            return sorted(set(map(str, df["category"].dropna().astype(str))))
    except Exception:
        pass

    # 3) fallback: по expenses.json
    data = _load_json_if_exists("expenses.json") or []
    if isinstance(data, list) and data:
        cats = {str(x.get("category", "")).strip() for x in data if x.get("category")}
        if cats:
            return sorted(cats)

    return []  # пусть функция add_expense сама спросит категорию вручную


def _msgs_lang():
    """Сообщения текущего языка (плоский словарь)."""
    return messages.get(LANG, messages.get("en", {}))


def _add_expense_adapter():
    """Вызов add_expense с нужными параметрами (локализованные messages)."""
    return _safe_call(
        "add_expense",
        messages=_msgs_lang(),  # ⬅️ передаём плоский словарь, а не весь messages
        lang=LANG,  # если функция использует lang — он есть
        budget_limits=_get_budget_limits(),
        categories=_get_categories(),
    )


def _call_fn_variants(fn, expenses, msgs, lang):
    """Пробуем популярные сигнатуры, возвращаем результат первой удачной."""
    # (ex, msgs, lang)
    try:
        return fn(expenses, msgs, lang)
    except TypeError:
        pass
    # (ex=..., messages=..., lang=...)
    try:
        return fn(expenses=expenses, messages=msgs, lang=lang)
    except TypeError:
        pass
    # (ex, msgs)
    try:
        return fn(expenses, msgs)
    except TypeError:
        pass
    # (ex=..., messages=...)
    try:
        return fn(expenses=expenses, messages=msgs)
    except TypeError:
        pass
    # () — как крайний случай
    try:
        return fn()
    except TypeError:
        return None


def _summarize_adapter():
    """Опция 2: агрегированный итог по категориям (через вашу функцию или fallback)."""
    expenses = _get_current_expenses()
    msgs = _msgs_lang()

    for name in ("summarize_expenses", "show_summary"):
        fn = globals().get(name)
        if callable(fn):
            res = _call_fn_variants(fn, expenses, msgs, LANG)
            if res is not None:
                return res

    # Fallback (если проектных функций нет) — печать простого summary:
    totals = {}
    for e in expenses:
        cat = str(e.get("category", "") or "uncategorized")
        amt = float(e.get("amount", 0) or 0)
        totals[cat] = totals.get(cat, 0.0) + amt

    print(_t("summary_header", "=== Expense Summary ==="))
    if not totals:
        print(_t("no_expenses", "No expenses recorded."))
        return

    grand = 0.0
    for cat in sorted(totals):
        val = totals[cat]
        print(
            _t("summary_line", "Category: {category}, Total: {total}").format(
                category=cat, total=f"{val:.2f}"
            )
        )
        grand += val

    print(_t("total_expenses", "Total expenses: {total}").format(total=f"{grand:.2f}"))


def _view_all_adapter():
    """Опция 6: показать все операции (через проектные функции или fallback)."""
    ex = _get_current_expenses()
    msgs = _msgs_lang()

    # Попробовать проектные функции
    for name in ("show_expenses", "view_all_expenses"):
        fn = globals().get(name)
        if callable(fn):
            res = _call_fn_variants(fn, ex, msgs, LANG)
            if res is not None:
                return

    # --- Fallback: печать всех операций (компактно, схлопывая дубли) ---
    print(_t("all_expenses_header", "=== All Expenses ==="))
    if not ex:
        print(_t("no_expenses", "No expenses recorded."))
        return

    from collections import Counter

    def _key(e):
        return (
            str(e.get("date", "")),
            str(e.get("category", "")),
            float(e.get("amount", 0) or 0),
            str(e.get("description") or e.get("note") or ""),
        )

    grouped = Counter(_key(e) for e in ex)

    # печатаем в хронологическом порядке
    for (date, cat, amt, desc), n in sorted(grouped.items(), key=lambda t: t[0]):
        line = f"{date.ljust(10)}  {cat.ljust(14)}  {amt:10.2f}"
        if desc:
            line += f" — {desc}"
        if n > 1:
            line += f"  ×{n}"
        print(line)


def main():
    while True:
        _render_menu()
        choice = input(_t("enter_option", "Enter option: ")).strip()

        if choice == "1":
            _add_expense_adapter()
        elif choice == "2":
            _summarize_adapter()
        elif choice == "3":
            _safe_call("filter_expenses_by_date")
        elif choice == "4":
            _safe_call("check_budget_limits")
        elif choice == "5":
            _safe_call("update_budget_limits")
        elif choice == "6":
            _view_all_adapter()
        elif choice == "7":  # Save & Exit
            _save_and_exit()
            break
        elif choice == "8":  # Generate charts
            _generate_charts()
        elif choice.lower() == "l":
            _set_language()
        else:
            print(_t("invalid_option", "Invalid option. Please enter a valid number."))


if __name__ == "__main__":
    main()
