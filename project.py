# project.py — Expense Analyzer with multi-language support + SQLite
import csv
import json
import os
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

import db  # <-- ДОБАВИЛИ: чтобы можно было писать db.get_expenses_df и т.п.
from charts import show_charts

# ПУБЛИЧНАЯ запись в БД — отдельным алиасом
# всё про БД – только из db.py (функции)
from db import DB_PATH  # если реально используете константу
from db import ensure_schema  # инициализация схемы при старте
from db import get_expenses_df  # универсальная выборка как DataFrame
from db import list_categories  # список категорий (из БД/предопределённый)
from db import add_expense as db_add_expense
from messages import messages
from utils import load_monthly_limits, save_monthly_limits

REPORTS_DIR = Path("reports/plots")

# Старые имена файлов — оставлены только для обратной совместимости/экспорта
EXPENSES_FILE = "expenses.json"
BUDGET_LIMITS_FILE = "budget_limits.json"
DATABASE_FILE = "expenses.db"
LANG = "en"
# en / fr / es - можешь переключать


def calculate_total_expenses(expenses):
    return sum(float(e.get("amount", 0)) for e in expenses)


# --------------------------- утилиты вывода ---------------------------


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
        SELECT date, category, amount, description
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


def check_budget_limits(
    conn,
    *,
    messages: Optional[Dict[str, str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    budget_limits: Optional[Dict[str, Dict[str, float]]] = None,
) -> List[str]:
    """
    Суммирует расходы по категориям в интервале [start_date; end_date]
    и сравнивает их с лимитами вида {"YYYY-MM": {"food": 70, ...}}.
    Возвращает список строк-предупреждений (или пустой список).
    """
    messages = messages or {}
    budget_limits = budget_limits or {}

    # --- собираем WHERE без None в параметрах ---
    where_parts: List[str] = ["WHERE 1=1"]
    params: List[str] = []

    if start_date:
        where_parts.append("AND date >= ?")
        params.append(start_date)
    if end_date:
        where_parts.append("AND date <= ?")
        params.append(end_date)

    query = f"""
        SELECT date, category, amount
        FROM expenses
        {' '.join(where_parts)}
        ORDER BY date
    """

    df = pd.read_sql_query(query, conn, params=tuple(params))

    # нет данных в интервале — возвращаем пустой список (тип строго List[str])
    if df.empty:
        return []

    # --- агрегируем по месяцам и категориям ---
    df["month"] = df["date"].str.slice(0, 7)
    totals = (
        df.groupby(["month", "category"])["amount"]
        .sum()
        .reset_index()  # колонки: month, category, amount
    )

    out: List[str] = []

    for _, row in totals.iterrows():
        month: str = row["month"]
        cat: str = row["category"]
        total = float(row["amount"])

        # безопасно достаём лимит
        limit: Optional[float] = None
        month_limits = budget_limits.get(month) or {}
        if isinstance(month_limits, dict):
            raw = month_limits.get(cat)
            if raw is not None:
                try:
                    limit = float(raw)
                except (TypeError, ValueError):
                    limit = None

        line = f"{month} {cat}: ${total:.2f}"
        if limit is not None:
            status = (
                messages.get("over_limit", "Over!")
                if total > limit
                else messages.get("within_limit", "Within")
            )
            line += f" [{status}] (Limit: ${limit:.2f})"

        out.append(line)

    return out


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


def add_expense(
    date: str,
    category: str,
    amount: float,
    description: Optional[str] = None,
    db_path: str = DB_PATH,
) -> None:
    """
    Высокоуровневая обёртка без интерактива — просто прокидывает в БД.
    Удобно использовать в тестах.
    """
    db.add_expense(date=date, category=category, amount=amount, description=description)


def _t(key: str, default: str = "") -> str:
    """i18n: возьми ключ из messages[LANG], иначе из en, иначе default."""
    return messages.get(LANG, messages.get("en", {})).get(
        key, messages.get("en", {}).get(key, default)
    )


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


def _get_current_expenses(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category: Optional[str] = None,
) -> List[Dict]:
    """
    Возвращаем текущие расходы ТОЛЬКО из SQLite:
    берём df через get_expenses_df и переводим в список словарей.
    """
    df = get_expenses_df(start_date=start_date, end_date=end_date, category=category)
    if df is None or df.empty:
        return []
    # гарантируем одинаковые ключи
    out: List[Dict] = []
    for r in df.to_dict(orient="records"):
        out.append(
            {
                "date": str(r.get("date", "")),
                "category": str(r.get("category", "")),
                "amount": float(r.get("amount") or 0.0),
                "description": (r.get("description") or r.get("note") or None),
            }
        )
    return out


def _save_and_exit():
    """Сохранить данные и выйти."""
    # так как теперь у нас SQLite, сохранять вручную не нужно —
    # база сама фиксирует изменения при commit()
    print(_t("saving_data", "Saving data..."))
    print(_t("goodbye", "Goodbye!"))
    exit(0)


def _ask(prompt_key: str, fallback: str) -> str:
    return input(_t(prompt_key, fallback)).strip()


def _generate_charts():
    from datetime import datetime
    from pathlib import Path

    import charts

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


def list_categories() -> list[str]:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT category FROM expenses ORDER BY category")
        return [str(r[0]) for r in cur.fetchall()]


def _df_records(df):
    # Всегда переводим DataFrame в list[dict]
    try:
        return df.to_dict(orient="records")
    except Exception:
        return []


def _total_from_any(obj) -> float:
    # Принимает либо DataFrame, либо list[dict]
    if hasattr(obj, "to_dict"):
        obj = _df_records(obj)
    return sum(float(e.get("amount", 0) or 0) for e in obj or [])


# --- адаптеры под функции, которым нужны аргументы ---------------------------


def _load_json_if_exists(path: str):
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return None


def get_budget_limits() -> dict:
    # 1) если есть явная функция 'load_monthly_limits' — попробуем её
    fn = globals().get("load_monthly_limits")
    if callable(fn):
        try:
            res = fn()
            if isinstance(res, dict):
                return res
        except Exception:
            pass  # тихо падаем к JSON

    # 2) иначе читаем JSON
    data = _load_json_if_exists("budget_limits.json")
    return data if isinstance(data, dict) else {}


def categories() -> list[str]:
    # можно читать из budget_limits.json, а если файла нет — дефолты
    import json
    import os

    try:
        if os.path.exists("budget_limits.json"):
            with open("budget_limits.json", "r", encoding="utf-8") as f:
                data = json.load(f)
            keys = list(data.keys())
            if keys:
                return keys
    except Exception:
        pass
    return ["food", "transport", "entertainment", "utilities", "groceries", "other"]


def _msgs_lang():
    """Сообщения текущего языка (плоский словарь)."""
    return messages.get(LANG, messages.get("en", {}))


# --- Add Expense (DB only) ---
def add_expense_adapter() -> None:
    """
    Диалог с пользователем + минимальная валидация,
    потом запись в БД через db.add_expense().
    """
    # 1) выбор категории (пример — подставьте ваш источник)
    categories = list_categories() if "list_categories" in globals() else []
    if not categories:
        print("No categories defined.")
        return

    for i, c in enumerate(categories, 1):
        print(f"{i}. {c}")
    try:
        idx = int(input("> ").strip()) - 1
        if not (0 <= idx < len(categories)):
            print("Invalid category!")
            return
        category = categories[idx]
    except Exception:
        print("Invalid category!")
        return

    # 2) дата
    date_str = input("Enter date (YYYY-MM-DD): ").strip()
    try:
        # валидация формата (а хранить будем строкой)
        datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        print("Invalid date! Use YYYY-MM-DD.")
        return

    # 3) сумма
    try:
        amount = float(input("Enter amount: ").strip())
    except Exception:
        print("Invalid amount! Please enter a number.")
        return

    # 4) описание (опционально)
    desc_input = input("Enter description (optional): ").strip()
    description: Optional[str] = desc_input or None

    # 5) запись в БД
    db.add_expense(
        date=date_str,
        category=category,
        amount=amount,
        description=description,
    )
    print("Expense added successfully!")


def _call_fn_variants(fn, expenses=None, msgs=None, lang=None):
    """Пробуем разные варианты сигнатур и возвращаем результат первой удачной попытки."""
    attempts = (
        # 1) позиционные (ex, msgs, lang)
        lambda: fn(expenses, msgs, lang),
        # 2) именованные (expenses=..., messages=..., lang=...)
        lambda: fn(expenses=expenses, messages=msgs, lang=lang),
        # 3) без lang
        lambda: fn(expenses, msgs),
        lambda: fn(expenses=expenses, messages=msgs),
        # 4) только (messages, lang)
        lambda: fn(msgs, lang),
        lambda: fn(messages=msgs, lang=lang),
        # 5) только expenses
        lambda: fn(expenses),
        # 6) крайний случай — без аргументов
        lambda: fn(),
    )
    for call in attempts:
        try:
            return call()
        except TypeError:
            continue
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
    """Опция 6: показать все операции (SQLite-only, с дедупликацией ×N)."""
    print(_t("all_expenses_header", "=== All Expenses ==="))
    ex = _get_current_expenses()
    if not ex:
        print(_t("no_expenses", "No expenses recorded."))
        return

    # сгруппируем одинаковые записи (date, category, amount, desc)
    def key(e):
        return (
            str(e.get("date", "")),
            str(e.get("category", "")),
            float(e.get("amount") or 0.0),
            (e.get("description") or ""),
        )

    grouped = Counter(key(e) for e in ex)

    # печать в хронологическом порядке
    for (date, cat, amt, desc), n in sorted(grouped.items(), key=lambda t: t[0]):
        line = f"{date.ljust(10)}  {cat.ljust(14)}  {amt:10.2f}"
        if desc:
            line += f"  — {desc}"
        if n > 1:
            line += f"  ×{n}"
        print(line)

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
    ensure_schema()

    while True:
        _render_menu()
        choice = input(_t("enter_option", "Enter option: ")).strip()

        if choice == "1":
            add_expense_adapter()
        elif choice == "2":
            _summarize_adapter()
        elif choice == "6":
            _view_all_adapter()
        elif choice == "8":
            # Генерация графиков — как у тебя было
            show_charts(REPORTS_DIR, lang=LANG)
        elif choice in ("7", "q", "Q", "exit"):
            print(_t("goodbye", "Goodbye!"))
            break
        else:
            print(_t("invalid_option", "Invalid option! Please enter a number."))


if __name__ == "__main__":
    main()
