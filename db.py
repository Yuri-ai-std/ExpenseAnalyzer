import os
import sqlite3
import json

DB_PATH = "expenses.db"
EXP_JSON = "expenses.json"
LIM_JSON = "budget_limits.json"


def migrate_json_to_sqlite(
    expenses_json: str = EXP_JSON,
    limits_json: str = LIM_JSON,
    db_path: str = DB_PATH,
) -> bool:
    """
    Переносит данные из JSON в SQLite, если таблица expenses ещё пустая.
    Возвращает True, если миграция выполнялась; False — если в БД уже были данные.
    """
    conn = get_conn(db_path)
    init_db(conn)

    cur = conn.execute("SELECT COUNT(*) FROM expenses")
    if (cur.fetchone() or [0])[0] > 0:
        conn.close()
        return False  # уже мигрировано/данные есть

    # 1) Загружаем расходы
    if os.path.exists(expenses_json):
        try:
            with open(expenses_json, "r", encoding="utf-8") as f:
                expenses = json.load(f)
        except json.JSONDecodeError:
            expenses = []
        for e in expenses or []:
            conn.execute(
                "INSERT INTO expenses(date, category, amount, note) VALUES (?, ?, ?, ?)",
                (
                    e.get("date"),
                    e.get("category"),
                    float(e.get("amount", 0) or 0),
                    e.get("note") or e.get("description", "") or "",
                ),
            )

    # 2) Загружаем лимиты
    if os.path.exists(limits_json):
        try:
            with open(limits_json, "r", encoding="utf-8") as f:
                limits = json.load(f)
        except json.JSONDecodeError:
            limits = {}
        for month, cats in (limits or {}).items():
            for cat, lim in (cats or {}).items():
                conn.execute(
                    "INSERT OR IGNORE INTO limits(month, category, limit) VALUES (?, ?, ?)",
                    (month, cat, float(lim or 0)),
                )

    conn.commit()
    conn.close()
    return True


def get_conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    # Основные таблицы
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,                 -- YYYY-MM-DD
            category TEXT NOT NULL,
            amount REAL NOT NULL CHECK(amount >= 0),
            note TEXT DEFAULT ''
        );
    """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS limits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month TEXT NOT NULL,                -- YYYY-MM
            category TEXT NOT NULL,
            limit REAL NOT NULL CHECK(limit >= 0),
            UNIQUE(month, category)
        );
    """
    )
    # Индексы
    conn.execute("CREATE INDEX IF NOT EXISTS idx_expenses_date ON expenses(date);")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_expenses_category ON expenses(category);"
    )
    conn.commit()


def initialize_db():
    conn = sqlite3.connect("expense_analyzer.db")
    c = conn.cursor()

    # Создание таблицы расходов
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            category TEXT NOT NULL,
            amount REAL NOT NULL,
            note TEXT
        )
    """
    )

    # Создание таблицы лимитов
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS budget_limits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month TEXT NOT NULL,
            category TEXT NOT NULL,
            limit REAL NOT NULL
        )
    """
    )

    conn.commit()
    conn.close()


def add_expense_to_db(date, category, amount, note):
    conn = sqlite3.connect("expense_analyzer.db")
    c = conn.cursor()
    c.execute(
        "INSERT INTO expenses (date, category, amount, note) VALUES (?, ?, ?, ?)",
        (date, category, amount, note),
    )
    conn.commit()
    conn.close()


def get_all_expenses():
    conn = sqlite3.connect("expense_analyzer.db")
    c = conn.cursor()
    c.execute("SELECT date, category, amount, note FROM expenses")
    results = c.fetchall()
    conn.close()
    return results


def get_monthly_limits():
    conn = sqlite3.connect("expense_analyzer.db")
    c = conn.cursor()
    c.execute("SELECT month, category, limit FROM budget_limits")
    results = c.fetchall()
    conn.close()
    return results
