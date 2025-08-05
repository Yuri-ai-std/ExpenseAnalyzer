import sqlite3

def initialize_db():
    conn = sqlite3.connect("expense_analyzer.db")
    c = conn.cursor()

    # Создание таблицы расходов
    c.execute('''
        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            category TEXT NOT NULL,
            amount REAL NOT NULL,
            note TEXT
        )
    ''')

    # Создание таблицы лимитов
    c.execute('''
        CREATE TABLE IF NOT EXISTS budget_limits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month TEXT NOT NULL,
            category TEXT NOT NULL,
            limit REAL NOT NULL
        )
    ''')

    conn.commit()
    conn.close()

def add_expense_to_db(date, category, amount, note):
    conn = sqlite3.connect("expense_analyzer.db")
    c = conn.cursor()
    c.execute("INSERT INTO expenses (date, category, amount, note) VALUES (?, ?, ?, ?)",
              (date, category, amount, note))
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
