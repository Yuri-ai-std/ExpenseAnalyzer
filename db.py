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
