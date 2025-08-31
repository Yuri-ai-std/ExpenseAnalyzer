import json
import re
import shutil
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
ARCHIVE_DIR = DATA_DIR / "archive"
ARCHIVE_DIR.mkdir(exist_ok=True)

BUDGET_LIMITS_FILE = "data/budget_limits.json"


def limits_path_for(user: str) -> Path:
    """Путь к файлу лимитов для конкретного пользователя."""
    # например: data/alex_budget_limits.json
    safe_user = (user or "default").strip() or "default"
    return DATA_DIR / f"{safe_user}_budget_limits.json"


def load_monthly_limits(
    filename: Optional[str | Path] = None,
    file_path: Optional[str | Path] = None,
    user: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Загружаем лимиты из JSON.
    Порядок выбора файла:
    1) filename (если передан)
    2) file_path (если передан)
    3) limits_path_for(user) (если передан user)
    4) BUDGET_LIMITS_FILE (дефолт/совместимость)
    """
    if filename is not None:
        path = Path(filename)
    elif file_path is not None:
        path = Path(file_path)
    elif user is not None:
        path = limits_path_for(user)
    else:
        path = Path(BUDGET_LIMITS_FILE)

    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_monthly_limits(
    budget_limits: Dict[str, Any],
    filename: Optional[str | Path] = None,
    file_path: Optional[str | Path] = None,
    user: Optional[str] = None,
) -> None:
    """
    Сохраняем лимиты в JSON.
    Порядок выбора файла — тот же, что и в load_monthly_limits.
    """
    if filename is not None:
        path = Path(filename)
    elif file_path is not None:
        path = Path(file_path)
    elif user is not None:
        path = limits_path_for(user)
    else:
        path = Path(BUDGET_LIMITS_FILE)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(budget_limits, f, indent=2, ensure_ascii=False)


def month_key(d: date) -> str:
    """
    Возвращает ключ месяца в формате 'YYYY-MM' для переданной даты.
    Пример:
        >>> month_key(date(2025, 8, 23))
        '2025-08'
    """
    return f"{d.year:04d}-{d.month:02d}"


def prev_month_key(mk: str) -> str:
    y, m = map(int, mk.split("-"))
    if m == 1:
        return f"{y-1}-12"
    return f"{y:04d}-{m-1:02d}"


def mean3(series: List[float]) -> float:
    s = [float(x) for x in series if x is not None]
    return float(pd.Series(s).tail(3).mean()) if s else 0.0


def db_path_for(user: str) -> str:
    user = (user or "default").strip() or "default"
    return str(DATA_DIR / f"{user}_expenses.db")


def slugify_user(name: str) -> str:
    """нормализует имя профиля (буквы/цифры/ - _ ), не пустое"""
    s = re.sub(r"[^a-zA-Z0-9_-]+", "-", (name or "").strip()).strip("-_")
    return s or "user"


def user_files(user: str) -> tuple[Path, Path]:
    """пути к файлам профиля (db, limits.json)"""
    user = slugify_user(user)
    return (DATA_DIR / f"{user}_expenses.db", DATA_DIR / f"{user}_budget_limits.json")


def list_users() -> list[str]:
    """поиск профилей по маскам *_expenses.db / *_budget_limits.json"""
    users: set[str] = set()
    for p in DATA_DIR.glob("*_expenses.db"):
        users.add(p.name.replace("_expenses.db", ""))
    for p in DATA_DIR.glob("*_budget_limits.json"):
        users.add(p.name.replace("_budget_limits.json", ""))
    if not users:
        users.add("default")
    return sorted(users)


def _init_db_if_needed(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS expenses(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                category TEXT NOT NULL,
                amount REAL NOT NULL,
                description TEXT
            );
        """
        )
        conn.commit()


def create_user(user: str) -> str:
    """создаёт профиль (если нет): пустая БД + limits.json (из шаблона/пустой)"""
    user = slugify_user(user)
    db_path, limits_path = user_files(user)
    if not db_path.exists():
        _init_db_if_needed(db_path)
    if not limits_path.exists():
        tmpl = DATA_DIR / "default_budget_limits.json"
        if tmpl.exists():
            shutil.copyfile(tmpl, limits_path)
        else:
            limits_path.write_text("{}", encoding="utf-8")
    return user


def archive_user(user: str) -> Path:
    """перемещает файлы профиля в архив data/archive/<ts>/<user>/"""
    user = slugify_user(user)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    target = ARCHIVE_DIR / ts / user
    target.mkdir(parents=True, exist_ok=True)
    db_path, limits_path = user_files(user)
    if db_path.exists():
        shutil.move(str(db_path), str(target / db_path.name))
    if limits_path.exists():
        shutil.move(str(limits_path), str(target / limits_path.name))
    return target


def delete_user(user: str, archive: bool = True) -> None:
    """удаляет профиль; опционально предварительно архивирует"""
    user = slugify_user(user)
    if archive:
        archive_user(user)
    else:
        for p in user_files(user):
            if p.exists():
                p.unlink()
