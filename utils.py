import json
import os
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

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


def db_path_for(user: str) -> str:
    user = (user or "default").strip() or "default"
    return str(DATA_DIR / f"{user}_expenses.db")
