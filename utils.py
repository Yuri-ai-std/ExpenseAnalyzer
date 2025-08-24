import json
import os
from typing import Optional, Dict, Any
from datetime import date

BUDGET_LIMITS_FILE = "budget_limits.json"


def month_key(d: date) -> str:
    """
    Возвращает ключ месяца в формате 'YYYY-MM' для переданной даты.
    Пример:
        >>> month_key(date(2025, 8, 23))
        '2025-08'
    """
    return f"{d.year:04d}-{d.month:02d}"


def load_monthly_limits(
    filename: Optional[str] = None,
    file_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Поддерживаем и filename, и file_path (совместимость с тестами)."""
    path = filename or file_path or BUDGET_LIMITS_FILE
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_monthly_limits(
    budget_limits: Dict[str, Any],
    filename: Optional[str] = None,
    file_path: Optional[str] = None,
) -> None:
    """Поддерживаем и filename, и file_path (совместимость с тестами)."""
    path = filename or file_path or BUDGET_LIMITS_FILE
    with open(path, "w", encoding="utf-8") as f:
        json.dump(budget_limits, f, indent=2, ensure_ascii=False)
