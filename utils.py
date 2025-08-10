import json
import os
from typing import Optional, Dict, Any

BUDGET_LIMITS_FILE = "budget_limits.json"


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
