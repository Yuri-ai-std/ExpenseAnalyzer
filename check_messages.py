# check_messages.py — валидатор словарей локализаций

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from typing import Dict, List, Set

REPO_ROOT = Path(__file__).resolve().parent


# ---------- загрузка messages.py ----------
def load_messages() -> Dict[str, Dict[str, str]]:
    """Находит и подгружает модуль messages.py, возвращает верхний словарь локализаций."""
    msg_path = REPO_ROOT / "messages.py"
    if not msg_path.exists():
        raise SystemExit(f"Не найден messages.py рядом со скриптом: {msg_path}")

    spec = importlib.util.spec_from_file_location("messages", str(msg_path))
    if spec is None or spec.loader is None:
        raise ImportError("Не удалось сформировать spec для messages.py")

    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]

    # ищем на уровне модуля первый dict со структурой {lang: {key: text}}
    for v in vars(mod).values():
        if (
            isinstance(v, dict)
            and v
            and all(isinstance(k, str) and isinstance(d, dict) for k, d in v.items())
        ):
            return v  # type: ignore[return-value]

    raise SystemExit("В messages.py не найден словарь вида {lang: {key: text}}")


# ---------- проверки ----------
def all_keys(messages: Dict[str, Dict[str, str]]) -> Set[str]:
    keys: Set[str] = set()
    for table in messages.values():
        keys |= set(table.keys())
    return keys


_key_re = re.compile(r"\{([A-Za-z0-9_]+)\}")


def placeholders(s: str) -> Set[str]:
    """Множество плейсхолдеров в строке: {name}."""
    return set(_key_re.findall(s))


def check_messages(messages: Dict[str, Dict[str, str]]) -> List[str]:
    problems: List[str] = []

    # 1) базовая структура
    for lang, table in messages.items():
        if not isinstance(table, dict):
            problems.append(f"{lang}: значение не dict")

    # 2) полнота ключей
    union = all_keys(messages)
    for lang, table in messages.items():
        miss = union - set(table.keys())
        if miss:
            problems.append(f"{lang}: отсутствуют ключи {sorted(miss)}")

    # 3)一致ность плейсхолдеров
    for key in sorted(union):
        ph_sets = {
            lang: placeholders(table[key])
            for lang, table in messages.items()
            if key in table
        }
        if len({frozenset(v) for v in ph_sets.values()}) > 1:
            details = ", ".join(f"{lang}:{sorted(v)}" for lang, v in ph_sets.items())
            problems.append(f"Плейсхолдеры различаются для '{key}': {details}")

    return problems


if __name__ == "__main__":
    msgs = load_messages()
    langs = ", ".join(sorted(msgs.keys()))
    print(f"Найдены языки: {langs}")

    issues = check_messages(msgs)
    if issues:
        print("\nНайдены проблемы:")
        for line in issues:
            print(" -", line)
        raise SystemExit(1)

    print("OK: все языки согласованы и плейсхолдеры совпадают.")
