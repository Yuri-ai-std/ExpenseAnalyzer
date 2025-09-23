#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Валидатор словарей локализаций:
- Синхронизация ключей относительно базового языка
- Пустые строки
- Дубликаты строк внутри каждого языка
- Проверка алиасов (существование цели, отсутствие циклов)
"""

import sys
from collections import defaultdict
from typing import Dict, List, Tuple

# импортируем из messages.py
from messages import ALIASES, messages  # messages — в нижнем регистре

BASE_LANG = "en"
LANGS = sorted(messages.keys())


def resolve_alias_chain(
    start: str, aliases: Dict[str, str]
) -> Tuple[str, List[str], bool]:
    """
    Возвращает (целевой_ключ, цепочка, есть_цикл)
    """
    seen = []
    cur = start
    while cur in aliases:
        if cur in seen:
            return (cur, seen + [cur], True)
        seen.append(cur)
        cur = aliases[cur]
    return (cur, seen, False)


def check_aliases(msgs: Dict[str, Dict[str, str]], aliases: Dict[str, str]) -> int:
    errors = 0
    if not aliases:
        return 0

    print("\n-- Aliases --")
    for src, dst in aliases.items():
        print(f"  {src} → {dst}")

    # цикл и существование цели в базовом языке
    for src, _ in aliases.items():
        target, chain, is_cycle = resolve_alias_chain(src, aliases)
        if is_cycle:
            print(f"ERROR: alias cycle detected: {' -> '.join(chain)}")
            errors += 1
            continue

        if target not in msgs.get(BASE_LANG, {}):
            print(
                f"ERROR: alias '{src}' resolves to '{target}', which is missing in base '{BASE_LANG}'"
            )
            errors += 1

    return errors


def check_sync_and_empties(msgs: Dict[str, Dict[str, str]]) -> int:
    errors = 0
    base = msgs.get(BASE_LANG, {})
    base_keys = set(base.keys())

    print("\n-- Sync against base language --")
    for lang in LANGS:
        if lang == BASE_LANG:
            continue
        cur = msgs.get(lang, {})
        cur_keys = set(cur.keys())

        missing = sorted(base_keys - cur_keys)
        extra = sorted(cur_keys - base_keys)

        if missing:
            errors += len(missing)
            print(f"ERROR: [{lang}] missing keys: {missing}")
        if extra:
            # это не критично, но сообщим — обычно ключи должны совпадать
            print(f"WARNING: [{lang}] extra keys (not in {BASE_LANG}): {extra}")

    print("\n-- Empty strings --")
    for lang in LANGS:
        empty_keys = [
            k
            for k, v in msgs.get(lang, {}).items()
            if isinstance(v, str) and v.strip() == ""
        ]
        if empty_keys:
            errors += len(empty_keys)
            print(f"ERROR: [{lang}] empty translations for keys: {empty_keys}")

    return errors


def check_duplicates(msgs: Dict[str, Dict[str, str]]) -> int:
    warnings = 0
    print("\n-- Duplicate strings inside each language --")
    for lang in LANGS:
        values = msgs.get(lang, {})
        rev = defaultdict(list)
        for k, v in values.items():
            if isinstance(v, str):
                rev[v.strip()].append(k)

        dups = {txt: keys for txt, keys in rev.items() if txt and len(keys) > 1}
        if dups:
            warnings += sum(len(keys) - 1 for keys in dups.values())
            print(f"WARNING: duplicate translations in '{lang}':")
            for txt, keys in dups.items():
                print(f"  '{txt}' used by keys: {keys}")
    return warnings


def main() -> None:
    print("=== Checking translation dictionaries ===")
    print(f"Base language: {BASE_LANG}")

    total_errors = 0
    total_warnings = 0

    # 1) алиасы
    total_errors += check_aliases(messages, ALIASES)

    # 2) синхронизация и пустые строки
    total_errors += check_sync_and_empties(messages)

    # 3) дубликаты текстов внутри языка
    total_warnings += check_duplicates(messages)

    print("\n=== Check finished ===")
    print(f"Errors: {total_errors} | Warnings: {total_warnings}")
    sys.exit(1 if total_errors > 0 else 0)


if __name__ == "__main__":
    main()
