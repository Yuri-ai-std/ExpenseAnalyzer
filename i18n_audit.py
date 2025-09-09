#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
i18n_audit.py — аудит i18n-использований и генерация шаблонов ключей.

Находит:
  • прямые обращения: messages["en"]["key"] / msgs["fr"]["key"]
  • хардкоды в st.success/error/warning/info/caption/toast("...")
  • старые ключи (ALIASES) по литералам в коде
  • «плохие» импорты messages/messages as msgs

Дополнительно:
  • генерирует предложения ключей для хардкодов и печатает
    готовые шаблоны для messages.py (EN/FR/ES).

Запуск:
  python i18n_audit.py [ROOT_DIR]
"""

from __future__ import annotations

import os
import re
import string
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Pattern, Tuple

# --- НАСТРОЙКИ ---

LANGS = ("en", "fr", "es")
OLD_KEYS = (
    "limit_updated",  # пример алиаса
    # добавляйте сюда старые ключи, которые хотите вычистить
)
EXTS = (".py", ".pyi")
EXCLUDE_DIRS = {
    ".git",
    ".hg",
    ".svn",
    "__pycache__",
    "venv",
    ".venv",
    "node_modules",
    "dist",
    "build",
}

# Попробуем импортировать словарь messages, чтобы проверять коллизии ключей
try:
    from messages import messages as I18N_MESSAGES  # noqa
except Exception:
    I18N_MESSAGES = {}  # не критично, просто не сможем проверить коллизии


# --- РЕГЭКСПЫ ---

RE_DIRECT_ACCESS: Pattern = re.compile(
    rf"""(?x)
    \b
    (?:messages|msgs)
    \s*\[
        \s*["'](?:{'|'.join(LANGS)})["']\s*
    \]\s*\[
        \s*["']([^"']+)["']\s*
    \]
    """
)

RE_ST_LITERAL: Pattern = re.compile(
    r"""\bst\.(success|error|warning|info|caption|toast)\s*\(\s*(['"])(.+?)\2\s*\)"""
)

RE_BAD_IMPORT: Pattern = re.compile(
    r"""^(?:from\s+messages\s+import\s+messages\b|import\s+messages\s+as\s+msgs\b|^import\s+messages\b)""",
    re.MULTILINE,
)

RE_OLD_KEYS: Pattern = (
    re.compile(r"\b(" + "|".join(map(re.escape, OLD_KEYS)) + r")\b")
    if OLD_KEYS
    else re.compile(r"^\Z")
)


@dataclass
class Hit:
    path: str
    lineno: int
    kind: str
    line: str
    extra: str = ""  # для direct: key; для st-literal: "func: text"


# --- УТИЛИТЫ ---


def should_skip_dir(name: str) -> bool:
    return name in EXCLUDE_DIRS or name.startswith(".")


def walk_files(root: str) -> List[str]:
    out: List[str] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if not should_skip_dir(d)]
        for fn in filenames:
            if fn.endswith(EXTS):
                out.append(os.path.join(dirpath, fn))
    return out


def scan_file(path: str) -> List[Hit]:
    hits: List[Hit] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except Exception:
        return hits

    content = "".join(lines)

    # 1) плохие импорты
    if RE_BAD_IMPORT.search(content):
        for i, line in enumerate(lines, 1):
            if RE_BAD_IMPORT.search(line):
                hits.append(Hit(path, i, "bad-import", line.rstrip()))

    # 2) прямой доступ messages["en"]["key"]
    for i, line in enumerate(lines, 1):
        for m in RE_DIRECT_ACCESS.finditer(line):
            key = m.group(1)
            hits.append(Hit(path, i, "direct", line.rstrip(), extra=key))

    # 3) хардкоды в st.success/error/...
    for i, line in enumerate(lines, 1):
        for m in RE_ST_LITERAL.finditer(line):
            func = m.group(1)  # success / error / ...
            text = m.group(3)  # буквальный текст
            hits.append(
                Hit(path, i, "st-literal", line.rstrip(), extra=f"{func}: {text}")
            )

    # 4) старые ключи
    if OLD_KEYS:
        for i, line in enumerate(lines, 1):
            if RE_OLD_KEYS.search(line):
                hits.append(Hit(path, i, "old-key", line.rstrip()))

    return hits


# --- ГЕНЕРАЦИЯ ШАБЛОНОВ ДЛЯ ХАРДКОДОВ ---


def slugify(text: str, max_words: int = 6) -> str:
    """
    Преобразовать текст в ключ-слуг:
    - нижний регистр, знаки препинания → пробел
    - берём первые max_words слов
    - соединяем _
    """
    # заменим пунктуацию на пробелы
    tbl = str.maketrans({c: " " for c in string.punctuation})
    canon = text.translate(tbl)
    words = [w for w in canon.lower().split() if w and w != "the"]
    words = words[:max_words] if max_words > 0 else words
    if not words:
        return "message"
    base = "_".join(words)
    # защитимся от слишком коротких/неинформативных ключей
    if len(base) < 4:
        base += "_msg"
    return base


def propose_key(func: str, text: str, existing: Dict[str, Dict[str, str]]) -> str:
    """
    Предложить ключ вида: <func>_<slug>
    Если коллизия — добавим суффикс _2, _3, ...
    """
    base = f"{func}_{slugify(text)}"
    if not existing:
        return base
    # проверим, нет ли такого ключа уже в en/fr/es
    all_keys = set()
    for lang, table in existing.items():
        if isinstance(table, dict):
            all_keys.update(table.keys())
    if base not in all_keys:
        return base
    # коллизия — наращиваем суффикс
    i = 2
    while f"{base}_{i}" in all_keys:
        i += 1
    return f"{base}_{i}"


def collect_literal_templates(
    hits: List[Hit],
) -> Dict[str, List[Tuple[str, str, str, int]]]:
    """
    Сгруппировать хардкоды по предложенным ключам.
    Возвращает: key -> list of (func, text, path, lineno)
    """
    buckets: Dict[str, List[Tuple[str, str, str, int]]] = defaultdict(list)
    for h in hits:
        if h.kind != "st-literal":
            continue
        try:
            func, text = h.extra.split(":", 1)
            func = func.strip()
            text = text.strip()
        except Exception:
            continue
        key = propose_key(
            func, text, I18N_MESSAGES if isinstance(I18N_MESSAGES, dict) else {}
        )
        buckets[key].append((func, text, h.path, h.lineno))
    return buckets


def print_message_templates(
    templates: Dict[str, List[Tuple[str, str, str, int]]],
) -> None:
    if not templates:
        return
    print("== Generated templates for messages.py (copy & adapt) ==")
    for key, items in templates.items():
        # берём первую фразу как «эталон»
        func, text, _, _ = items[0]
        print(f"\n# Suggested key: {key}")
        print("# Occurrences:")
        for func_i, text_i, path, lineno in items[:5]:
            print(f"#  - {path}:{lineno} -> st.{func_i}({text_i!r})")
        # Шаблон для messages.py
        print("messages['en'][%r] = %r" % (key, text))
        print("messages['fr'][%r] = ''  # TODO" % key)
        print("messages['es'][%r] = ''  # TODO" % key)


# --- MAIN ---


def main() -> int:
    root = sys.argv[1] if len(sys.argv) > 1 else "."
    files = walk_files(root)
    all_hits: List[Hit] = []
    for p in files:
        all_hits.extend(scan_file(p))

    all_hits.sort(key=lambda h: (h.path, h.lineno, h.kind))
    cats = {"bad-import": [], "direct": [], "st-literal": [], "old-key": []}
    for h in all_hits:
        cats[h.kind].append(h)

    print(f"Scanned {len(files)} files under {os.path.abspath(root)}\n")

    if cats["bad-import"]:
        print("== Bad imports (use `from messages import t` instead):")
        for h in cats["bad-import"]:
            print(f"  {h.path}:{h.lineno}: {h.line}")
        print()

    if cats["direct"]:
        print("== Direct dictionary access (replace with t('key', lang)):")
        for h in cats["direct"]:
            print(f"  {h.path}:{h.lineno}: {h.line}")
            print(f"      -> suggestion: t('{h.extra}', lang)")
        print()

    if cats["st-literal"]:
        print("== Hardcoded literals in st.* (consider moving to messages + t())")
        for h in cats["st-literal"]:
            print(f"  {h.path}:{h.lineno}: {h.line}")
        print()
        # Сгенерируем шаблоны ключей для хардкодов
        templates = collect_literal_templates(cats["st-literal"])
        print_message_templates(templates)
        print()

    if cats["old-key"]:
        print(
            "== Old keys (aliases) usages (replace with t(old_key, lang) then migrate to target):"
        )
        for h in cats["old-key"]:
            print(f"  {h.path}:{h.lineno}: {h.line}")
        print()

    total = sum(len(v) for v in cats.values())
    print("Summary:")
    print(f"  Bad imports : {len(cats['bad-import'])}")
    print(f"  Direct dict : {len(cats['direct'])}")
    print(f"  ST literals : {len(cats['st-literal'])}")
    print(f"  Old keys    : {len(cats['old-key'])}")
    print(f"\nTotal findings: {total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
