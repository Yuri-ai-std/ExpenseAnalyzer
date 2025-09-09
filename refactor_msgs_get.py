#!/usr/bin/env python3
import json
import pathlib
import re

p = pathlib.Path("app.py")
src = p.read_text(encoding="utf-8")

# 1) msgs.get("key", "Default") -> t("key", lang, default="Default")
pat_with_def = re.compile(
    r"""\bmsgs\.get\(
        \s*(['"])(?P<key>[A-Za-z0-9_]+)\1      # ключ
        \s*,\s*
        (['"])(?P<def>.*?)\3                   # дефолт (строка)
        \s*\)""",
    re.VERBOSE | re.DOTALL,
)


def repl_with_def(m):
    key = m.group("key")
    default_str = m.group("def")
    # безопасно квотируем дефолт (на случай кавычек/эскейпов)
    return f't("{key}", lang, default={json.dumps(default_str)})'


src = pat_with_def.sub(repl_with_def, src)

# 2) msgs.get("key") -> t("key", lang)
pat_no_def = re.compile(
    r"""\bmsgs\.get\(\s*(['"])(?P<key>[A-Za-z0-9_]+)\1\s*\)""", re.VERBOSE
)
src = pat_no_def.sub(lambda m: f't("{m.group("key")}", lang)', src)

p.write_text(src, encoding="utf-8")
print("✅ app.py: msgs.get(...) → t(..., lang) завершено.")
