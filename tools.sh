#!/usr/bin/env bash
# tools.sh — вспомогательный скрипт для разработки ExpenseAnalyzer
# Все вызовы идут через python3, чтобы исключить путаницу с интерпретатором.

set -Eeuo pipefail

# ---------- утилиты вывода ----------
ok()   { printf "\033[32m✓ %s\033[0m\n" "$*"; }
err()  { printf "\033[31m✗ %s\033[0m\n" "$*" >&2; exit 1; }
info() { printf "\n\033[36m== %s ==\033[0m\n" "$*"; }

# ---------- 1) тесты (pytest через python3) ----------
run_tests() {
  info "Запуск тестов"

  # Всегда используем активный интерпретатор из venv
  python3 - <<'PY' || exit 1
import sys, subprocess

def ensure(module):
    try:
        __import__(module)
        return True
    except ModuleNotFoundError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-U", module])
        __import__(module)
        return True

# Гарантируем установку pytest и запуск через API
ensure("pytest")
import pytest

# -q: тихий вывод; можно заменить на [] для подробного
code = pytest.main(["-q"])
sys.exit(code)
PY

  if [ $? -eq 0 ]; then
    ok "Тесты пройдены"
  else
    bad "Тесты не пройдены"
  fi
}

# ---------- 2) быстрый экспорт CSV (smoke для export) ----------
check_export() {
  info "Проверка экспорта CSV (smoke)"

  # делаем пробный экспорт за 2024 год без категории
  python3 project.py <<'EOF'
en
6
test_export.csv
2024-01-01
2024-12-31

4
EOF

  [[ -f test_export.csv ]] || err "Файл test_export.csv не создан"
  head -n 1 test_export.csv | grep -q '^date,category,amount,note' || err "Неверный заголовок CSV"
  rm -f test_export.csv
  ok "Экспорт CSV работает"
}

# ---------- 3) smoke основного меню ----------
smoke_menu() {
  info "Smoke тест основного меню"

  # просто войти и выйти (en → Exit)
  python3 project.py <<'EOF'
en
4
EOF

  ok "Smoke OK"
}

# ---------- 4) проверки messages.py ----------
check_messages() {
  info "Проверка messages.py (синтаксис)"
  python3 -m py_compile messages.py || err "Синтаксическая ошибка в messages.py"
  ok "messages.py компилируется"

  info "Поиск дубликатов ключей в messages.py (только по тексту файла)"
  # ищем повторяющиеся строковые ключи "ключ":
  dups="$(grep -Po '^\s*"\K[^"]+(?=":\s*)' messages.py | sort | uniq -d || true)"
  if [[ -n "${dups}" ]]; then
    echo "Найдены дубликаты:"
    echo "${dups}"
    exit 1
  fi
  ok "Дубликатов ключей не найдено"
}

# ---------- 5) режимы запуска ----------
usage() {
  cat <<EOF
Usage: $0 [tests|export|smoke|messages|all]

  tests    – установить pytest (если нужно) и запустить pytest
  export   – smoke-проверка экспорта CSV
  smoke    – быстрый дым-тест меню (войти/выйти)
  messages – проверки messages.py (синтаксис + дубли ключей)
  all      – выполнить всё выше по порядку

Если аргумент не указан, выполняется 'all'.
EOF
}

main() {
  local cmd="${1:-all}"
  case "${cmd}" in
    tests)  run_tests ;;
    export) check_export ;;
    smoke)  smoke_menu ;;
    messages)   check_messages ;;
    all)    run_tests; check_export; smoke_menu; check_messages ;;
    -h|--help|help) usage ;;
    *) usage; exit 1 ;;
  esac
}

main "$@"