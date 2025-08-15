#!/bin/bash
# tools.sh — вспомогательный скрипт для разработки ExpenseAnalyzer

set -e  # прерывать выполнение при ошибках

echo "=== 1. Запуск тестов ==="
pytest -v || { echo "❌ Тесты не пройдены"; exit 1; }

echo -e "\n=== 2. Проверка экспорта CSV ==="
python project.py --export test_export.csv || { echo "❌ Ошибка экспорта CSV"; exit 1; }
[ -f test_export.csv ] && echo "✅ Файл test_export.csv создан" || { echo "❌ Файл test_export.csv не найден"; exit 1; }
rm -f test_export.csv

echo -e "\n=== 3. Smoke test проекта ==="
python project.py --help || { echo "❌ Smoke test не пройден"; exit 1; }

echo -e "\n=== 4. Проверка messages.py ==="
python -m py_compile messages.py && echo "✅ messages.py: синтаксис корректен"

echo -e "\n=== 5. Проверка дубликатов ключей в messages.py ==="
duplicates=$(grep -oP "^\s*'[^']+'" messages.py | sort | uniq -d)
if [ -n "$duplicates" ]; then
    echo "⚠️ Найдены дубликаты ключей:"
    echo "$duplicates"
    exit 1
else
    echo "✅ Дубликаты ключей не найдены"
fi

echo -e "\n🎯 Все проверки завершены успешно!"