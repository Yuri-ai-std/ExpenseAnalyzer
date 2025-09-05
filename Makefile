# -------------------------------
# ExpenseAnalyzer_Restore - Makefile
# -------------------------------

# Быстрая чистка проекта (локальные кэши)
clean:
	@echo "🧹 Cleaning project cache..."
	@bash tools/cleanup_vscode.sh --yes

# Глубокая чистка (VS Code, Streamlit, pip кэши)
deep-clean:
	@echo "🧹 Cleaning project + user caches..."
	@bash tools/cleanup_vscode.sh --deep --yes

# Проверка без удаления
dry-run:
	@echo "🔍 Dry run - no files will be deleted"
	@bash tools/cleanup_vscode.sh --dry-run

# Форматирование кода (isort + black)
format:
	@echo "🎨 Sorting imports with isort and formatting code with black..."
	@isort .
	@black .

# ---- утилиты ----
.PHONY: clean deep-clean dry-run format format-check format-app lint test all

# Проверка форматирования (без изменений)
format-check:
	@echo "🔍 Checking formatting (isort + black --check)..."
	@isort . --check-only --profile black
	@black . --check

# Форматировать один файл (используйте: make format-app f=app.py)
format-app:
	@echo "✨ Formatting file: $(f)"
	@isort $(f) --profile black
	@black $(f)

# Линтер (если пользуетесь flake8 — удобно для быстрого запуска)
lint:
	@echo "🔎 Running flake8..."
	@flake8

# Пример: единая команда «исправь всё в app.py и запусти тесты»
fix-app:
	@$(MAKE) format-app f=app.py
	@$(MAKE) test

# Проверка тестов
test:
	@echo "🧪 Running pytest..."
	@pytest -q

# Полный рабочий цикл
all: clean format test