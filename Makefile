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

.PHONY: unused unused-fast

unused:
	@echo "🔎 Running vulture (unused code)…"
	@bash tools.sh unused

unused-fast:
	@echo "🔎 Running ripgrep heuristic (unused code)…"
	@bash tools.sh unused-fast

.PHONY: audit audit-clean

# === 1) Полный аудит дерева + отчёт ===
audit:
	@echo "🔎 Auditing project tree and large files..."
	@bash -c '\
	  { \
	    echo "=== TREE (trimmed) ==="; \
	    tree -ah --du -I ".git|.venv|node_modules|__pycache__|.pytest_cache|dist|build"; \
	    echo; echo "=== TOP DIRS ==="; \
	    du -hd1 . | sort -hr | head -n 20; \
	    echo; echo "=== TOP FILES ==="; \
	    find . -type f -not -path "*/.git/*" -not -path "*/.venv/*" -not -path "*/__pycache__/*" -exec du -h {} + | sort -hr | head -n 50; \
	    echo; echo "=== DB/CSV/JSON ==="; \
	    echo "-- .db"; find . -type f -name "*.db" | sort; \
	    echo "-- .csv"; find . -type f -name "*.csv" | sort; \
	    echo "-- .json"; find . -type f -name "*.json" | sort; \
	    echo; echo "=== >100MB FILES ==="; \
	    find . -type f -size +100M -not -path "*/.git/*" -not -path "*/.venv/*" -print0 | xargs -0 ls -lh | sort -k5 -hr; \
	  } > audit_report.txt'
	@echo "📄 Audit complete → see audit_report.txt"
	@open -a TextEdit audit_report.txt || true

# === 2) Аудит + авто-архив CSV/JSON из data/ ===
audit-clean: audit
	@echo "🗄️  Archiving legacy CSV/JSON from data/ → data/_archive_legacy/..."
	@mkdir -p data/_archive_legacy
	@find data -maxdepth 1 -type f \( -name "*.csv" -o -name "*.json" \) -print -exec mv {} data/_archive_legacy/ \;
	@echo "✅ Archive complete. All CSV/JSON moved to data/_archive_legacy/"
	@echo "📦 Remaining files in data/:"
	@find data -maxdepth 1 -type f -exec ls -lh {} \; | sort -k5 -hr || true
	@open -a TextEdit audit_report.txt || true

# === 3) Расширенная чистка: оставить только активную БД и лимиты ===
# Использование:
#   make audit-clean-extended ACTIVE_DB=data/default_expenses.db ACTIVE_LIMITS=data/default_budget_limits.json
#
# Если переменные не заданы — берём значения по умолчанию.
.PHONY: audit-clean-extended
ACTIVE_DB ?= data/default_expenses.db
ACTIVE_LIMITS ?= data/default_budget_limits.json

audit-clean-extended: audit
	@echo "🧹 Extended clean: keep only active DB/LIMITS, archive the rest..."
	@mkdir -p data/_archive_legacy exports
	@echo "→ Active DB:      $(ACTIVE_DB)"
	@echo "→ Active LIMITS:  $(ACTIVE_LIMITS)"
	@echo "→ Archiving other *.db / *.json / *.csv from data/ (except active pair)..."

	# Перенести все .db кроме активной
	@find data -maxdepth 1 -type f -name "*.db" ! -path "$(ACTIVE_DB)" -print -exec mv {} data/_archive_legacy/ \;

	# Перенести все .json кроме активного limits
	@find data -maxdepth 1 -type f -name "*.json" ! -path "$(ACTIVE_LIMITS)" -print -exec mv {} data/_archive_legacy/ \;

	# Перенести все csv из data/
	@find data -maxdepth 1 -type f -name "*.csv" -print -exec mv {} data/_archive_legacy/ \;

	# Экспорты в отдельную папку
	@find . -maxdepth 1 -type f -name "export*.csv" -print -exec mv {} exports/ \;

	@echo "✅ Extended archive complete."
	@echo "📦 Remaining files in data/:"
	@find data -maxdepth 1 -type f -exec ls -lh {} \; | sort -k5 -hr || true
	@open -a TextEdit audit_report.txt || true

# === 4) Глубокая уборка артефактов/кэшей ===
.PHONY: deep-clean
deep-clean:
	@echo "🧽 Deep clean: pyc/__pycache__/pytest_cache/build/dist..."
	@find . -name "__pycache__" -type d -prune -exec rm -rf {} + || true
	@find . -name "*.pyc" -delete || true
	@rm -rf .pytest_cache dist build || true
	@echo "✅ Deep clean done."