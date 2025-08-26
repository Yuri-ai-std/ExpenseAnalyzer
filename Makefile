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

# Проверка тестов
test:
	@echo "🧪 Running pytest..."
	@pytest -q

# Полный рабочий цикл
all: clean format test