#!/usr/bin/env bash
# Cleanup script for VS Code, Streamlit, and Python caches

# Включаем поддержку рекурсивного поиска ** (если поддерживается)
shopt -s globstar 2>/dev/null || true

echo ""
echo "📂 Project root: $(pwd)"
echo "⚠️  Protecting: data data/* data/**/*.db data/**/*.json reports tools .git"
echo "This will remove cached/temporary files in the repo."
echo "Use --deep to also clear user-level caches (VS Code, Streamlit, pip wheels)."
echo ""

read -rp "Proceed? [y/N] " confirm
if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
  echo "❌ Aborted."
  exit 1
fi

echo ""
echo "🧹 Cleaning project caches..."

# Удаляем кеши и временные файлы проекта
rm -rf .pytest_cache __pycache__ **/__pycache__ .ipynb_checkpoints .ruff_cache .mypy_cache .vscode/*.log 2>/dev/null

# Удаляем временные файлы Python
find . -type f -name "*.pyc" -delete 2>/dev/null
find . -type f -name "*.pyo" -delete 2>/dev/null

# Удаляем временные логи Streamlit
rm -rf ~/.streamlit/cache 2>/dev/null

# Если выбран deep режим, чистим и user-level кеши
if [[ "$1" == "--deep" ]]; then
  echo "🧹 Deep cleaning user-level caches..."
  rm -rf ~/.cache/pip ~/.cache/huggingface ~/.vscode/*.log 2>/dev/null
fi

echo ""
echo "✅ Project cleanup done. (size: $(du -sh . | cut -f1))"