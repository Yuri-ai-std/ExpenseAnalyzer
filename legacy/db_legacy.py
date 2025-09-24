# legacy/db_legacy.py
# -*- coding: utf-8 -*-
"""
SQLite database integration (LEGACY / ARCHIVE)

Архив старого API работы с БД. Сигнатуры сохранены для справки.
Любой вызов выдаёт DeprecationWarning и NotImplementedError.
"""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any

__all__ = [
    "get_conn",
    "categories_and_version",
    "get_all_expenses",
    "load_expenses",
    "update_expense",
    "delete_expense",
    "migrate_json_to_sqlite",
]


def _deprecated_stub(name: str) -> None:
    warnings.warn(
        f"[legacy] Function '{name}' is deprecated and kept only for archival purposes. "
        f"Use the new unified data pipeline instead.",
        category=DeprecationWarning,
        stacklevel=2,
    )
    raise NotImplementedError(
        f"[legacy] '{name}' has been archived. Migrate to the new data layer."
    )


def get_conn(db_path: str | Path) -> Any:
    _deprecated_stub("get_conn")


def categories_and_version(conn: Any) -> tuple[list[str], str]:
    _deprecated_stub("categories_and_version")


def get_all_expenses(conn: Any) -> "pd.DataFrame":
    _deprecated_stub("get_all_expenses")


def load_expenses(db_path: str | Path) -> "pd.DataFrame":
    _deprecated_stub("load_expenses")


def update_expense(conn: Any, expense_id: int, **fields: Any) -> None:
    _deprecated_stub("update_expense")


def delete_expense(conn: Any, expense_id: int) -> None:
    _deprecated_stub("delete_expense")


def migrate_json_to_sqlite(json_path: str | Path, db_path: str | Path) -> None:
    _deprecated_stub("migrate_json_to_sqlite")


if __name__ == "__main__":
    print(
        "This is an archived legacy module. "
        "Functions intentionally raise NotImplementedError to prevent accidental use."
    )
