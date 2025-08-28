# limits_tools.py
from __future__ import annotations
from typing import Dict, Optional, List
import pandas as pd
import streamlit as st
import numpy as np

from db import get_expenses_df
from utils import prev_month_key


def _to_float(x) -> float:
    """Безопасно привести скаляр/NumPy/pandas значение к float."""
    try:
        if pd.isna(x):
            return 0.0
        # у pandas/NumPy скаляров часто есть .item()
        if hasattr(x, "item"):
            x = x.item()
        return float(x)
    except Exception:
        return 0.0


def check_budget_limits(
    conn,  # совместимость со старым вызовом: параметр не используется
    *,
    messages: Optional[Dict[str, str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    budget_limits: Optional[Dict[str, Dict[str, float]]] = None,
) -> List[str]:
    """Проверка лимитов по диапазону дат. Возвращает список строк-предупреждений."""
    messages = messages or {}
    budget_limits = budget_limits or {}

    # NB: читаем активный путь из session_state — без зависимостей от app.py
    db_path = st.session_state.get("ACTIVE_DB_PATH", "data/default_expenses.db")
    df = get_expenses_df(db_path=db_path)
    if df is None or df.empty:
        return []

    df["month"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m")
    totals = df.groupby(["month", "category"])["amount"].sum().reset_index()

    out: List[str] = []
    for _, row in totals.iterrows():
        month = str(row["month"])
        cat = str(row["category"])
        total = float(row["amount"])

        limit = None
        month_limits = budget_limits.get(month) or {}
        if isinstance(month_limits, dict) and cat in month_limits:
            try:
                limit = float(month_limits[cat])
            except (TypeError, ValueError):
                limit = None

        line = f"{month} {cat}: ${total:.2f}"
        if limit is not None:
            status = (
                messages.get("over_limit", "Over!")
                if total > limit
                else messages.get("within_limit", "Within")
            )
            line += f" [{status}] (Limit: ${limit:.2f})"
        out.append(line)

    return out


def suggest_limits_for_month(user: str, month_key: str) -> Dict[str, float]:
    """
    Рекомендации лимитов на месяц по истории: среднее за последние 3 месяца.
    Fallback — значения прошлого месяца.
    """
    # Собираем список последних 4 месяцев (чтобы покрыть 3 полных)
    months = []
    cur = month_key
    for _ in range(4):
        months.append(cur)
        cur = prev_month_key(cur)

    db_path = st.session_state.get("ACTIVE_DB_PATH", "data/default_expenses.db")
    df = get_expenses_df(db_path=db_path)
    if df is None or df.empty:
        return {}

    df["ym"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m")
    pool = df[df["ym"].isin(months)]
    if pool.empty:
        return {}

    piv = pool.pivot_table(
        index="ym", columns="category", values="amount", aggfunc="sum"
    ).sort_index()

    # Среднее по последним трём
    sugg: Dict[str, float] = {}
    for cat in piv.columns:
        avg3 = piv[cat].tail(3).mean(skipna=True)
        sugg[cat] = _to_float(avg3)

    # Fallback по прошлому месяцу
    if all(v == 0.0 for v in sugg.values()):
        pm = prev_month_key(month_key)
        if pm in piv.index:
            for cat in piv.columns:
                v = piv.loc[pm, cat]
                try:
                    sugg[cat] = _to_float(v)
                except Exception:
                    sugg[cat] = 0.0

    return {k: round(v, 2) for k, v in sugg.items()}
