# limits_tools.py
from __future__ import annotations

import csv
import io
import json
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st
from db import get_expenses_df
from utils import prev_month_key

# ---------- base utilities ----------


def _to_float(x: Any) -> float:
    """Безопасно привести скаляр/NumPy/pandas значение к float."""
    try:
        if pd.isna(x):
            return 0.0
        if hasattr(x, "item"):  # у pandas/NumPy скаляров часто есть .item()
            x = x.item()
        return float(x)
    except Exception:
        return 0.0


# ---------- limits check / suggestions ----------


def check_budget_limits(
    conn,  # совместимость со старым вызовом: параметр не используется
    *,
    messages: Optional[Dict[str, str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    budget_limits: Optional[Dict[str, Dict[str, float]]] = None,
) -> List[str]:
    """
    Проверка лимитов по диапазону дат.
    Возвращает список строк-предупреждений.

    Примечание:
    БД выбирается из st.session_state["ACTIVE_DB_PATH"]
    (fallback: data/default_expenses.db).
    """
    messages = messages or {}
    budget_limits = budget_limits or {}

    db_path = st.session_state.get("ACTIVE_DB_PATH", "data/default_expenses.db")
    df = get_expenses_df(db_path=str(db_path), start_date=start_date, end_date=end_date)
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
    """Рекомендации лимитов на месяц по истории: среднее за последние 3 месяца.
    Fallback — взять значения прошлого месяца.
    """
    # собираем 4 последовательных ключа месяцев (чтобы покрыть 3 полных)
    months = []
    cur = month_key
    for _ in range(4):
        months.append(cur)
        cur = prev_month_key(cur)

    db_path = st.session_state.get("ACTIVE_DB_PATH", "data/default_expenses.db")
    df = get_expenses_df(db_path=str(db_path))
    if df is None or df.empty:
        return {}

    df["ym"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m")
    pool = df[df["ym"].isin(months)]
    if pool.empty:
        return {}

    piv = pool.pivot_table(
        index="ym", columns="category", values="amount", aggfunc="sum"
    ).sort_index()

    sugg: Dict[str, float] = {}
    for cat in piv.columns:
        avg3 = piv[cat].tail(3).mean(skipna=True)
        sugg[cat] = _to_float(avg3)

    # fallback по прошлому месяцу, если всё нули
    if all(v == 0.0 for v in sugg.values()):
        pm = prev_month_key(month_key)
        if pm in piv.index:
            for cat in piv.columns:
                sugg[cat] = _to_float(piv.loc[pm, cat])

    return {k: round(v, 2) for k, v in sugg.items()}


# ---------- CSV helpers for monthly limits ----------


def limits_to_csv_bytes(limits: Dict[str, float]) -> bytes:
    """Сериализовать словарь лимитов текущего месяца в CSV-байты.
    Формат: category,limit
    """
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["category", "limit"])
    for cat, val in sorted(limits.items()):
        writer.writerow([cat, _to_float(val)])
    return buf.getvalue().encode("utf-8")


def csv_bytes_to_limits(data: bytes) -> Dict[str, float]:
    """Десериализовать CSV (category,limit) в словарь {category: float}."""
    out: Dict[str, float] = {}
    if not data:
        return out
    buf = io.StringIO(data.decode("utf-8"))
    reader = csv.DictReader(buf)
    for row in reader:
        cat = str(row.get("category", "")).strip()
        if not cat:
            continue
        out[cat] = _to_float(row.get("limit"))
    return out


# ---------- Audit helpers (session_state) ----------

_AUDIT_KEY = "limits_audit"


def append_audit_row(old: Dict[str, float], new: Dict[str, float]) -> None:
    """Добавить запись об изменении лимитов в session_state[_AUDIT_KEY]."""
    changes = []
    cats = sorted(set(old.keys()) | set(new.keys()))
    for cat in cats:
        before = _to_float(old.get(cat))
        after = _to_float(new.get(cat))
        if abs(before - after) > 1e-9:
            changes.append({"category": cat, "before": before, "after": after})
    if not changes:
        return

    rec = {
        "user": st.session_state.get("current_user", "default"),
        "month": st.session_state.get("current_limits_month", ""),  # опционально
        "changes": changes,
    }
    st.session_state.setdefault(_AUDIT_KEY, []).append(rec)


def get_audit() -> List[Dict[str, Any]]:
    """Вернуть текущий аудит изменений лимитов из session_state."""
    return list(st.session_state.get(_AUDIT_KEY, []))


def audit_to_json_bytes(audit: List[Dict[str, Any]] | None = None) -> bytes:
    """Аудит → JSON bytes."""
    payload = audit if audit is not None else get_audit()
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


def audit_to_csv_bytes(audit: List[Dict[str, Any]] | None = None) -> bytes:
    """Аудит → CSV bytes. Формат: user,month,category,before,after"""
    rows: List[List[Any]] = []
    payload = audit if audit is not None else get_audit()
    for rec in payload:
        user = rec.get("user", "")
        month = rec.get("month", "")
        for ch in rec.get("changes", []):
            rows.append(
                [
                    user,
                    month,
                    ch.get("category", ""),
                    _to_float(ch.get("before")),
                    _to_float(ch.get("after")),
                ]
            )

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["user", "month", "category", "before", "after"])
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")
