import json
import os
import sqlite3
from datetime import date
from datetime import date as _date
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple, cast

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st
from attrs import field
from dateutil.relativedelta import relativedelta

from flash import flash, render_flash

# CSV/аудит для лимитов
from limits_tools import (
    append_audit_row,
    audit_to_csv_bytes,
    audit_to_json_bytes,
    csv_bytes_to_limits,
    get_audit,
    limits_to_csv_bytes,
)
from messages import t
from project import (
    add_expense,
    ensure_schema,
    get_db_path,
    get_expenses_df,
    list_categories,
)
from utils import (
    db_path_for,
    limits_path_for,
    load_monthly_limits,
    month_key,
    save_monthly_limits,
)


def debug_hud(page_name: str, df=None, extra: dict | None = None):
    lang = st.session_state.get("lang")
    user = st.session_state.get("ACTIVE_USER")
    db_path = st.session_state.get("ACTIVE_DB_PATH")
    ver = (
        st.session_state.get("__data_v__") or st.session_state.get("data_version") or 0
    )
    rows = len(df) if df is not None else "—"
    bits = {
        "page": page_name,
        "lang": lang,
        "user": user,
        "db_path": db_path,
        "data_ver": ver,
        "rows": rows,
        "ts": datetime.now().strftime("%H:%M:%S"),
    }
    if extra:
        bits.update(extra)
    st.caption(" | ".join(f"{k}={v}" for k, v in bits.items()))


# Обход старых type-stubs streamlit для параметра width="stretch"
st_any = cast(Any, st)

# --- aliases for tests (test_limits_io.py expects underscored names)
_limits_to_csv_bytes = limits_to_csv_bytes

# ---- Пользователь ----
st.session_state.setdefault("current_user", "default")


def current_user() -> str:
    """Возвращает текущего пользователя из session_state."""
    return st.session_state["current_user"]


def assert_db_path(p: str | Path) -> Path:
    """Гарантия, что путь ведёт на .db и БД готова."""
    path = Path(p)
    if path.suffix != ".db":
        raise ValueError(f"ACTIVE_DB_PATH must be a .db file, got: {path}")
    ensure_schema(path)
    return path


# Основные пути (только .db и .json)
ACTIVE_DB_PATH = assert_db_path(db_path_for(current_user()))  # data/default_expenses.db
ACTIVE_DB_PATH_STR = str(ACTIVE_DB_PATH)
ACTIVE_LIMITS_PATH = limits_path_for(current_user())  # data/default/budget_limits.json
DATA_DIR = Path("data")

BASE_CATEGORIES = [
    "entertainment",
    "food",
    "groceries",
    "other",
    "transport",
    "utilities",
]

DEBUG_HUD = False

# Делаем пути видимыми для других модулей через session_state
st.session_state["ACTIVE_DB_PATH"] = ACTIVE_DB_PATH_STR
st.session_state["ACTIVE_LIMITS_PATH"] = str(ACTIVE_LIMITS_PATH)

LabelFn = Callable[[Any], str]

# ---- legacy _flash -> new flash shim ----
_legacy = st.session_state.pop("_flash", None)
if _legacy:
    from flash import flash

    msg, icon = (_legacy + (None,))[:2]
    level = {"✅": "success", "ℹ️": "info", "⚠️": "warning", "❌": "error"}.get(
        icon, "info"
    )
    flash(str(msg), level, 3.0)

# ---- Active user & paths (single source of truth) ----


def _parse_limits_csv(data):
    """Парсинг CSV → dict; принимает bytes, str или io.BytesIO."""
    # io.BytesIO или любой объект-файлоподобный
    if hasattr(data, "read"):
        data = data.read()
    # строку превращаем в bytes
    elif isinstance(data, str):
        data = data.encode("utf-8")
    # далее точно bytes
    return csv_bytes_to_limits(data)


def get_active_user() -> str:
    """Имя активного пользователя из session_state (по умолчанию 'default')."""
    return st.session_state.setdefault("current_user", "default")


def get_active_paths():
    """Возвращает (db_path, limits_path) для активного пользователя, всегда актуальные."""
    user = get_active_user()
    return db_path_for(user), limits_path_for(user)


_db, _limits = get_active_paths()
st.caption(f"DB: {_db} — Limits: {_limits.name}")


def prepare_df_for_view(
    df: pd.DataFrame,
    *,
    remove_dups: bool = True,
    newest_first: bool = True,
) -> pd.DataFrame:
    """Очищает и сортирует данные для отображения/метрик."""
    d = df.copy()
    if remove_dups:
        d = d.drop_duplicates(
            subset=["date", "category", "amount", "description"],
            keep="last",
        )
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d = d.sort_values("date", ascending=not newest_first).reset_index(drop=True)
    return d


def _normalize_limits_json(obj: dict) -> dict[str, dict[str, float]]:
    """
    Приводим вход к формату:
      {"YYYY-MM": {"food": 200.0, "transport": 50.0, ...}, ...}
    Бросаем ValueError при неверной структуре.
    """
    if not isinstance(obj, dict):
        raise ValueError("Root must be an object")
    out: dict[str, dict[str, float]] = {}
    for mk, limits in obj.items():
        if not isinstance(mk, str) or len(mk) != 7 or mk[4] != "-":
            raise ValueError(f"Invalid month key: {mk!r}")
        if not isinstance(limits, dict):
            raise ValueError(f"Month {mk} must map to an object of category->limit")
        inner: dict[str, float] = {}
        for cat, val in limits.items():
            if not isinstance(cat, str) or not cat.strip():
                continue
            try:
                f = float(val)
            except Exception:
                continue
            if f >= 0:
                inner[cat.strip()] = f
        out[mk] = inner
    return out


def _mdonth_key(date_value):
    """
    Преобразует дату в строку формата YYYY-MM для работы с месячными лимитами
    """
    return date_value.strftime("%Y-%m")


def _collect_limits_from_form(prefix: str) -> Dict[str, float]:
    """
    Считывает все st.session_state[prefix + <category>] и возвращает словарь {category: float}.
    Никаких внешних зависимостей — работает по текущей форме.
    """
    out: Dict[str, float] = {}
    plen = len(prefix)
    for k, v in st.session_state.items():
        if isinstance(k, str) and k.startswith(prefix):
            cat = k[plen:]
            try:
                out[cat] = float(v) if v not in ("", None) else 0.0
            except Exception:
                # игнорируем нечисловые/пустые значения поля
                pass
    return out


# ---- Табличные заголовки (локализация) ----
def _col_labels(lang: str) -> dict[str, str]:
    return {
        "id": t("col.id", lang, default="id"),
        "date": t("col.date", lang, default="Date"),
        "category": t("col.category", lang, default="Category"),
        "amount": t("col.amount", lang, default="Amount"),
        "description": t("col.description", lang, default="Description"),
    }


# ===== Унифицированный вывод таблиц =====
def render_table(
    df,
    cols=None,
    lang="en",
    hide_index=True,
    *,
    width="stretch",  # 'stretch' | 'content' | int(px)
    height=None,  # int(px) | 'stretch' | 'auto' | None
    labels: dict[str, str] | None = None,
):
    """
    Универсальный рендер таблиц для Streamlit:
    - width: 'stretch' растягивает таблицу на ширину контейнера,
             'content' = по содержимому, либо конкретная ширина в пикселях (int).
    - height: целое число пикселей, либо 'stretch'/'auto'. Если None — параметр не передаем.
    """

    # 1) Подготовка датафрейма и локализация (если передали labels)
    df_disp = df if cols is None else df.loc[:, cols]
    if labels is not None:
        df_disp = _localize_category_column(df_disp, labels)

    # 2) Нормализация аргументов под разные версии Streamlit
    st_kwargs: dict = {}

    # ширина: новые версии — width='stretch'/'content'/int
    if isinstance(width, int):
        st_kwargs["width"] = width
    elif width in ("stretch", "content"):
        st_kwargs["width"] = width  # для новых версий
    # если None — просто не передаем ничего

    # высота: передаем ТОЛЬКО если не None
    if isinstance(height, int) or (
        isinstance(height, str) and height in ("stretch", "auto")
    ):
        st_kwargs["height"] = height
    # если None — не добавляем 'height' вовсе (иначе RuntimeError)

    # 3) Сам вывод
    st.dataframe(df_disp, hide_index=hide_index, **st_kwargs)


def render_recent_expenses_table(
    db_path: str, n: int = 10, *, show_title: bool = False, lang: str = "en"
) -> None:
    if show_title:
        st.subheader(t("recent_expenses", lang, default="Recent expenses"))

    raw_df = get_expenses_df(db_path=str(db_path))
    df = prepare_df_for_view(raw_df, remove_dups=True, newest_first=True)
    df_recent = df.head(n)

    # 🔹 Локальная локализация категорий + заголовков
    _, cat_labels = categories_ui(lang)  # <- НЕТ внешних глобальных ссылок
    df_recent = df_recent.copy()
    if "category" in df_recent.columns:
        df_recent["category"] = df_recent["category"].map(
            lambda c: cat_labels.get(str(c), str(c))
        )

    # Заголовки столбцов
    col_names = _col_labels(lang)
    df_recent = df_recent.rename(columns=col_names)

    st.dataframe(
        df_recent,
        hide_index=True,
        width="stretch",
        column_config={
            "amount": st.column_config.NumberColumn(
                t("col.amount", lang, default="Amount"), format="%.2f"
            ),
            "date": st.column_config.DatetimeColumn(
                t("col.date", lang, default="Date"), format="YYYY-MM-DD"
            ),
        },
    )


# ===== ЛОГ ПЕРЕЗАПУСКА =====
print(f"\n🔄 Streamlit перезапущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")


# ===== Вспомогательные функции =====
@st.cache_data(ttl=10, show_spinner=False)
def load_df(
    db_path: str,  # <— НОВОЕ: путь к БД теперь часть ключа кэша
    start: str | None = None,
    end: str | None = None,
    *,
    _ver: int = 0,
) -> pd.DataFrame:
    """
    Загружает операции из БД как DataFrame.
    db_path входит в ключ кэша — смена профиля всегда даёт свежие данные.
    Параметр _ver используется только для инвалидации кэша.
    """
    df = get_expenses_df(str(db_path), start_date=start, end_date=end)

    expected = ["date", "category", "amount", "description"]
    for col in expected:
        if col not in df.columns:
            df[col] = pd.NA

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    return df.dropna(subset=["date", "amount"])


@st.cache_data(ttl=120)
def get_categories(
    db_path: str = "expenses.db", ver: int = 0
) -> Tuple[list[str], float]:
    """
    Список категорий из БД (distinct), кэшируем на 2 мин.
    Кэш дополнительно «привязан» к:
      - пути к БД (db_path),
      - версии данных (ver),
    и сбрасывается при изменении файла БД (mtime).
    """
    try:
        db_mtime = os.path.getmtime(db_path)

        with sqlite3.connect(db_path) as conn:
            rows = conn.execute("SELECT DISTINCT category FROM expenses").fetchall()
            cats = [r[0] for r in rows if r and r[0]]

        return cats, db_mtime
    except Exception:
        return [], 0.0


def make_category_formatter(labels: dict[str, str]):
    """Создаёт функцию: key -> локализованная подпись"""

    def fmt(val: Any) -> str:
        s = str(val) if val is not None else ""
        return labels.get(s, s)

    return fmt


def categories_ui(lang: str) -> tuple[list[str], dict[str, str]]:
    """
    Возвращает:
      - cats: список ТЕХКЛЮЧЕЙ (отсортированный по локализованной подписи)
      - labels: словарь {ключ -> локализованная подпись}
    """
    db_path = str(st.session_state.get("ACTIVE_DB_PATH", "data/default_expenses.db"))

    # 1) достаём категории из БД (поддержим обе сигнатуры get_categories)
    try:
        got = get_categories(db_path=db_path, ver=get_data_version())
        db_cats = got[0] if isinstance(got, tuple) else got
    except Exception:
        db_cats = []

    # 2) UNION базовых и БД
    all_cats = set(BASE_CATEGORIES) | {str(c).strip() for c in db_cats if c}

    # 3) локализация (ключи в lower(), чтобы 'VISA' совпадало с 'visa' в messages)
    def tr(key: str) -> str:
        return t(f"categories.{key.lower()}", lang, default=key)

    labels = {c: tr(c) for c in all_cats}

    # 4) сортировка по локализованной подписи
    cats_sorted = sorted(all_cats, key=lambda c: labels[c].lower())
    return list(cats_sorted), labels


def build_limit_suggestions(
    df: pd.DataFrame,
    *,
    month_ref: date,  # текущий выбранный месяц (любой день месяца)
    months: int,  # окно анализа, напр. 3
    limits_by_cat: dict[str, float],  # текущие лимиты по категориям (0.0 если нет)
    labels: dict[str, str],  # локализованные названия категорий
    lang: str,  # 'en' | 'fr' | 'es' | 'ru'
) -> list[str]:
    """
    Возвращает список строк (Markdown) с рекомендациями по лимитам:
    - считаем средний расход за N последних месяцев (включая текущий),
    - сравниваем с действующим лимитом,
    - предлагаем «подтянуть» лимит до среднего (или выделяем сильные отклонения).
    """

    # Границы окна: от первого числа (month_ref - (months-1)) до конца текущего month_ref
    start_month = (
        pd.to_datetime(month_ref).to_period("M") - (months - 1)
    ).to_timestamp(how="start")
    end_month = pd.to_datetime(month_ref).to_period("M").to_timestamp(how="end")

    # Фильтр по окну
    dfw = df.copy()
    dfw["date"] = pd.to_datetime(dfw["date"])
    dfw = dfw[(dfw["date"] >= start_month) & (dfw["date"] <= end_month)]
    if dfw.empty:
        return []

    # Сумма по месяцам и категориям -> затем среднее по месяцам
    # (берём ровно N месяцев, даже если по категории нет операций в какой-то месяц)
    dfw["month"] = dfw["date"].dt.to_period("M").dt.to_timestamp(how="start")
    sums = dfw.groupby(["category", "month"], as_index=False)["amount"].sum()

    # Все месяцы окна (ровно N)
    months_index = pd.date_range(start=start_month, end=end_month, freq="MS")

    # Гарантируем наличие «пустых» месяцев для точного среднего
    all_rows = (
        pd.MultiIndex.from_product(
            [sums["category"].unique(), months_index], names=["category", "month"]
        )
        .to_frame(index=False)
        .merge(sums, how="left", on=["category", "month"])
        .fillna({"amount": 0.0})
    )

    avg = all_rows.groupby("category", as_index=False)["amount"].sum()
    avg["avg"] = avg["amount"] / float(months)

    # Формируем строки с i18n-фрагментами
    lines: list[str] = []
    for _, row in avg.iterrows():
        key = str(row["category"])
        cat_label = labels.get(key, key)
        current_limit = float(limits_by_cat.get(key, 0.0))
        avg_val = float(row["avg"])

        # Решение: предлагаем «подтянуть» до среднего, если заметимо отличается
        # Порог «заметности» — 5% или ≥ 1 у.е.
        delta = avg_val - current_limit
        significant = abs(delta) >= max(1.0, 0.05 * max(avg_val, current_limit))

        # Стрелка тренда относительно текущего лимита
        arrow = "↗" if delta > 0 else ("↘" if delta < 0 else "→")

        # Локализуем небольшие текстовые фрагменты через t()
        # (сами числа и имена категорий понятны и без перевода)
        s_avg = t("sugg.avg", lang, default="avg")
        s_limit = t("sugg.limit", lang, default="limit")
        s_consider = t("sugg.consider", lang, default="consider")
        s_last = t("sugg.last_m", lang, default="last {n} mo").format(n=months)

        # Если отличие незначительное — всё равно показываем, но без «жёсткой» рекомендации
        recommendation = (
            f"**{s_consider}** {avg_val:,.2f}"
            if significant
            else f"{s_consider} ~ {avg_val:,.2f}"
        )

        line = (
            f"- {arrow} **{cat_label}**: {s_avg} {s_last} **{avg_val:,.2f}** • "
            f"{s_limit} **{current_limit:,.2f}** → {recommendation}"
        )
        lines.append(line)

    # Сортировка: сначала большие отклонения
    lines.sort(reverse=True)
    return lines


def classify_diff(avg_val: float, limit: float, *, tol_ok=0.05, tol_warn=0.20) -> str:
    """
    Классифицирует разницу между средними расходами и лимитом.
    Возвращает текстовую метку: 'OK', 'WARN', 'ALERT'.
    - OK: |diff| <= tol_ok
    - WARN: tol_ok < |diff| <= tol_warn
    - ALERT: |diff| > tol_warn
    """
    if limit <= 0:
        return "ALERT" if avg_val > 0 else "OK"

    rel_diff = abs(avg_val - limit) / limit
    if rel_diff <= tol_ok:
        return "OK"
    elif rel_diff <= tol_warn:
        return "WARN"
    else:
        return "ALERT"


def get_month_limits_dict(
    *, month: date, lang: str, user: str = "default"
) -> dict[str, float]:
    """
    Возвращает словарь лимитов на месяц для всех категорий.
    Берёт лимиты из JSON через load_monthly_limits(), заполняет отсутствующие категории нулями.
    """
    cats_all, _ = categories_ui(lang)  # уже есть в проекте
    limits_map = load_monthly_limits(user=user)  # уже есть в проекте
    mk = month.strftime("%Y-%m")  # ключ "YYYY-MM"
    cur_limits = limits_map.get(mk, {}) or {}
    return {c: float(cur_limits.get(c, 0.0)) for c in cats_all}


def make_fmt(labels: dict[str, str]) -> LabelFn:
    """Возвращает безопасный форматтер: key -> локализованный label (str)."""

    def _fmt(v: Any) -> str:
        s = "" if v is None else str(v)
        return labels.get(s, s)

    return _fmt


# --- Locale-aware labelExpr builders for Vega (Altair) ---


def _vega_month_expr(lang: str, short: bool = True) -> str:
    """JS-выражение для вывода месяцев в нужной локали (массив строк)."""
    if lang == "fr":
        months = (
            "['janv.','févr.','mars','avr.','mai','juin','juil.','août','sept.','oct.','nov.','déc.']"
            if short
            else "['janvier','février','mars','avril','mai','juin','juillet','août','septembre','octobre','novembre','décembre']"
        )
    elif lang == "es":
        months = (
            "['ene.','feb.','mar.','abr.','may.','jun.','jul.','ago.','sept.','oct.','nov.','dic.']"
            if short
            else "['enero','febrero','marzo','abril','mayo','junio','julio','agosto','septiembre','octubre','noviembre','diciembre']"
        )
    else:  # en
        months = (
            "['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']"
            if short
            else "['January','February','March','April','May','June','July','August','September','October','November','December']"
        )
    return f"{months}[month(datum.value)]"


def _vega_day_month_expr(lang: str) -> str:
    """'DD mon' на нужном языке (например '12 sept.')."""
    return "timeFormat(datum.value, '%d ') + " + _vega_month_expr(lang, short=True)


def _vega_month_with_year_on_jan_expr(lang: str, short: bool = True) -> str:
    """Месяц в нужной локали; для января добавляет год: 'jan 2024' / 'janv. 2024' / 'ene. 2024'."""
    mon = _vega_month_expr(lang, short=short)
    # Если январь (month()==0) → добавляем год, иначе только месяц
    return (
        f"(month(datum.value)==0 ? {mon} + ' ' + timeFormat(datum.value, '%Y') : {mon})"
    )


# ---- ЕДИНЫЙ ФИЛЬТР ПО ПЕРИОДУ + ПОЛЯМ BROWSE ----
def get_filtered_df_for_period(
    base_df: pd.DataFrame,
    *,
    start: date,
    end: date,
    categories: list[str] | None = None,
    search: Optional[str] = None,
    min_amount: float | None = None,
    max_amount: float | None = None,
) -> pd.DataFrame:
    df = base_df.copy()
    if df.empty:
        return df

    # нормализуем даты и фильтруем по периоду
    d = pd.to_datetime(df["date"], errors="coerce")
    df = df[~d.isna()]
    df = df[(d >= pd.to_datetime(start)) & (d <= pd.to_datetime(end))]

    # категории (если заданы)
    if categories:
        df = df[df["category"].astype(str).isin(categories)]

    # суммы
    if min_amount is not None:
        df = df[df["amount"].astype(float) >= float(min_amount)]
    if max_amount is not None:
        df = df[df["amount"].astype(float) <= float(max_amount)]

    # ПОИСК: и по description, и по category (без учета регистра)
    if search:
        s = search.strip().casefold()
        if s:
            cat = df["category"].fillna("").astype(str).str.casefold()
            desc = df["description"].fillna("").astype(str).str.casefold()
            df = df[cat.str.contains(s) | desc.str.contains(s)]

    return df


# ---- язык интерфейса ----
st.session_state.setdefault("lang", "en")
lang = st.session_state["lang"]

# чтение лимитов пользователя
limits = load_monthly_limits(user=current_user())

# ...изменили словарь limits на форме...

# сохранение лимитов пользователя
save_monthly_limits(limits, user=current_user())


# ===== Add Expense: helpers =====


def add_form_keys(user: str | None = None) -> dict[str, str]:
    sfx = user or st.session_state.get("current_user", "default")
    return {
        "mode": f"add_cat_mode_{sfx}",
        "choose": f"add_cat_choose_{sfx}",
        "new": f"add_cat_new_{sfx}",
        "date": f"add_date_{sfx}",
        "amount": f"add_amount_{sfx}",
        "note": f"add_note_{sfx}",
        "reset": f"add_form_reset_{sfx}",  # флаг сброса
    }


# ===== Версия данных (для инвалидирования кэша) =====


def get_data_version() -> int:
    """Текущая версия данных для инвалидации кэшей."""
    return st.session_state.setdefault("data_version", 0)


def bump_data_version() -> None:
    """Инкремент версии данных — все кэшируемые загрузчики получают новый _ver."""
    st.session_state["data_version"] = get_data_version() + 1


# ---- запросить сброс (ставим только флажок!) ----
def request_form_reset(keys: dict[str, str]) -> None:
    st.session_state[keys["reset"]] = True


# ---- применить сброс (реально чистим значения) ----


def apply_form_reset(keys: dict[str, str]) -> None:
    ss = st.session_state
    if ss.pop(keys["reset"], False):
        # НЕ трогаем режим:
        # ss[keys["mode"]] оставляем как есть
        ss[keys["choose"]] = None
        ss[keys["new"]] = ""
        ss[keys["amount"]] = 0.0
        ss[keys["note"]] = ""
        ss[keys["date"]] = ss.get(keys["date"], _date.today())


def _localize_category_column(df: pd.DataFrame, labels: dict[str, str]) -> pd.DataFrame:
    """Вернёт копию df, где category отображается локализованной подписью.
    Данные/экспорт не трогаем — только вид."""
    if "category" not in df.columns:
        return df
    d = df.copy()
    d["category"] = d["category"].map(lambda c: labels.get(str(c), str(c)))
    return d


# --- Меню ---
lang = st.session_state.get("lang", "en")

MENU = {
    "dashboard": "menu.dashboard",
    "add_expense": "menu.add_expense",
    "browse": "menu.browse",
    "charts": "menu.charts",
    "settings": "menu.settings",
}

choice = st.sidebar.radio(
    label=t("menu.title", lang, default="Menu"),
    options=list(MENU.keys()),
    format_func=lambda k: t(MENU[k], lang, default=k),
    key="sidebar_choice",
)

# ------ Dashboard ------
if choice == "dashboard":
    st.header(t("menu.dashboard", lang, default="Dashboard"))
    st.write(
        "📊 " + t("dashboard.placeholder", lang, default="Dashboard page (placeholder)")
    )
    render_flash()

    # i18n для категорий
    lang = st.session_state.get("lang", "en")
    cats, cat_labels = categories_ui(lang)
    fmt = make_category_formatter(cat_labels)

    # --- базовые данные из активной БД ---
    base_df = load_df(ACTIVE_DB_PATH_STR, _ver=get_data_version())

    # ===== Фильтры по дате =====
    today = date.today()
    month_start = today.replace(day=1)

    if "dash_start" not in st.session_state:
        st.session_state["dash_start"] = month_start.isoformat()
    if "dash_end" not in st.session_state:
        st.session_state["dash_end"] = today.isoformat()

    # --- clamp диапазона дат ДО виджетов ---
    def clamp_date(value: date, lo: date, hi: date) -> date:
        return min(max(value, lo), hi)

    data_min = (
        pd.to_datetime(base_df["date"]).min().date() if not base_df.empty else today
    )
    data_max = (
        pd.to_datetime(base_df["date"]).max().date() if not base_df.empty else today
    )

    s = clamp_date(
        pd.to_datetime(st.session_state["dash_start"]).date(), data_min, data_max
    )
    e = clamp_date(
        pd.to_datetime(st.session_state["dash_end"]).date(), data_min, data_max
    )

    st.session_state["dash_start"], st.session_state["dash_end"] = (
        s.isoformat(),
        e.isoformat(),
    )
    # --- конец clamp ---
    c1, c2, c3 = st.columns((1, 1, 0.5))
    with c1:
        start_d = st.date_input(
            t("common.start", lang, default="Start"),
            value=pd.to_datetime(st.session_state["dash_start"]).date(),
            key="dash_start_input",
        )
    with c2:
        end_d = st.date_input(
            t("common.end", lang, default="End"),
            value=pd.to_datetime(st.session_state["dash_end"]).date(),
            key="dash_end_input",
        )
    with c3:
        refresh = st.button(t("common.apply", lang, default="Apply"), key="dash_apply")

    if refresh:
        st.session_state["dash_start"] = start_d.isoformat()
        st.session_state["dash_end"] = end_d.isoformat()
        st.session_state["_flash"] = (
            t("dashboard.filters_applied", lang, default="Filters applied"),
            "⚙️",
        )
        st.rerun()

    # ===== ЕДИНЫЙ фильтр данных (как на Browse) =====
    df_filtered = get_filtered_df_for_period(
        base_df,
        start=pd.to_datetime(st.session_state["dash_start"]).date(),
        end=pd.to_datetime(st.session_state["dash_end"]).date(),
        # без category/search/min/max — «всё за период»
    )

    # ===== KPI =====
    total = float(df_filtered["amount"].sum()) if not df_filtered.empty else 0.0
    count = int(len(df_filtered))
    avg = float(df_filtered["amount"].mean()) if count else 0.0
    cats_n = int(df_filtered["category"].nunique()) if not df_filtered.empty else 0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric(t("kpi.total", lang, default="Total"), f"{total:.2f}")
    k2.metric(t("kpi.operations", lang, default="Operations"), f"{count}")
    k3.metric(t("kpi.average", lang, default="Average"), f"{avg:.2f}")
    k4.metric(t("kpi.categories", lang, default="Categories"), f"{cats_n}")

    # --- временный HUD для проверки ---
    with st.expander("🔎 Debug HUD (temp)"):
        dmin = (
            str(pd.to_datetime(df_filtered["date"]).min())
            if not df_filtered.empty
            else "-"
        )
        dmax = (
            str(pd.to_datetime(df_filtered["date"]).max())
            if not df_filtered.empty
            else "-"
        )
        st.write(
            {
                "db_path": str(st.session_state.get("ACTIVE_DB_PATH")),
                "user": st.session_state.get("current_user"),
                "range": {
                    "start": st.session_state["dash_start"],
                    "end": st.session_state["dash_end"],
                    "df_min": dmin,
                    "df_max": dmax,
                },
                "rows": len(df_filtered),
                "total": total,
                "ops": count,
                "cats": cats_n,
            }
        )

    st.divider()

    # ===== Last operations (TOP N) =====
    st.subheader(t("dashboard.last_operations", lang, default="Last operations"))
    last_ops = df_filtered.sort_values(["date"], ascending=False).head(10).copy()
    last_ops["date"] = pd.to_datetime(last_ops["date"]).dt.strftime("%Y-%m-%d")
    last_ops["category"] = last_ops["category"].map(fmt)

    st.dataframe(
        last_ops[["date", "category", "amount", "description"]],
        width="stretch",
        hide_index=True,
        height=220,
        column_config={"amount": st.column_config.NumberColumn(format="%.2f")},
    )

    # ===== Полный список за период =====
    st.subheader(
        t("dashboard.recent_expenses_full", lang, default="All expenses in period")
    )
    all_exp = df_filtered.copy()
    all_exp["date"] = pd.to_datetime(all_exp["date"]).dt.strftime("%Y-%m-%d")
    all_exp["category"] = all_exp["category"].map(fmt)

    st.dataframe(
        all_exp[["id", "date", "category", "amount", "description"]],
        width="stretch",
        hide_index=True,
        height=420,
        column_config={"amount": st.column_config.NumberColumn(format="%.2f")},
    )

    # ===== Диаграмма по категориям (опционально) =====
    st.subheader(t("dashboard.by_category", lang, default="By category"))
    cat_totals = (
        df_filtered.groupby("category", dropna=False)["amount"]
        .sum()
        .sort_values(ascending=False)
        .rename("total")
        .to_frame()
    )
    cat_totals_local = cat_totals.copy()
    cat_totals_local.index = cat_totals_local.index.map(fmt)
    st.bar_chart(cat_totals_local)


# =================== Add Expense ===================
elif choice == "add_expense":
    lang = st.session_state.get("lang", "en")
    st.header(t("menu.add_expense", lang, default="Add Expense"))
    render_flash()

    # стабильные токены режима
    MODE_CHOOSE = "choose"
    MODE_NEW = "new"

    # ключи формы и состояние
    user = current_user()
    keys = add_form_keys(user)  # <- у вас уже есть этот генератор ключей
    ss = st.session_state

    # дефолтный режим при первом входе
    if keys["mode"] not in ss:
        ss[keys["mode"]] = MODE_NEW

    # применяем сброс формы (ВАЖНО: хелпер ждёт keys, не user)
    apply_form_reset(keys)

    # переключатель режима
    def _on_mode_change():
        request_form_reset(keys)  # <- тоже передаём keys

    try:
        st.segmented_control(
            label=t("browse.category", lang, default="Category"),
            options=[MODE_CHOOSE, MODE_NEW],
            format_func=lambda m: (
                t("add_expense.mode.existing", lang, default="Choose existing")
                if m == MODE_CHOOSE
                else t("add_expense.mode.new", lang, default="Enter new")
            ),
            key=keys["mode"],
            on_change=_on_mode_change,
        )
    except Exception:
        # fallback на radio, если segmented_control недоступен
        ss[keys["mode"]] = st.radio(
            t("browse.category", lang, default="Category"),
            options=[MODE_CHOOSE, MODE_NEW],
            index=[MODE_CHOOSE, MODE_NEW].index(ss[keys["mode"]]),
            format_func=lambda m: (
                t("add_expense.mode.existing", lang, default="Choose existing")
                if m == MODE_CHOOSE
                else t("add_expense.mode.new", lang, default="Enter new")
            ),
            key="add_expense_mode_radio",
        )

    # категории
    cats, cat_labels = categories_ui(lang)
    fmt = make_fmt(cat_labels)

    # def cat_label_fn(c: Any) -> str:
    #     Всегда str: никаких Optional
    #     return str(cat_labels.get(c, c))

    # ----- форма -----
    with st.form(f"add_form_{user}", clear_on_submit=False):
        d = st.date_input(t("col.date", lang, default="Date"), key=keys["date"])

        mode = ss[keys["mode"]]
        if mode == MODE_CHOOSE:
            opts = cats if len(cats) > 0 else [""]
            index0 = 0
            cat_val = st.selectbox(
                t("add_expense.choose_existing", lang, default="Choose category"),
                options=cats,
                index=0 if cats else None,
                format_func=fmt,
                key=keys["choose"],
            )
            if not cats or cat_val == "":
                cat_val = None
            new_val = ""
        else:
            new_val = st.text_input(
                t("add_expense.new_category", lang, default="New category"),
                key=keys["new"],
            )
            cat_val = None

        amt = st.number_input(
            t("col.amount", lang, default="Amount"),
            min_value=0.0,
            step=1.0,
            key=keys["amount"],
        )
        note = st.text_area(
            t("common.description", lang, default="Description"),
            key=keys["note"],
        )

        submit = st.form_submit_button(t("common.submit", lang, default="Submit"))

    # ----- обработка сабмита -----
    if submit:
        errors = []

        # 1) дата -> ISO-строка
        date_str = ""
        if d:
            try:
                # d это date | datetime | None
                date_str = d.strftime("%Y-%m-%d")
            except Exception:
                date_str = str(d)
        else:
            errors.append(
                t("error.missing_date", lang, default="Please select a date.")
            )

        # 2) категория -> гарантируем строку
        if mode == MODE_NEW:
            cat = (new_val or "").strip()
        else:
            cat = (cat_val or "").strip()
        if not cat:
            errors.append(
                t(
                    "error.missing_category",
                    lang,
                    default="Please choose or enter a category.",
                )
            )

        # 3) сумма > 0
        try:
            ok_amount = float(amt) > 0
        except Exception:
            ok_amount = False
        if not ok_amount:
            errors.append(
                t(
                    "error.invalid_amount",
                    lang,
                    default="Amount must be greater than 0.",
                )
            )

        # показываем ошибки или сохраняем
        if errors:
            for e in errors:
                st.error(e)
        else:
            # сохранение: ваша функция добавления записи ожидает date как str
            add_expense(
                date=date_str,
                category=cat,
                amount=float(amt),
                description=(note or "").strip(),
            )

            # ⬇️ Сразу после успешной записи ОБНОВЛЯЕМ версию данных:
            st.cache_data.clear()  # сбрасываем кэши загрузчиков
            bump_data_version()

            flash(
                t("info.expense_added", lang, default="Expense added."), "success", 3.5
            )
            request_form_reset(keys)
            st.rerun()

    # ---- таблица сегодняшних расходов ----
    today = pd.to_datetime("today").date()

    df_today = get_expenses_df(ACTIVE_DB_PATH_STR)
    df_today = df_today[pd.to_datetime(df_today["date"]).dt.date == today]

    if not df_today.empty:
        st.subheader(t("add_expense.today_table", lang, default="Expenses added today"))
        st.dataframe(
            df_today,
            width="stretch",  # ✅ новая версия API
            hide_index=True,
        )
    else:
        st.info(t("add_expense.no_today", lang, default="No expenses added today yet."))

# ====== Browse & Filter ======
if choice == "browse":
    st.header(t("browse.title", lang, default="Browse & Filter"))
    st.caption(
        "🔍 "
        + t("browse.placeholder", lang, default="Browse & Filter page (placeholder)")
    )

    ss = st.session_state

    # ---------- Source dataframe + date bounds (shim, with fallback load) ----------
    # 1) пробуем найти уже загруженный DF под разными именами
    _base_candidates = []
    for _name in ("base_df", "raw_df", "ops_df", "data_df", "df"):
        if _name in locals():
            _base_candidates.append(locals()[_name])
        elif _name in globals():
            _base_candidates.append(globals()[_name])
        elif isinstance(st.session_state.get(_name), pd.DataFrame):
            _base_candidates.append(st.session_state[_name])

    base_df = next((x for x in _base_candidates if isinstance(x, pd.DataFrame)), None)

    # 2) если не нашли — грузим как на Dashboard
    if base_df is None:
        try:
            base_df = load_df(ACTIVE_DB_PATH_STR, _ver=get_data_version())
            # по желанию сохраним для повторного использования
            st.session_state["base_df"] = base_df
        except Exception as e:
            st.error(f"Browse page: cannot load base dataframe: {e}")
            st.stop()

    # 3) нормализуем даты и считаем границы
    _date_series = pd.to_datetime(base_df["date"], errors="coerce")
    if _date_series.isna().all():
        st.error("Browse page: 'date' column is empty or invalid.")
        st.stop()

    data_min = _date_series.min().date()
    data_max = _date_series.max().date()

    # ==== Ожидаемые хелперы/данные ====
    # - base_df: DataFrame с колонками ['id','date','category','amount','description']
    # - data_min, data_max: крайние даты в данных
    # - categories_ui(lang) -> (cats_all: list[str], labels: dict[str,str])
    # - make_fmt(labels) -> callable key->localized label
    cats_all, labels = categories_ui(lang)
    fmt = make_fmt(labels)

    # Значения по умолчанию в сессии
    ss.setdefault("bf_start", pd.to_datetime(data_min).date().isoformat())
    ss.setdefault("bf_end", pd.to_datetime(data_max).date().isoformat())
    ss.setdefault("filter_categories", cats_all.copy())
    ss.setdefault("bf_search", "")
    ss.setdefault("bf_min", 0.0)
    ss.setdefault("bf_max", None)

    # ---------- Форма фильтров ----------
    with st.form("browse_form", clear_on_submit=False):
        c1, c2 = st.columns(2)
        with c1:
            start_d = st.date_input(
                t("common.start", lang, default="Start"),
                value=pd.to_datetime(ss["bf_start"]).date(),
                min_value=pd.to_datetime(data_min).date(),
                max_value=pd.to_datetime(data_max).date(),
                key="bf_start_input",
            )
        with c2:
            end_d = st.date_input(
                t("common.end", lang, default="End"),
                value=pd.to_datetime(ss["bf_end"]).date(),
                min_value=pd.to_datetime(data_min).date(),
                max_value=pd.to_datetime(data_max).date(),
                key="bf_end_input",
            )

        # Тумблер: показывать только категории, реально встречающиеся в диапазоне
        df_range = base_df[
            (pd.to_datetime(base_df["date"]).dt.date >= start_d)
            & (pd.to_datetime(base_df["date"]).dt.date <= end_d)
        ]
        only_present = st.toggle(
            t("browse.only_present_cats", lang, default="Show only present categories"),
            value=True,
            help=t(
                "browse.only_present_cats_help",
                lang,
                default="Limit the list to categories found in the selected date range",
            ),
            key="bf_only_present",
        )

        if only_present and not df_range.empty:
            present_keys = sorted(
                str(x) for x in df_range["category"].dropna().astype(str).unique()
            )
            cat_options = present_keys
        else:
            cat_options = cats_all

        c3, c4 = st.columns(2)
        with c3:
            selected = st.multiselect(
                t("browse.category", lang, default="Category"),
                options=cat_options,
                default=[
                    c
                    for c in ss.get("filter_categories", cat_options)
                    if c in cat_options
                ]
                or cat_options,
                format_func=fmt,
            )
        with c4:
            search_q = st.text_input(
                t("browse.search", lang, default="Search (description or category)"),
                value=ss.get("bf_search", ""),
            )

        c5, c6 = st.columns(2)
        with c5:
            min_amt = st.number_input(
                t("browse.min_amount", lang, default="Min amount"),
                value=float(ss.get("bf_min") or 0.0),
                min_value=0.0,
                step=1.0,
            )
        with c6:
            max_amt = st.number_input(
                t("browse.max_amount", lang, default="Max amount"),
                value=(
                    float(ss["bf_max"])
                    if ss.get("bf_max") not in (None, "", 0.0)
                    else 0.0
                ),
                min_value=0.0,
                step=1.0,
                help=t("browse.max_amount_help", lang, default="0 means no limit"),
            )

        c7, c8 = st.columns(2)
        with c7:
            apply_clicked = st.form_submit_button(
                t("common.apply", lang, default="Apply")
            )
        with c8:
            reset_clicked = st.form_submit_button(
                t("common.reset", lang, default="Reset")
            )

    # ---------- Обработка кнопок ----------
    if apply_clicked:
        ss["bf_start"] = start_d.isoformat()
        ss["bf_end"] = end_d.isoformat()
        ss["filter_categories"] = selected or cat_options.copy()
        ss["bf_search"] = (search_q or "").strip()
        ss["bf_min"] = float(min_amt or 0.0)
        ss["bf_max"] = (
            None if (max_amt is None or float(max_amt) == 0.0) else float(max_amt)
        )
        bump_data_version()
        st.rerun()

    if reset_clicked:
        ss["bf_start"] = pd.to_datetime(data_min).date().isoformat()
        ss["bf_end"] = pd.to_datetime(data_max).date().isoformat()
        ss["filter_categories"] = cats_all.copy()
        ss["bf_search"] = ""
        ss["bf_min"] = 0.0
        ss["bf_max"] = None
        bump_data_version()
        st.rerun()

    # ---------- Построение фильтра ----------
    df_filtered = get_filtered_df_for_period(
        base_df,
        start=pd.to_datetime(ss["bf_start"]).date(),
        end=pd.to_datetime(ss["bf_end"]).date(),
        categories=ss.get("filter_categories"),
        search=ss.get(
            "bf_search"
        ),  # ← передаём, и функция ищет по category И description
        min_amount=float(ss.get("bf_min") or 0.0),
        max_amount=(
            None if ss.get("bf_max") in (None, "", 0.0) else float(ss["bf_max"])
        ),
    )

    # ---------- Метрики ----------
    total = float(df_filtered["amount"].sum()) if not df_filtered.empty else 0.0
    ops = int(len(df_filtered))
    avg = float(total / ops) if ops > 0 else 0.0

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric(t("browse.total", lang, default="Total"), f"{total:,.2f}")
    with c2:
        st.metric(t("browse.operations", lang, default="Operations"), f"{ops}")
    with c3:
        st.metric(t("browse.avg", lang, default="Average"), f"{avg:,.2f}")

    # ---------- Таблица ----------
    st.markdown("#### " + t("browse.results", lang, default="Filtered results"))
    if df_filtered.empty:
        st.info(t("common.no_data", lang, default="No data to display."))
    else:
        show_df = df_filtered[
            ["id", "date", "category", "amount", "description"]
        ].copy()
        show_df["category"] = show_df["category"].map(lambda k: fmt(k))
        st.dataframe(show_df, width="stretch", hide_index=True)


# ================ Charts ================
elif choice == "charts":
    # Заголовок и подпись
    st.subheader(t("menu.charts", lang, default="Charts"))
    st.caption(
        "📈 " + t("charts.placeholder", lang, default="Charts page (placeholder)")
    )
    #  render_flash()
    #  debug_hud("Charts/pre")

    lang = st.session_state.get("lang", "en")
    cats, cat_labels = categories_ui(lang)
    #  debug_hud("labels", extra={"labels_lang": lang, "labels_cnt": len(cat_labels)})

    def _fmt_cat(key: object) -> str:
        s = "" if key is None else str(key)
        return cat_labels.get(s, s)

    # 0) исходные данные (активная БД + те же фильтры, что и на Dashboard/Browse)
    db_path = st.session_state.get("ACTIVE_DB_PATH", "data/default_expenses.db")
    base_df = load_df(str(db_path), _ver=get_data_version()).copy()

    # приведение типов (на всякий случай)
    base_df["amount"] = pd.to_numeric(base_df["amount"], errors="coerce").fillna(0)
    base_df["date"] = pd.to_datetime(base_df["date"], errors="coerce")
    base_df = base_df.dropna(subset=["date"])

    # --- нормализация дат и фильтров для Charts/Browse (безопасно) ---

    def _coerce_date(v, fallback: date) -> date:
        """Преобразует str/date/datetime в date; при ошибке возвращает fallback."""
        if isinstance(v, date) and not isinstance(v, datetime):
            return v
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, str) and v.strip():
            ts = pd.to_datetime(v, errors="coerce")
            if pd.notna(ts):
                return ts.date()
        return fallback

    def _coerce_float(v):
        """str/float -> float | None, пустые строки и None превращаем в None."""
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if not s:
            return None
        try:
            return float(s)
        except Exception:
            return None

    # границы по данным для подстраховки
    data_min = pd.to_datetime(base_df["date"], errors="coerce").min()
    data_max = pd.to_datetime(base_df["date"], errors="coerce").max()
    data_min = data_min.date() if pd.notna(data_min) else date.today()
    data_max = data_max.date() if pd.notna(data_max) else date.today()

    ss = st.session_state

    # берём из dash_*, если их нет — из bf_*, иначе жёстко ограничиваемся по данным
    start_raw = ss.get("dash_start") or ss.get("bf_start")
    end_raw = ss.get("dash_end") or ss.get("bf_end")

    start_d = _coerce_date(start_raw, data_min)
    end_d = _coerce_date(end_raw, data_max)

    # приводим к остальным фильтрам
    categories = ss.get("filter_categories") or None
    search_s = (ss.get("bf_search") or "").strip()
    min_amt = _coerce_float(ss.get("bf_min"))
    max_amt = _coerce_float(ss.get("bf_max"))

    # итоговый срез (единый фильтр)
    ch_df = get_filtered_df_for_period(
        base_df,
        start=start_d,
        end=end_d,
        categories=categories,
        search=search_s,
        min_amount=min_amt,
        max_amount=max_amt,
    )

    # 1) агрегаты — БЕЗ масштабирования /100
    # --- By category --------------------------------------------------------------
    if not ch_df.empty:
        bar_df = ch_df.groupby("category", dropna=False, as_index=False).agg(
            total=("amount", "sum")
        )
    else:
        bar_df = pd.DataFrame({"category": [], "total": []})

    # подписи категорий (локализация)
    bar_df["cat_label"] = bar_df["category"].map(_fmt_cat)

    # --- By date (дневная агрегация) ---------------------------------------------
    if not ch_df.empty:
        line_df = (
            ch_df.assign(date=ch_df["date"].dt.floor("D"))
            .groupby("date", as_index=False)
            .agg(total=("amount", "sum"))
        )
    else:
        line_df = pd.DataFrame({"date": [], "total": []})

    # 3) sanity-check на время отладки (можно потом удалить)
    try:
        if not ch_df.empty:
            total_raw = float(ch_df["amount"].sum())
            total_chart = float(bar_df["total"].sum())
            if abs(total_chart - total_raw) > 1e-6:
                st.warning(
                    "Charts total != raw total (проверь масштабирование/фильтры)"
                )
    except Exception:
        pass

    # 4) Zoom selectors (Altair 4/5 compatible)
    # Подключаемся до блока визуализаций (bar/line), один раз на страницу.

    # Altair 5 использует add_params; Altair 4 — add_selection.
    try:
        zoom_y = alt.selection_interval(
            bind="scales", encodings=["y"]
        )  # зум по Y (бар-чарт)
        zoom_xy = alt.selection_interval(bind="scales")  # зум по X+Y (линейный)
        _ALT_USES_PARAMS = True
    except Exception:
        zoom_y = alt.selection(type="interval", bind="scales", encodings=["y"])
        zoom_xy = alt.selection(type="interval", bind="scales")
        _ALT_USES_PARAMS = False

    def _attach_zoom(chart, sel):
        """Кросс-версионная привязка zoom-selection для Altair 4/5."""
        try:
            return (
                chart.add_params(sel) if _ALT_USES_PARAMS else chart.add_selection(sel)
            )
        except Exception:
            try:
                return chart.add_params(sel)
            except Exception:
                return chart.add_selection(sel)

    # 2) ВИЗУАЛИЗАЦИИ
    CHART_H = 320  # единая высота для всех графиков

    # --- Тумблеры ---
    enable_zoom = st.toggle(
        t("charts.enable_zoom", lang, default="🔍 Enable zoom"),
        value=True,
        help=t("charts.enable_zoom_help", lang, default="Scroll to zoom, drag to pan"),
        key="charts_enable_zoom",
    )

    enable_logy = (
        st.toggle(
            t("charts.enable_logy", lang, default="Logarithmic Y scale"),
            value=False,
            help=t(
                "charts.enable_logy_help",
                lang,
                default="Affects the 'BY date' chart only",
            ),
            key="charts_enable_logy",
        ),
    )

    # --- Вспомогательная функция для выбора шкалы ---
    def _mk_y_scale(df: pd.DataFrame) -> alt.Scale:
        use_log = bool(st.session_state.get("charts_enable_logy", False))
        if use_log and not df.empty:
            vals = pd.to_numeric(df["total"], errors="coerce")
            if (vals > 0).any():
                # clamp=True — отсекаем случайные отрицательные, nice=False — без «красивых» границ (чтобы не прыгало)
                return alt.Scale(type="log", clamp=True, nice=False)
        return alt.Scale(type="linear")

    def _y_field_and_df(
        df: pd.DataFrame, field: str = "total"
    ) -> tuple[str, pd.DataFrame]:
        """
        Для лог-шкалы заменяем значения <= 0 на NaN и убеждаемся, что тип float.
        Если после фильтрации не осталось положительных значений — возвращаем исходное поле.
        """
        use_log = bool(st.session_state.get("charts_enable_logy", False))
        if not use_log or df.empty:
            return f"{field}:Q", df

        vals = pd.to_numeric(df[field], errors="coerce")
        if not (vals > 0).any():
            # Нет положительных значений -> лог не имеет смысла
            return f"{field}:Q", df

        df2 = df.copy()
        safe_col = f"{field}_pos"
        # принудительно float и NaN для неположительных
        df2[safe_col] = pd.to_numeric(df2[field], errors="coerce").astype(float)
        df2[safe_col] = df2[safe_col].where(df2[safe_col] > 0, np.nan)
        return f"{safe_col}:Q", df2

    # =====================================================================
    # By category
    # =====================================================================
    st.markdown("#### " + t("dashboard.by_category", lang, default="By category"))

    if not bar_df.empty:
        bar = (
            alt.Chart(bar_df)
            .mark_bar()
            .encode(
                x=alt.X(
                    "cat_label:N", title=t("col.category", lang, default="Category")
                ),
                y=alt.Y(
                    "total:Q",
                    title=t("kpi.total", lang, default="Total"),
                    axis=alt.Axis(format=",.2f"),
                    scale=alt.Scale(type="linear"),  # всегда линейная для бар-чарта
                ),
                tooltip=[
                    alt.Tooltip(
                        "cat_label:N", title=t("col.category", lang, default="Category")
                    ),
                    alt.Tooltip(
                        "total:Q",
                        title=t("kpi.total", lang, default="Total"),
                        format=",.2f",
                    ),
                ],
            )
            .properties(height=CHART_H, width="container")
        )
        if st.session_state.get("charts_enable_zoom", True):
            bar = _attach_zoom(bar, zoom_y)
        st.altair_chart(bar)
    else:
        st.info(t("common.no_data", lang, default="No data to display."))

    # =====================================================================
    # By date
    # =====================================================================
    st.markdown("#### " + t("charts.by_date", lang, default="By date"))

    if not line_df.empty:
        # адаптивный формат подписей оси X
        _span_days = (
            (end_d - start_d).days
            if "end_d" in locals() and "start_d" in locals()
            else 9999
        )
        if _span_days <= 60:
            _label_expr = _vega_day_month_expr(lang)  # '12 sep'
            _tick_count = 8
        else:
            _label_expr = _vega_month_with_year_on_jan_expr(
                lang
            )  # 'jan 2024', 'feb', ...
            _tick_count = 10

        # подготовка поля Y с учётом лог-шкалы (если включена)
        y_field_line, line_src = _y_field_and_df(line_df)

        line = (
            alt.Chart(line_src)
            .mark_line(point=True)
            .encode(
                x=alt.X(
                    "date:T",
                    title=t("charts.date", lang, default="Date"),
                    axis=alt.Axis(
                        labelAngle=-30,
                        labelExpr=_label_expr,
                        tickCount=_tick_count,
                        labelOverlap="greedy",
                    ),
                ),
                y=alt.Y(
                    y_field_line,
                    title=t("kpi.total", lang, default="Total"),
                    axis=alt.Axis(format=",.2f"),
                    scale=_mk_y_scale(line_df),
                ),
                tooltip=[
                    alt.Tooltip("date:T", title=t("charts.date", lang, default="Date")),
                    alt.Tooltip(
                        "total:Q",
                        title=t("kpi.total", lang, default="Total"),
                        format=",.2f",
                    ),
                ],
            )
            .properties(height=CHART_H, width="container")
        )

        if st.session_state.get("charts_enable_zoom", True):
            line = _attach_zoom(line, zoom_xy)
        st.altair_chart(line)
    else:
        st.info(t("common.no_data", lang, default="No data to display."))

    # ---------- Экспандер: круговая по категориям ----------
    with st.expander(
        t("charts.share_by_category_pie", lang, default="Share by category (pie)"),
        expanded=False,
    ):
        pie_df = (
            ch_df.groupby("category", dropna=False)["amount"]
            .sum()
            .rename("amount")
            .reset_index()
            if not ch_df.empty
            else pd.DataFrame({"category": [], "amount": []})
        )

        # добавляем локализованное поле
        if not pie_df.empty:
            pie_df["cat_label"] = pie_df["category"].map(_fmt_cat)

        if not pie_df.empty:
            chart = (
                alt.Chart(pie_df)
                .mark_arc()
                .encode(
                    theta=alt.Theta("amount:Q"),
                    color=alt.Color(
                        "cat_label:N",
                        legend=alt.Legend(
                            title=t("col.category", lang, default="Category")
                        ),
                    ),
                    tooltip=[
                        alt.Tooltip(
                            "cat_label:N",
                            title=t("col.category", lang, default="Category"),
                        ),
                        alt.Tooltip(
                            "amount:Q",
                            title=t("kpi.total", lang, default="Total"),
                            format=",.2f",
                        ),
                    ],
                )
                .properties(height=300)
            )
            chart = chart.properties(height=300, width="container")
            st.altair_chart(chart)
        else:
            st.info(t("common.no_data", lang, default="No data to display."))

    # ---------- Экспандер: показать данные ----------
    with st.expander(t("charts.show_data", lang, default="Show data"), expanded=False):
        ch_show = _localize_category_column(ch_df, cat_labels)
        if not ch_show.empty:
            ch_show["date"] = pd.to_datetime(
                ch_show["date"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")
        st.dataframe(
            ch_show,
            width="stretch",
            hide_index=True,
            column_config={
                "amount": st.column_config.NumberColumn(
                    t("col.amount", lang, default="Amount"), format="%.2f"
                ),
                "date": st.column_config.DatetimeColumn(
                    t("col.date", lang, default="Date"), format="YYYY-MM-DD"
                ),
            },
        )

# ================= Settings =================
elif choice == "settings":
    st.header(t("menu.settings", lang, default="Settings"))
    render_flash()

    # текущий язык (по умолчанию en)
    langs = ["en", "fr", "es"]
    current_lang = st.session_state.get("lang", "en")
    idx = langs.index(current_lang) if current_lang in langs else 0

    # селектор языка
    new_lang = st.selectbox(
        t("settings.language", current_lang, default="Language"),
        options=langs,
        index=idx,
        key="sidebar_lang_select",
    )

    # сохраняем только если изменили
    if new_lang != current_lang:
        st.session_state["lang"] = new_lang
        st.toast(t("language_switched", new_lang, default="Language switched"))
        st.rerun()

# =================== /User / Profile ===================


def limits_path(user: str) -> Path:
    return DATA_DIR / f"{user}_budget_limits.json"


def list_users() -> list[str]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    users = sorted(
        p.name.replace("_expenses.db", "") for p in DATA_DIR.glob("*_expenses.db")
    )
    # если совсем пусто — гарантируем default
    return users or ["default"]


def files_for(user: str) -> tuple[Path, Path]:
    return Path(get_db_path(user)), limits_path(user)


def archive_user(user: str) -> Path:
    """Перемещает файлы юзера в архивную папку и возвращает путь архива."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    arch_dir = DATA_DIR / "archives" / f"{user}_{ts}"
    arch_dir.mkdir(parents=True, exist_ok=True)
    dbf, limf = files_for(user)
    if dbf.exists():
        dbf.rename(arch_dir / dbf.name)
    if limf.exists():
        limf.rename(arch_dir / limf.name)
    return arch_dir


def rename_user(old: str, new: str) -> None:
    """Переименовать файлы пользователя (если есть)."""
    if old == new:
        return
    src_db, src_lim = files_for(old)
    dst_db, dst_lim = files_for(new)
    # защита от перезаписи
    if dst_db.exists() or dst_lim.exists():
        raise FileExistsError("User with this name already exists.")
    if src_db.exists():
        src_db.rename(dst_db)
    if src_lim.exists():
        src_lim.rename(dst_lim)


def switch_user(user: str, toast: str = "Switched"):
    st.session_state["current_user"] = user
    st.session_state["_flash"] = (f"{toast} to '{user}'", "🆕")
    st.rerun()


# ---- UI ----
st.subheader(t("profile.title", lang, default="User / Profile"))

# текущее значение
current = st.session_state.setdefault("current_user", "default")
users = list_users()
if current not in users:
    current = users[0]
    st.session_state["current_user"] = current

# первая строка: Active user + Create / rename user + Create
c1, c2, c3 = st.columns([1.2, 1.2, 0.6])

with c1:
    sel = st.selectbox(
        t("profile.active_user", lang, default="Active user"),
        users,
        index=users.index(current),
        key="settings_active_user",
    )

with c2:
    new_name = st.text_input(
        t("profile.create_rename_user", lang, default="Create / rename user"),
        value="",
        placeholder=t("profile.type_name", lang, default="Type name"),
        key="settings_new_name",
    ).strip()

with c3:
    if st.button(
        t("buttons.create", lang, default="Create"), key="settings_btn_create"
    ):
        if not new_name:
            st.warning(
                t("errors.warning_enter_name", lang, default="Please enter a name.")
            )
        elif new_name in users:
            st.warning(
                t(
                    "errors.warning_user_name_exists",
                    lang,
                    default="User with this name already exists.",
                )
            )
        else:
            ensure_schema(get_db_path(new_name))
            switch_user(
                new_name,
                toast=t(
                    "profile.toast_created_switched", lang, default="Created & switched"
                ),
            )
            st.cache_data.clear()
            bump_data_version()
            st.rerun()

# подпись с файлами активного пользователя
dbf, limf = files_for(sel)
st.caption(f"DB:  {dbf.name}  —  Limits:  {limf.name}")

# вторая строка: архивирование + Delete + Rename
c4, c5, c6 = st.columns([0.9, 0.7, 0.7])

with c4:
    do_archive = st.checkbox(
        t("profile.archive_before_delete", lang, default="Archive before delete"),
        value=True,
        key="settings_archive_before_delete",
    )

with c5:
    disable_delete = len(users) <= 1
    delete_help = t(
        "profile.cannot_delete_last",
        lang,
        default="You cannot delete the last remaining user.",
    )

    if disable_delete:
        st.caption(f"ℹ️ {delete_help}")

    if st.button(
        t("profile.delete_user", lang, default="Delete user"),
        disabled=disable_delete,
        help=delete_help,  # подсказка при наведении
        key="settings_btn_delete",
    ):
        if disable_delete:
            st.info(
                t(
                    "profile.cannot_delete_last",
                    lang,
                    default="You cannot delete the last remaining user.",
                )
            )
        else:
            try:
                if do_archive:
                    archive_user(sel)
                else:
                    if dbf.exists():
                        dbf.unlink()
                    if limf.exists():
                        limf.unlink()
                remaining = [u for u in list_users() if u != sel]
                switch_user(
                    remaining[0] if remaining else "default",
                    toast=t(
                        "profile.toast_deleted_switched",
                        lang,
                        default="Deleted, switched",
                    ),
                )
                st.cache_data.clear()
                bump_data_version()
                st.rerun()
            except Exception as e:
                st.error(t("profile.deletion_failed", lang, default="Deletion failed."))
                st.exception(e)

with c6:
    if st.button(
        t("profile.rename", lang, default="Rename"), key="settings_btn_rename"
    ):
        if not new_name:
            st.warning(
                t(
                    "errors.warning_enter_new_name",
                    lang,
                    default="Please enter a new name.",
                )
            )
        elif new_name in users:
            st.warning(
                t(
                    "errors.warning_user_name_exists",
                    lang,
                    default="User with this name already exists.",
                )
            )
        else:
            try:
                rename_user(sel, new_name)
                switch_user(
                    new_name,
                    toast=t(
                        "profile.toast_renamed_switched",
                        lang,
                        default="Renamed & switched",
                    ),
                )
                st.cache_data.clear()
                bump_data_version()
                st.rerun()
            except Exception as e:
                st.error(t("profile.rename_failed", lang, default="Rename failed."))
                st.exception(e)

# быстрый свитч, если пользователь в select изменён
if sel != current and st.session_state.get("settings_active_user") == sel:
    switch_user(sel, toast=t("profile.toast_switched", lang, default="Switched"))
    st.cache_data.clear()
    bump_data_version()
    st.rerun()

# --- Monthly limits ----------------------------------------------------------


def _active_user() -> str:
    return get_active_user()  # твоя функция


# Активные пути: DB как str, limits как Path (без внешних зависимостей)
def _active_paths() -> tuple[str, Path]:
    user = _active_user()
    db_path_str = get_db_path(user)  # ← снова получаем реальный путь к БД
    limits_file = Path("data") / f"{user}_budget_limits.json"
    return db_path_str, limits_file


# Надёжный список категорий (даже если БД пустая)
def _categories_for_editor(db_path: str) -> list[str]:
    try:
        cats = list_categories(db_path=db_path) or []
    except Exception:
        cats = []
    base = {"food", "transport", "groceries", "utilities", "entertainment", "other"}
    return sorted(set(cats) | base)


# Ключ месяца
def _mk(d: date) -> str:
    return month_key(d)


# Чтение/сохранение лимитов JSON
def _load_limits(mk: str, path: Path) -> dict[str, float]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        raw = data.get(mk, {}) or {}
        return {k: float(v) for k, v in raw.items()}
    except Exception:
        return {}


def _save_limits(mk: str, values: dict[str, float], path: Path) -> None:
    data: dict = {}
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8")) or {}
        except Exception:
            data = {}
    data[mk] = values
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def current_limits_month() -> str:
    """Возвращает месяц для блока Limits в формате YYYY-MM."""
    src = (
        st.session_state.get("limits_month")  # если выбран месяц в UI
        or st.session_state.get("dash_start")  # иначе дата старта Dashboard
        or date.today().replace(day=1)  # fallback: первый день текущего месяца
    )
    if isinstance(src, str):
        try:
            src = datetime.fromisoformat(src).date()
        except ValueError:
            src = date.today().replace(day=1)
    return src.strftime("%Y-%m")


# ---------------- UI ----------------

st.subheader(t("limits.monthly_title", lang, default="Monthly limits"))

# Показываем активные файлы
db_path_str, limits_file = _active_paths()
st.caption(f"DB: {db_path_str} — Limits: {limits_file.name}")

# 1) Выбор месяца (уникальный key обязателен)
month = st.date_input(
    t("limits.month", lang, default="Month"),
    value=date.today().replace(day=1),
    format="YYYY/MM/DD",
    key="limits_month",
)
mk = _mk(month)

# 2) Загружаем лимиты и категории
cats = _categories_for_editor(db_path_str)
limits_now = _load_limits(mk, limits_file)

# 🆕 Локализация подписей категорий для UI + сортировка по переводу
_, cat_labels = categories_ui(lang)  # уже есть в проекте, словарь {key -> label}
# гарантируем подписи для всех cats (на случай редких ключей)
for c in cats:
    cat_labels.setdefault(c, t(f"categories.{c}", lang, default=c))
cats = sorted(cats, key=lambda c: cat_labels[c].lower())

# 3) Редактор лимитов
user = current_user()
ym = current_limits_month()

st.write(
    f"{t('profile.title', lang, default='User / Profile').split(' / ')[0]}: {user} • "
    f"{t('limits.month', lang, default='Month')}: {ym}"
)

values: dict[str, float] = {}
for cat in cats:
    # 🆕 подпись поля — локализованная
    label = cat_labels.get(cat, cat)
    values[cat] = st.number_input(
        label,
        min_value=0.0,
        step=10.0,
        value=float(limits_now.get(cat, 0.0)),
        key=f"limit_{ym}_{cat}",
    )

# 4) Кнопки управления (Save / Clear)
col1, col2 = st.columns(2)
with col1:
    if st.button(t("buttons.save", lang, default="Save"), key=f"save_limits_{mk}"):
        _save_limits(mk, values, limits_file)
        st.session_state["_flash"] = (t("saved", lang, default="Saved!"), "✅")
        st.cache_data.clear()
        st.rerun()

with col2:
    if st.button(
        t("limits.clear_month", lang, default="Clear month limits"),
        key=f"clear_limits_{mk}",
    ):
        _save_limits(mk, {}, limits_file)
        st.session_state["_flash"] = ("Limits cleared", "🗑️")
        st.cache_data.clear()
        st.rerun()


# --- Import / Export CSV ------------------------------------------------------
mk = st.session_state.get("current_limits_month", month_key(date.today()))
current_user = st.session_state.get("current_user", "default")

# соберём текущие значения из формы (микро-хелпер вы уже добавили ранее)
current_limits = _collect_limits_from_form(prefix=f"limit_{mk}_")

exp_col1, exp_col2 = st.columns(2)

# --- Export CSV
with exp_col1:
    csv_bytes = limits_to_csv_bytes(current_limits)
    st.download_button(
        label=t("download_csv", lang, default="Download CSV"),
        data=csv_bytes,
        file_name=f"{current_user}_{mk}_limits.csv",
        mime="text/csv",
        key=f"dl_limits_csv_{current_user}_{mk}",
        help=t("download_csv", lang, default="Download CSV"),
    )

# --- Import CSV
with exp_col2:
    up = st.file_uploader(
        t("limits.import_csv", lang, default="Upload CSV"),
        type=["csv"],
        key=f"ul_limits_csv_{current_user}_{mk}",
        help=t("limits.import", lang, default="Upload CSV"),
    )

    if up is not None:
        try:
            uploaded_bytes = up.read()
            imported_limits = csv_bytes_to_limits(uploaded_bytes)

            # проставляем в поля редактора
            for cat, val in imported_limits.items():
                st.session_state[f"limit_{mk}_{cat}"] = float(val)

            # ЛОГ ИЗМЕНЕНИЙ: сравниваем «что было в форме» vs «что импортировали»
            append_audit_row(old=current_limits, new=imported_limits)

            # уведомление + мягкий rerun
            st.session_state["_flash"] = (t("saved", lang, default="Saved!"), "✅")
            st.cache_data.clear()
            st.rerun()

        except Exception:
            st.error(t("csv_import_failed", lang, default="CSV import failed"))

# ---- Change log (session) ----------------------------------------------------
st.markdown(f"#### {t('change_log', lang, default='Change log (session)')}")

log_col1, log_col2, log_col3, log_col4 = st.columns(4)

audit_data = get_audit()  # список записей аудита за сессию

with log_col1:
    st.download_button(
        label=t("download_json", lang, default="Download JSON"),
        data=audit_to_json_bytes(audit_data),
        file_name=f"audit_{current_user}_{mk}.json",
        mime="application/json",
        key=f"dl_audit_json_{current_user}_{mk}",
    )

with log_col2:
    st.download_button(
        label=t("download_csv", lang, default="Download CSV"),
        data=audit_to_csv_bytes(audit_data),
        file_name=f"audit_{current_user}_{mk}.csv",
        mime="text/csv",
        key=f"dl_audit_csv_{current_user}_{mk}",
    )

with log_col4:
    if st.button(
        t("clear_audit", lang, default="Clear audit"),
        key=f"btn_clear_audit_{current_user}_{mk}",
    ):
        st.session_state.setdefault("__limits_audit__", [])
        st.session_state["__limits_audit__"].clear()
        st.success(t("cleared", lang, default="Cleared!"))

# 6) Suggestions (last 3 months) — локализуемый и аккуратный
with st.expander(t("suggestions", lang, default="Suggestions (last 3 months)")):
    df_hist = get_expenses_df(db_path=str(db_path_str))
    if df_hist is None or df_hist.empty:
        st.caption(t("suggestions.none", lang, default="No suggestions yet."))
    else:
        # -- последние 3 «YYYY-MM» от максимальной даты в БД --
        df_hist["date"] = pd.to_datetime(df_hist["date"])
        df_hist["ym"] = df_hist["date"].dt.strftime("%Y-%m")
        last_ym = df_hist.sort_values("date")["ym"].drop_duplicates().tail(3).tolist()
        if not last_ym:
            st.caption(t("suggestions.none", lang, default="No suggestions yet."))
        else:
            recent = df_hist[df_hist["ym"].isin(last_ym)].copy()

            # -- сводная: сумма по категориям за месяц, затем среднее за 3м --
            piv = (
                recent.pivot_table(
                    index="ym", columns="category", values="amount", aggfunc="sum"
                )
                .sort_index()
                .fillna(0.0)
            )
            avg3: pd.Series = piv.tail(3).mean(axis=0)  # среднее по категориям

            # -- лимиты выбранного в UI месяца (или текущего при ошибке) --
            try:
                month_str = current_limits_month()  # "YYYY-MM"
                month_dt = date.fromisoformat(month_str + "-01")
            except Exception:
                month_dt = date.today().replace(day=1)

            user = st.session_state.get("current_user", "default")
            lims = get_month_limits_dict(
                month=month_dt, lang=lang, user=user
            )  # {cat: limit}

            # пороги: ↑ если среднее > лимита на 10%, ↓ если лимит > среднего на 25%
            UP_K = 1.10
            DN_K = 1.25

            # множество всех категорий, встречающихся в avg3 или имеющихся в лимитах
            cats_all = sorted(set(avg3.index.tolist()) | set(lims.keys()))

            recs: list[tuple[float, str]] = []  # (|дельта|, строка)
            for cat in cats_all:
                a = float(avg3.get(cat, 0.0))  # средний расход за 3м
                L = float(lims.get(cat, 0.0))  # лимит на выбранный месяц

                # если совсем пусто — пропускаем
                if a == 0 and L == 0:
                    continue

                # ↑ предложить увеличить лимит
                if a > max(0.0, L) * UP_K:
                    diff = a - L  # на сколько не хватает лимита
                    rec_line = t(
                        "suggestions.row_up",
                        lang,
                        default="{cat}: avg last 3 mo {avg:.2f} > limit {lim:.2f} → consider +{delta:.2f}",
                    ).format(cat=cat, avg=a, lim=L, delta=diff)
                    recs.append((abs(diff), rec_line))

                # ↓ предложить уменьшить лимит
                elif L > 0 and L > a * DN_K:
                    diff = L - a  # на сколько лимит превышает среднее
                    rec_line = t(
                        "suggestions.row_down",
                        lang,
                        default="{cat}: limit {lim:.2f} >> avg {avg:.2f} → consider -{delta:.2f}",
                    ).format(cat=cat, avg=a, lim=L, delta=diff)
                    recs.append((abs(diff), rec_line))

            if recs:
                # сортируем по силе рекомендации (по убыванию дельты)
                for _, line in sorted(recs, key=lambda x: x[0], reverse=True):
                    st.write("• " + line)
            else:
                st.caption(t("suggestions.none", lang, default="No suggestions yet."))
