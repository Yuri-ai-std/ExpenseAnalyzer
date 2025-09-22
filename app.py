import json
import os
import sqlite3
from datetime import date
from datetime import date as _date
from datetime import datetime
from functools import partial
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Dict, Tuple, cast

import altair as alt
import pandas as pd
import streamlit as st

from db import (
    _limits_path_for_db,
    add_expense,
    ensure_db,
    ensure_limits_file,
    get_db_path,
    get_expenses_df,
    list_categories,
)
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


ACTIVE_DB_PATH = db_path_for(current_user())  # data/default_expenses.db
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
DEBUG_HUD = False  # True — чтобы видеть отладочные строки на странице

# делаем пути видимыми для других модулей через session_state
st.session_state["ACTIVE_DB_PATH"] = ACTIVE_DB_PATH
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


def export_df_to_excel_button(df: pd.DataFrame, filename: str = "expenses.xlsx"):
    if df.empty:
        st.info("Нет данных для экспорта.")
        return

    # Пытаемся выбрать доступный движок
    engine = None
    try:
        import xlsxwriter  # noqa: F401

        engine = "xlsxwriter"
    except ImportError:
        try:
            import openpyxl  # noqa: F401

            engine = "openpyxl"
        except ImportError:
            st.error(
                "Для экспорта в Excel установите один из пакетов: "
                "`pip install XlsxWriter` или `pip install openpyxl`."
            )
            return

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine=engine) as writer:
        df.to_excel(writer, index=False, sheet_name="Expenses")

    st.download_button(
        label="⬇️ Download Excel",
        data=buf.getvalue(),
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


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


def _collect_limits_for_month(mk: str, categories: list[str]) -> dict[str, float]:
    """Собирает текущие значения из полей редактирования в st.session_state."""
    out: dict[str, float] = {}
    for cat in categories:
        raw = st.session_state.get(f"limit_{mk}_{cat}")
        try:
            out[cat] = float(raw) if raw is not None else 0.0
        except Exception:
            out[cat] = 0.0
    return out


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


def render_table(
    df, cols, lang: str, *, labels: dict[str, str] | None = None, **st_kwargs
):
    df_disp = df.loc[:, cols]
    if labels is not None:
        df_disp = _localize_category_column(df_disp, labels)
    df_disp = df_disp.rename(columns=_col_labels(lang))
    st.dataframe(df_disp, **st_kwargs)


def render_recent_expenses_table(
    db_path: str, n: int = 10, *, show_title: bool = False, lang: str = "en"
) -> None:
    if show_title:
        st.subheader(t("recent_expenses", lang, default="Recent expenses"))

    raw_df = get_expenses_df(db_path=db_path)
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
    df = get_expenses_df(db_path, start_date=start, end_date=end)

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


def make_fmt(labels: dict[str, str]) -> LabelFn:
    """Возвращает безопасный форматтер: key -> локализованный label (str)."""

    def _fmt(v: Any) -> str:
        s = "" if v is None else str(v)
        return labels.get(s, s)

    return _fmt


def get_filtered_df_for_period(
    base_df: pd.DataFrame,
    start: date,
    end: date,
    categories: list[str] | None = None,
    search: str | None = None,
    min_amount: float | None = None,
    max_amount: float | None = None,
) -> pd.DataFrame:
    df = base_df.copy()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[(df["date"].dt.date >= start) & (df["date"].dt.date <= end)]
    if categories:
        df = df[df["category"].isin(categories)]
    if search:
        s = search.strip().lower()
        if s:
            df = df[df["description"].astype(str).str.lower().str.contains(s, na=False)]
    if min_amount is not None:
        df = df[df["amount"] >= float(min_amount)]
    if max_amount is not None:
        df = df[df["amount"] <= float(max_amount)]
    return df


def get_expenses_view(
    *, db_path: str, start: str, end: str, lang: str, newest_first: bool = True
) -> Tuple[pd.DataFrame, LabelFn, dict[str, str]]:
    """
    Единый пайплайн данных для всех страниц:
      1) load_df(..., _ver=get_data_version())
      2) prepare_df_for_view(..., remove_dups=True, newest_first=...)
      3) локализация КОЛОНКИ 'category' (отображение; ключи в БД не трогаем)

    Возвращает:
      df  — нормализованные операции с локализованной колонкой 'category'
      fmt — форматтер для категорий (Any -> str)
      labels — словарь {key -> label}
    """
    ver = get_data_version()
    raw = load_df(db_path, start, end, _ver=ver)
    df = prepare_df_for_view(raw, remove_dups=True, newest_first=newest_first)

    _, labels = categories_ui(lang)  # labels: {тех. ключ -> локализованный label}
    fmt = make_fmt(labels)

    if "category" in df.columns:
        df = df.copy()
        df["category"] = df["category"].map(fmt)

    return df, fmt, labels


# ---- язык интерфейса ----
st.session_state.setdefault("lang", "en")
lang = st.session_state["lang"]

# чтение лимитов пользователя
limits = load_monthly_limits(user=current_user())

# ...изменили словарь limits на форме...

# сохранение лимитов пользователя
save_monthly_limits(limits, user=current_user())


def _fetch_categories() -> list[str]:
    # 1) если есть list_categories в db.py — используйте его
    try:
        from db import (
            list_categories as _list_categories,
        )  # локальный импорт на случай отсутствия

        cats = _list_categories()
        if cats:
            return cats
    except Exception:
        pass
    # 2) иначе соберём уникальные категории из БД
    try:
        db_path = st.session_state.get("ACTIVE_DB_PATH", "data/default_expenses.db")
        df = get_expenses_df(db_path=db_path)
        if "category" in df.columns and not df.empty:
            return sorted(map(str, df["category"].dropna().unique().tolist()))
    except Exception:
        pass
    # 3) дефолт
    return ["food", "transport", "health", "entertainment", "other"]


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


def render_add_expense_page(lang: str) -> None:
    ss = st.session_state
    user = current_user()  # получаем имя пользователя
    keys = add_form_keys(user)  # генерируем ключи формы
    apply_form_reset(keys)  # сброс формы для этого пользователя


def cat_label_fn_factory(labels: dict[str, str]) -> LabelFn:
    def fmt(key: Any) -> str:  # <<< имя параметра "key", как ожидает тип
        s = "" if key is None else str(key)
        return labels.get(s, s)

    return fmt


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
    if DEBUG_HUD:
        debug_hud("Dashboard/pre")

    lang = st.session_state.get("lang", "en")
    cats, cat_labels = categories_ui(lang)
    fmt = make_category_formatter(cat_labels)

    # Загружаем базовый датафрейм
    base_df = load_df(
        st.session_state.get("ACTIVE_DB_PATH", "data/default_expenses.db"),
        _ver=get_data_version(),
    )

    # ===== Фильтры по дате =====
    today = date.today()
    month_start = today.replace(day=1)

    # 1) Хранимые значения фильтров в session_state (строки 'YYYY-MM-DD')
    if "dash_start" not in st.session_state:
        st.session_state["dash_start"] = month_start.isoformat()
    if "dash_end" not in st.session_state:
        st.session_state["dash_end"] = today.isoformat()

    # >>> ЕДИНЫЙ фильтр данных для Dashboard (совместим с Browse) <<<
    df_filtered = get_filtered_df_for_period(
        base_df,
        start=pd.to_datetime(st.session_state["dash_start"]).date(),
        end=pd.to_datetime(st.session_state["dash_end"]).date(),
        # на Dashboard без category/search/min/max — показываем всё за период
    )

    # 2) Виджеты используют другие ключи, чтобы не конфликтовать с session_state
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

    # 3) При нажатии Apply переносим значения из виджетов в хранилище
    # и мягко перерисовываем страницу
    if refresh:
        st.session_state["dash_start"] = start_d.isoformat()
        st.session_state["dash_end"] = end_d.isoformat()
        st.session_state["_flash"] = (
            t("dashboard.filters_applied", lang, default="Filters applied"),
            "⚙️",
        )
        st.rerun()

    # 4) Строки для загрузки данных
    start_s = st.session_state["dash_start"]  # 'YYYY-MM-DD'
    end_s = st.session_state["dash_end"]  # 'YYYY-MM-DD'

    # ---- Данные ----
    lang = st.session_state.get("lang", "en")
    db_path = str(st.session_state.get("ACTIVE_DB_PATH", "data/default_expenses.db"))

    st.cache_data.clear()  # только для диагностики, потом уберём
    raw_df = load_df(db_path, start_s, end_s, _ver=get_data_version())
    if DEBUG_HUD:
        debug_hud("Dashboard/post", raw_df)

    if raw_df.empty:
        st.info(
            t(
                "no_expenses_found",
                lang,
                default="No expenses found for selected period.",
            )
        )
        st.stop()

    # Очистка и сортировка через хелпер
    df = prepare_df_for_view(raw_df, remove_dups=True, newest_first=True)
    if DEBUG_HUD:
        debug_hud("Dashboard/clean", df)

    # ===== KPI =====
    total = float(df["amount"].sum()) if not df.empty else 0.0
    count = int(len(df))
    avg = float(df["amount"].mean()) if count else 0.0
    cats = int(df["category"].nunique()) if not df.empty else 0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric(t("kpi.total", lang, default="Total"), f"{total:.2f}")
    k2.metric(t("kpi.operations", lang, default="Operations"), f"{count}")
    k3.metric(t("kpi.average", lang, default="Average"), f"{avg:.2f}")
    k4.metric(t("kpi.categories", lang, default="Categories"), f"{cats}")
    st.divider()

    # ===== Last operations (TOP N) =====
    N_LAST = 10
    st.subheader(t("dashboard.last_operations", lang, default="Last operations"))

    last_ops = df.sort_values(["date"], ascending=False).head(N_LAST).copy()
    last_ops["date"] = pd.to_datetime(last_ops["date"]).dt.strftime("%Y-%m-%d")
    last_ops["category"] = last_ops["category"].map(fmt)

    st.dataframe(
        last_ops[["date", "category", "amount", "description"]],
        use_container_width=True,
        hide_index=True,
        height=220,  # даёт скролл в блоке
        column_config={"amount": st.column_config.NumberColumn(format="%.2f")},
    )

    # ===== All expenses in period (полный список) =====
    st.subheader(
        t("dashboard.recent_expenses_full", lang, default="All expenses in period")
    )

    all_exp = df.copy()
    all_exp["date"] = pd.to_datetime(all_exp["date"]).dt.strftime("%Y-%m-%d")
    all_exp["category"] = all_exp["category"].map(fmt)

    st.dataframe(
        all_exp[["id", "date", "category", "amount", "description"]],
        use_container_width=True,
        hide_index=True,
        height=420,  # прокручиваемый полный список
        column_config={"amount": st.column_config.NumberColumn(format="%.2f")},
    )

    # ----- Диаграмма по категориям -----
    st.subheader(t("dashboard.by_category", lang, default="By category"))
    cat_totals = (
        df.groupby("category", dropna=False)["amount"]
        .sum()
        .sort_values(ascending=False)
        .rename("total")
        .to_frame()
    )
    # локализуем подписи категорий в индексе
    cat_totals_local = cat_totals.copy()
    cat_totals_local.index = cat_totals_local.index.map(fmt)

    st.bar_chart(cat_totals_local, use_container_width=True)

    # ----- Последние операции -----
    render_recent_expenses_table(ACTIVE_DB_PATH, n=10, show_title=True, lang=lang)

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

    # ---- таблица последних записей (как было у вас) ----
    render_recent_expenses_table(ACTIVE_DB_PATH, n=10, show_title=False, lang=lang)

# ===== Browse & Filter (новый каркас) =====
elif choice == "browse":
    st.subheader(t("menu.browse", lang, default="Browse & Filter"))
    st.caption(
        "🔎 "
        + t("browse.placeholder", lang, default="Page Browse & Filter (placeholder)")
    )
    render_flash()
    if DEBUG_HUD:
        debug_hud("Browse/pre")

    # ---------- База и диапазоны ----------
    db_path = str(st.session_state.get("ACTIVE_DB_PATH", "data/default_expenses.db"))
    base_df = load_df(db_path, _ver=get_data_version())  # без ограничений дат
    if DEBUG_HUD:
        debug_hud("Browse/post", base_df)

    if base_df is not None and not base_df.empty:
        base_df["date"] = pd.to_datetime(base_df["date"], errors="coerce")
        min_date = base_df["date"].min().date()
        max_date = base_df["date"].max().date()
        cats_all_from_db = sorted(
            c for c in base_df["category"].dropna().unique().tolist()
        )
        min_amount_def = float(base_df["amount"].min())
        max_amount_def = float(base_df["amount"].max())
    else:
        today = date.today()
        min_date, max_date = today.replace(day=1), today
        cats_all_from_db, min_amount_def, max_amount_def = [], 0.0, 0.0

    cats, cat_labels = categories_ui(lang)
    fmt = make_category_formatter(cat_labels)
    if DEBUG_HUD:
        debug_hud("labels", extra={"labels_lang": lang, "labels_cnt": len(cat_labels)})

    cats_all = list(dict.fromkeys([*cats, *cats_all_from_db]))

    ss = st.session_state

    # ---------- Инициализация (хранимые значения) ----------
    if "bf_init" not in ss:
        ss["bf_start"] = min_date.isoformat()
        ss["bf_end"] = max_date.isoformat()
        ss["bf_categories"] = cats_all.copy()
        ss["bf_search"] = ""
        ss["bf_min"] = min_amount_def
        ss["bf_max"] = max_amount_def
        ss["bf_init"] = True

    # ---------- Обработка Reset / Apply ДО рендера ----------
    if ss.pop("_bf_do_reset", False):
        ss["bf_start"] = min_date.isoformat()
        ss["bf_end"] = max_date.isoformat()
        ss["bf_categories"] = cats_all.copy()
        ss["bf_search"] = ""
        ss["bf_min"] = min_amount_def
        ss["bf_max"] = max_amount_def
        st.rerun()

    if ss.pop("_bf_do_apply", False):
        # переносим значения из *_input в хранилище и перерисовываем
        ss["bf_start"] = ss["bf_start_input"].isoformat()
        ss["bf_end"] = ss["bf_end_input"].isoformat()
        ss["bf_categories"] = ss["bf_categories_input"]
        ss["bf_search"] = ss["bf_search_input"]
        ss["bf_min"] = ss["bf_min_input"]
        ss["bf_max"] = ss["bf_max_input"]
        ss["_flash"] = (
            t("dashboard.filters_applied", lang, default="Filters applied"),
            "⚙️",
        )
        st.rerun()

    # ---------- Виджеты (используем отдельные ключи *_input) ----------
    with st.form("filter_form", clear_on_submit=False):
        c1, c2 = st.columns(2)
        with c1:
            st.date_input(
                t("common.start", lang, default="Start"),
                value=pd.to_datetime(ss["bf_start"]).date(),
                min_value=min_date,
                max_value=max_date,
                key="bf_start_input",
            )
        with c2:
            st.date_input(
                t("common.end", lang, default="End"),
                value=pd.to_datetime(ss["bf_end"]).date(),
                min_value=min_date,
                max_value=max_date,
                key="bf_end_input",
            )

        c3, c4 = st.columns([2, 1])
        with c3:
            st.multiselect(
                t("col.category", lang, default="Category"),
                options=cats_all,
                default=ss["bf_categories"],
                format_func=fmt,
                key="bf_categories_input",
            )
        with c4:
            st.text_input(
                t(
                    "browse.search_contains",
                    lang,
                    default="Search (description contains)",
                ),
                value=ss["bf_search"],
                key="bf_search_input",
            )

        a1, a2 = st.columns(2)
        with a1:
            st.number_input(
                t("browse.min_amount", lang, default="Min amount"),
                value=float(ss["bf_min"]),
                step=1.0,
                key="bf_min_input",
            )
        with a2:
            st.number_input(
                t("browse.max_amount", lang, default="Max amount"),
                value=float(ss["bf_max"]),
                step=1.0,
                key="bf_max_input",
            )

        col_btn1, col_btn2 = st.columns(2)
        run = col_btn1.form_submit_button(t("common.apply", lang, default="Apply"))
        reset = col_btn2.form_submit_button(t("browse.reset", lang, default="Reset"))

    if reset:
        ss["_bf_do_reset"] = True
        st.rerun()

    if run:
        ss["_bf_do_apply"] = True
        st.rerun()

    # ---------- Фильтрация данных ----------
    f = base_df.copy()
    if not f.empty:
        start = pd.to_datetime(ss["bf_start"]).date()
        end = pd.to_datetime(ss["bf_end"]).date()
        sel_cats = ss["bf_categories"]
        sv = (ss["bf_search"] or "").strip().lower()
        a_min, a_max = float(ss["bf_min"]), float(ss["bf_max"])

        f = f[(f["date"].dt.date >= start) & (f["date"].dt.date <= end)]
        if sel_cats:
            f = f[f["category"].isin(sel_cats)]
        if sv:
            f = f[f["description"].astype(str).str.lower().str.contains(sv, na=False)]
        f = f[(f["amount"] >= a_min) & (f["amount"] <= a_max)]

    # ---------- KPI ----------
    total = float(f["amount"].sum()) if not f.empty else 0.0
    count = int(len(f))
    avg = float(f["amount"].mean()) if count else 0.0

    k1, k2, k3 = st.columns(3)
    k1.metric(t("kpi.total", lang, default="Total"), f"{total:.2f}")
    k2.metric(t("kpi.operations", lang, default="Operations"), f"{count}")
    k3.metric(t("kpi.average", lang, default="Average"), f"{avg:.2f}")

    st.divider()

    # ---------- Параметры отображения ----------
    st.subheader(t("browse.view_options", lang, default="View options"))
    col_opts, _ = st.columns([1, 3])
    with col_opts:
        rm_dups = st.checkbox(
            t("browse.remove_dups", lang, default="Remove exact duplicates"),
            value=True,
            key="opt_remove_dups",
        )
        newest_first = st.checkbox(
            t("browse.newest_first", lang, default="Newest first"),
            value=True,
            key="opt_newest_first",
        )

    f_disp = prepare_df_for_view(f, remove_dups=rm_dups, newest_first=newest_first)

    # ---------- Таблица (локализация категорий тем же fmt) ----------
    f_show = f_disp.copy()
    f_show["date"] = pd.to_datetime(f_show["date"], errors="coerce").dt.strftime(
        "%Y-%m-%d"
    )
    f_show["category"] = f_show["category"].map(fmt)

    col_names = _col_labels(lang)
    f_show = f_show.rename(columns=col_names)

    # Заголовок таблицы результатов
    st.subheader(t("browse.results", lang, default="Filtered results"))

    st.dataframe(
        f_show,
        width="stretch",
        hide_index=True,
        column_config={
            col_names["amount"]: st.column_config.NumberColumn(
                t("col.amount", lang, default="Amount"), format="%.2f"
            ),
            col_names["date"]: st.column_config.DatetimeColumn(
                t("col.date", lang, default="Date"), format="YYYY-MM-DD"
            ),
        },
    )

    st.divider()
    st.subheader(t("browse.export_data", lang, default="Export Data"))
    csv_bytes = f_disp.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="📥 " + t("common.download_csv", lang, default="Download CSV"),
        data=csv_bytes,
        file_name="expenses_filtered.csv",
        mime="text/csv",
        key="btn_download_csv",
    )
    export_df_to_excel_button(f_disp, filename="expenses_filtered.xlsx")


# ================ Charts ================
elif choice == "charts":
    # Заголовок и подпись
    st.subheader(t("menu.charts", lang, default="Charts"))
    st.caption(
        "📈 " + t("charts.placeholder", lang, default="Charts page (placeholder)")
    )
    render_flash()
    debug_hud("Charts/pre")

    lang = st.session_state.get("lang", "en")
    cats, cat_labels = categories_ui(lang)
    debug_hud("labels", extra={"labels_lang": lang, "labels_cnt": len(cat_labels)})

    def _fmt_cat(key: object) -> str:
        s = "" if key is None else str(key)
        return cat_labels.get(s, s)

    # 0) исходные данные
    db_path = str(st.session_state.get("ACTIVE_DB_PATH", "data/default_expenses.db"))
    df = load_df(db_path, _ver=get_data_version()).copy()
    debug_hud("Charts/post", df)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])

    # если у вас есть фильтры по датам/категориям выше — применяйте их здесь,
    # чтобы ch_df уже соответствовал выбранному диапазону/категориям
    ch_df = df.copy()

    # 2) агрегаты — БЕЗ масштабирования /100
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

    # 4) ВИЗУАЛИЗАЦИИ
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
            .properties(height=300)
        )
        st.altair_chart(bar, use_container_width=True)
    else:
        st.info(t("common.no_data", lang, default="No data to display."))

    st.markdown("#### " + t("charts.by_date", lang, default="By date"))
    if not line_df.empty:
        line = (
            alt.Chart(line_df)
            .mark_line(point=True)
            .encode(
                x=alt.X("date:T", title=t("charts.date", lang, default="Date")),
                y=alt.Y(
                    "total:Q",
                    title=t("kpi.total", lang, default="Total"),
                    axis=alt.Axis(format=",.2f"),
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
            .properties(height=280)
        )
        st.altair_chart(line, use_container_width=True)
    else:
        st.info(t("common.no_data", lang, default="No data to display."))
    # ============================================================================

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
            st.altair_chart(chart, use_container_width=True)
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
            ensure_db(get_db_path(new_name))
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
    db_path_str = str(get_db_path(user))
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

# 6) Подсказки (3 последних месяца)
with st.expander("Suggestions (last 3 months)"):
    df_hist = get_expenses_df(db_path=db_path_str)
    recs = []
    if df_hist is not None and not df_hist.empty:
        df_hist["ym"] = pd.to_datetime(df_hist["date"]).dt.strftime("%Y-%m")
        recent = df_hist[df_hist["ym"] <= mk].copy()
        recent = recent.sort_values("date")
        piv = recent.pivot_table(
            index="ym", columns="category", values="amount", aggfunc="sum"
        ).sort_index()
        user = st.session_state.get("current_user", "default")
        limits_map = load_monthly_limits(
            limits_path_for(user)
        )  # dict: { "YYYY-MM": {cat: val, ...}, ... }
        cur_limits = limits_map.get(mk, {})
        for cat in sorted(set(list(piv.columns) + list(cur_limits.keys()))):
            avg3 = float(piv.get(cat, pd.Series(dtype=float)).tail(3).mean() or 0.0)
            lim = float(cur_limits.get(cat, 0.0))
            if avg3 == 0 and lim == 0:
                continue
            if avg3 > lim * 1.1:
                recs.append(
                    f"↑ {cat}: avg last 3 mo {avg3:.2f} > limit {lim:.2f} → "
                    f"consider +{avg3-lim:.2f}"
                )
            elif lim > 0 and lim > avg3 * 1.25:
                recs.append(
                    f"↓ {cat}: limit {lim:.2f} >> avg {avg3:.2f} → consider −{lim-avg3:.2f}"
                )
    if recs:
        for r in recs:
            st.write("• " + r)
    else:
        st.caption("No suggestions yet.")
