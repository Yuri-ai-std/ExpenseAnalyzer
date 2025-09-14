import json
import sqlite3
from datetime import date
from datetime import date as _date
from datetime import datetime
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, cast

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

# делаем пути видимыми для других модулей через session_state
st.session_state["ACTIVE_DB_PATH"] = ACTIVE_DB_PATH
st.session_state["ACTIVE_LIMITS_PATH"] = str(ACTIVE_LIMITS_PATH)

# ---- flash-toast from previous run ----
_flash = st.session_state.pop("_flash", None)
if _flash:
    # _flash: tuple[str, str|None] -> (message, icon)
    msg, icon = (_flash + (None,))[:2]
    st.toast(msg, icon=icon)


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


def _month_key(date_value):
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


def render_table(df, cols, lang: str, **st_kwargs):
    """
    Срезает нужные колонки (по исходным ключам) и ПЕРЕИМЕНОВЫВАЕТ только для отображения.
    Логику (группировки/сортировки) это не ломает.
    """
    df_disp = df.loc[:, cols].rename(columns=_col_labels(lang))
    st.dataframe(df_disp, **st_kwargs)


def render_recent_expenses_table(
    db_path, n: int = 10, *, show_title: bool = False, lang: str = "en"
) -> None:
    """Показывает последние n операций из указанной БД.
    Сортировка как везде: новые сверху, дубликаты убираем.
    """
    if show_title:
        st.subheader(t("recent_expenses", lang, default="Recent expenses"))

    raw_df = get_expenses_df(db_path=db_path)
    df = prepare_df_for_view(raw_df, remove_dups=True, newest_first=True)

    # так как newest_first=True, новые строки сверху => берём .head(n)
    df_recent = df.head(n)

    # Локализованный вывод таблицы
    cols = ["id", "date", "category", "amount", "description"]
    render_table(
        df_recent,
        cols=cols,
        lang=lang,
        hide_index=True,
        width="stretch",  # или width="stretch", если вам так привычнее
        height=360,  # опционально, для одинаковой высоты
    )


# ===== ЛОГ ПЕРЕЗАПУСКА =====
print(f"\n🔄 Streamlit перезапущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")


# ===== Вспомогательные функции =====
@st.cache_data(ttl=10, show_spinner=False)
def load_df(
    start: str | None = None, end: str | None = None, _ver: int = 0
) -> pd.DataFrame:
    """Загружает операции из БД активного пользователя как DataFrame.
    Параметр _ver нужен только для инвалидации кэша."""
    db_path = st.session_state.get("ACTIVE_DB_PATH", "data/default_expenses.db")
    df = get_expenses_df(db_path=db_path, start_date=start, end_date=end)
    # ↓ ваш существующий код нормализации колонок
    expected = ["date", "category", "amount", "description"]
    for col in expected:
        if col not in df.columns:
            df[col] = pd.NA
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    return df.dropna(subset=["date", "amount"])


@st.cache_data(ttl=120)
def get_categories() -> list[str]:
    """Список категорий из БД (distinct), кэшируем на 2 мин."""
    try:
        with sqlite3.connect("expenses.db") as conn:
            rows = conn.execute("SELECT DISTINCT category FROM expenses").fetchall()
        return [r[0] for r in rows if r and r[0]]
    except Exception:
        return []


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


def bump_data_version() -> None:
    """Инкремент версии данных, чтобы инвалидировать кэш загрузчиков таблиц."""
    st.session_state["data_version"] = st.session_state.get("data_version", 0) + 1


def render_add_expense_page(lang: str) -> None:
    ss = st.session_state
    user = current_user()  # получаем имя пользователя
    keys = add_form_keys(user)  # генерируем ключи формы
    apply_form_reset(keys)  # сброс формы для этого пользователя


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

# ----- Dashboard -----
if choice == "dashboard":
    st.header(t("menu.dashboard", lang, default="Dashboard"))
    st.write(
        "📊 " + t("dashboard.placeholder", lang, default="Dashboard page (placeholder)")
    )

    # ----- Фильтры по дате -----
    today = date.today()
    month_start = today.replace(day=1)

    # 1) Хранимые значения фильтров в session_state (строки 'YYYY-MM-DD')
    if "dash_start" not in st.session_state:
        st.session_state["dash_start"] = month_start.isoformat()
    if "dash_end" not in st.session_state:
        st.session_state["dash_end"] = today.isoformat()

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

    # ----- Данные -----
    raw_df = load_df(start_s, end_s)
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

    # ----- KPI -----
    total = float(df["amount"].sum())
    count = len(df)
    avg = float(df["amount"].mean())
    cats = int(df["category"].nunique())

    k1, k2, k3, k4 = st.columns(4)
    k1.metric(t("kpi.total", lang, default="Total"), f"{total:.2f}")
    k2.metric(t("kpi.operations", lang, default="Operations"), f"{count}")
    k3.metric(t("kpi.average", lang, default="Average"), f"{avg:.2f}")
    k4.metric(t("kpi.categories", lang, default="Categories"), f"{cats}")

    st.divider()

    # ----- Топ последних операций -----
    st.subheader(t("dashboard.last_operations", lang, default="Last operations"))
    show_cols = ["date", "category", "amount", "description"]

    # проверяем, есть ли колонка id для надёжной сортировки
    sort_cols = ["date"] + (["id"] if "id" in df.columns else [])

    last5 = (
        df.sort_values(sort_cols, ascending=[False] * len(sort_cols))
        .loc[:, show_cols]
        .head(5)
    )

    show_cols = ["date", "category", "amount", "description"]
    render_table(
        last5,
        cols=show_cols,
        lang=lang,
        hide_index=True,
        width="stretch",
        height=220,
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
    st.bar_chart(cat_totals, use_container_width=True)

    # ----- Последние операции -----
    render_recent_expenses_table(ACTIVE_DB_PATH, n=10, show_title=True, lang=lang)

# =================== Add Expense ===================
elif choice == "add_expense":
    lang = st.session_state.get("lang", "en")
    st.header(t("menu.add_expense", lang, default="Add Expense"))

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
    try:
        cats = list(get_categories())
    except Exception:
        cats = []

    if not cats:
        cats = list(BASE_CATEGORIES)

    cats = sorted(cats)

    # ----- форма -----
    with st.form(f"add_form_{user}", clear_on_submit=False):
        d = st.date_input(t("col.date", lang, default="Date"), key=keys["date"])

        mode = ss[keys["mode"]]
        if mode == MODE_CHOOSE:
            cat_val = st.selectbox(
                t("add_expense.choose_existing", lang, default="Choose category"),
                options=cats,
                index=0 if cats else None,
                key=keys["choose"],
            )
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

            st.success(t("info.expense_added", lang, default="Expense added."))
            request_form_reset(keys)  # сброс поля/режима после добавления
            st.rerun()

    # ---- таблица последних записей (как было у вас) ----
    render_recent_expenses_table(ACTIVE_DB_PATH, n=10, show_title=False, lang=lang)

# ================= Browse & Filter =================
elif choice == "browse":
    st.header(t("menu.browse", lang, default="Browse & Filter"))
    st.write(
        "🔎 "
        + t("browse.placeholder", lang, default="Browse & Filter page (placeholder)")
    )

    # Загружаем все данные
    db_path = st.session_state.get("ACTIVE_DB_PATH", "data/default_expenses.db")
    base_df = get_expenses_df(db_path=db_path)
    if base_df.empty:
        st.info(t("no_expenses_found", lang, default="No expenses found."))
        st.stop()

    df = base_df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])

    # Все категории из базы
    cats_all = sorted(get_categories() or df["category"].dropna().unique().tolist())
    min_date = df["date"].min().date()
    max_date = df["date"].max().date()
    min_amt = float(df["amount"].min())
    max_amt = float(df["amount"].max())

    # --- Фильтры ---
    with st.form("filter_form", clear_on_submit=False):
        c1, c2 = st.columns(2)
        with c1:
            start = st.date_input(
                "Start",
                value=min_date,
                min_value=min_date,
                max_value=max_date,
                key="filter_start_date",  # Уникальный ключ для поля Start
            )
        with c2:
            end = st.date_input(
                "End",
                value=max_date,
                min_value=min_date,
                max_value=max_date,
                key="filter_end_date",  # Уникальный ключ для поля End
            )

        c3, c4 = st.columns([2, 1])
        with c3:
            sel_cats = st.multiselect(
                "Category",
                options=cats_all,
                default=cats_all,
                placeholder="Select categories...",
            )
        with c4:
            search = st.text_input("Search (description contains)", value="")

        a1, a2 = st.columns(2)
        with a1:
            amt_min = st.number_input(
                "Min amount", value=float(f"{min_amt:.2f}"), step=1.0
            )
        with a2:
            amt_max = st.number_input(
                "Max amount", value=float(f"{max_amt:.2f}"), step=1.0
            )

        fcol1, fcol2 = st.columns([1, 1])
        run = fcol1.form_submit_button(t("apply", lang, default="Apply"))
        reset = fcol2.form_submit_button(t("reset", lang, default="Reset"))

    if reset:
        st.cache_data.clear()
        st.rerun()

    # --- Применение фильтров ---
    f = df.copy()
    f = f[(f["date"].dt.date >= start) & (f["date"].dt.date <= end)]
    if sel_cats:
        f = f[f["category"].isin(sel_cats)]
    if search.strip():
        s = search.strip().lower()
        f = f[f["description"].fillna("").str.lower().str.contains(s)]
    f = f[(f["amount"] >= amt_min) & (f["amount"] <= amt_max)]

    if f.empty:
        st.warning(
            t(
                "no_expenses_found",
                lang,
                default="No expenses found for selected filters.",
            )
        )
        st.stop()

    # --- KPI ---
    k1, k2, k3 = st.columns(3)
    with k1:
        st.metric(t("total", lang, default="Total"), f"{f['amount'].sum():.2f}")
    with k2:
        st.metric(t("operations", lang, default="Operations"), len(f))
    with k3:
        st.metric(t("average", lang, default="Average"), f"{f['amount'].mean():.2f}")

    st.divider()

    # --- Опции отображения/очистки ---
    st.subheader(t("browse.view_options", lang, default="View options"))
    col_opts, _ = st.columns([1, 3])
    with col_opts:
        rm_dups = st.checkbox(
            t("browse.remove_dups", lang, default="Remove exact duplicates"),
            value=True,
            help=t(
                "browse.remove_dups_help",
                lang,
                default="Remove rows that are exact duplicates (date, category, amount, description).",
            ),
        )
        newest_first = st.checkbox(
            t("browse.newest_first", lang, default="Newest first"),
            value=True,
        )

    # --- Подготовка данных к показу ---
    f_disp = f.copy()

    # 1) Удаление точных дубликатов (если включено)
    if rm_dups:
        f_disp = f_disp.drop_duplicates(
            subset=["date", "category", "amount", "description"],
            keep="last",
        )

    # 2) Сортировка по дате
    f_disp["date"] = pd.to_datetime(f_disp["date"], errors="coerce")
    f_disp = f_disp.sort_values("date", ascending=not newest_first).reset_index(
        drop=True
    )

    # 3) Копия для красивого показа дат как YYYY-MM-DD
    f_show = f_disp.copy()
    f_show["date"] = f_show["date"].dt.strftime("%Y-%m-%d")

    # --- Таблица ---
    st.dataframe(
        f_show,
        width="stretch",
        hide_index=True,
        column_config={
            "amount": st.column_config.NumberColumn("Amount", format="%.2f"),
            "date": st.column_config.DatetimeColumn("Date", format="YYYY-MM-DD"),
        },
    )

    st.divider()

    # --- Экспорт данных ---
    st.subheader(t("browse.export_data", lang, default="Export Data"))
    csv_bytes = f_disp.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="📥 Download CSV",
        data=csv_bytes,
        file_name="expenses_filtered.csv",
        mime="text/csv",
    )

    export_df_to_excel_button(f_disp, filename="expenses_filtered.xlsx")


# ================= Charts & Analytics =================
elif choice == "charts":
    st.header(t("menu.charts", lang, default="Charts"))
    st.write("📈 Charts page (placeholder)")

    # ---- Контроли периода ----
    colp1, colp2, colp3 = st.columns([1.4, 1.4, 1])

    start_c = colp1.date_input(
        t("start", lang, default="Start"),
        value=_date(_date.today().year, _date.today().month, 1),
        key="charts_start",  # уникальный ключ
    )

    end_c = colp2.date_input(
        t("end", lang, default="End"),
        value=_date.today(),
        key="charts_end",  # уникальный ключ
    )

    apply_c = colp3.button(t("apply", lang, default="Apply"), width="stretch")

    # Грузим данные по периоду
    df_raw = load_df(str(start_c), str(end_c)) if apply_c or True else load_df()
    if df_raw.empty:
        st.info(
            t(
                "no_expenses_found",
                lang,
                default="No expenses found for selected period.",
            )
        )
        st.stop()

    # Очищаем/нормализуем и удаляем возможные дубликаты показа
    df = prepare_df_for_view(df_raw, remove_dups=True)

    # ---- Быстрые фильтры по категориям ----
    cats_all = sorted(df["category"].astype(str).unique().tolist())
    sel_cats = st.multiselect(
        t("category", lang, default="Category"),
        options=cats_all,
        default=cats_all,
    )
    if sel_cats:
        df = df[df["category"].isin(sel_cats)]
    if df.empty:
        st.info(
            t(
                "no_expenses_found",
                lang,
                default="No expenses found for selected period.",
            )
        )
        st.stop()

    st.divider()

    # =========================
    #  A) Бар-чарт по категориям
    # =========================
    st.subheader(t("dashboard.by_category", lang, default="By category"))
    cat_sum = (
        df.groupby("category", as_index=False)
        .agg(amount=("amount", "sum"))  # <- получаем DataFrame с колонкой amount
        .sort_values(
            "amount", ascending=False
        )  # <- сортировка DataFrame (без 'by=' для Series)
    )

    cat_chart = (
        alt.Chart(cat_sum)
        .mark_bar()
        .encode(
            x=alt.X(
                "category:N", title=t("category", lang, default="Category"), sort="-y"
            ),
            y=alt.Y("amount:Q", title=t("total", lang, default="Total")),
            tooltip=[
                alt.Tooltip(
                    "category:N", title=t("category", lang, default="Category")
                ),
                alt.Tooltip(
                    "amount:Q", title=t("total", lang, default="Total"), format=".2f"
                ),
            ],
        )
        .properties(height=360)
    )
    st.altair_chart(cat_chart, use_container_width=True)

    # =========================
    #  B) Линия: динамика по датам
    # =========================
    st.subheader(t("charts.by_date", lang, default="By date"))
    daily = (
        df.groupby("date", as_index=False)
        .agg(amount=("amount", "sum"))  # DataFrame
        .sort_values("date")  # сортировка по дате
    )

    line_chart = (
        alt.Chart(daily)
        .mark_line(point=True)
        .encode(
            x=alt.X("date:T", title=t("date", lang, default="Date")),
            y=alt.Y("amount:Q", title=t("total", lang, default="Total")),
            tooltip=[
                alt.Tooltip("date:T", title=t("date", lang, default="Date")),
                alt.Tooltip(
                    "amount:Q", title=t("total", lang, default="Total"), format=".2f"
                ),
            ],
        )
        .properties(height=360)
    )
    st.altair_chart(line_chart, use_container_width=True)

    # =========================
    #  C) (опционально) Pie-chart
    # =========================
    with st.expander(
        t("charts.share_by_category", lang, default="Share by category (pie)")
    ):
        share = cat_sum.copy()
        total_sum = float(share["amount"].sum()) or 1.0
        share["share"] = share["amount"] / total_sum

        pie = (
            alt.Chart(share)
            .mark_arc(innerRadius=60)  # пончиковая диаграмма
            .encode(
                theta=alt.Theta("amount:Q"),
                color=alt.Color(
                    "category:N",
                    legend=alt.Legend(title=t("category", lang, default="Category")),
                ),
                tooltip=[
                    alt.Tooltip(
                        "category:N", title=t("category", lang, default="Category")
                    ),
                    alt.Tooltip(
                        "amount:Q",
                        title=t("total", lang, default="Total"),
                        format=",.2f",
                    ),
                    alt.Tooltip(
                        "share:Q", title=t("share", lang, default="Share"), format=".1%"
                    ),
                ],
            )
            .properties(height=320)
        )
        st.altair_chart(pie, use_container_width=True)

    # =========================
    #  D) Показ таблицы (без дублей)
    # =========================
    with st.expander(t("show_table", lang, default="Show data")):
        st.dataframe(
            (df.sort_values("date", ascending=False)).reset_index(drop=True),
            width="stretch",
            hide_index=True,
            column_config={
                "amount": st.column_config.NumberColumn(
                    t("amount", lang, default="Amount"), format="%.2f"
                ),
                "date": st.column_config.DatetimeColumn(
                    t("date", lang, default="Date"), format="YYYY-MM-DD"
                ),
            },
        )

# ================= Settings =================
elif choice == "settings":
    st.header(t("menu.settings", lang, default="Settings"))

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
            except Exception as e:
                st.error(t("profile.rename_failed", lang, default="Rename failed."))
                st.exception(e)

# быстрый свитч, если пользователь в select изменён
if sel != current and st.session_state.get("settings_active_user") == sel:
    switch_user(sel, toast=t("profile.toast_switched", lang, default="Switched"))

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

# 3) Редактор лимитов
user = current_user()  # получаем текущего пользователя
ym = current_limits_month()  # получаем текущий месяц в формате YYYY-MM

st.write(
    f"{t('profile.title', lang, default='User / Profile').split(' / ')[0]}: {user} • "
    f"{t('limits.month', lang, default='Month')}: {ym}"
)

values: dict[str, float] = {}
for cat in cats:
    values[cat] = st.number_input(
        cat,
        min_value=0.0,
        step=10.0,
        value=float(limits_now.get(cat, 0.0)),
        key=f"limit_{ym}_{cat}",  # уникальные ключи на месяц+категорию
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
