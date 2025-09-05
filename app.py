import json
import sqlite3
from datetime import date
from datetime import date as _date
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Dict

import altair as alt
import pandas as pd
import streamlit as st
from pyparsing import cast

from db import add_expense, ensure_db, get_db_path, get_expenses_df, list_categories

# CSV/аудит для лимитов
from limits_tools import (
    append_audit_row,
    audit_to_csv_bytes,
    audit_to_json_bytes,
    csv_bytes_to_limits,
    get_audit,
    limits_to_csv_bytes,
)
from messages import messages
from utils import (
    db_path_for,
    limits_path_for,
    load_monthly_limits,
    month_key,
    save_monthly_limits,
)

# --- aliases for tests (test_limits_io.py expects underscored names)
_limits_to_csv_bytes = limits_to_csv_bytes

st.session_state.setdefault("current_user", "default")
current_user = st.session_state["current_user"]

ACTIVE_DB_PATH = db_path_for(current_user)  # data/default_expenses.db
ACTIVE_LIMITS_PATH = limits_path_for(current_user)  # data/default/budget_limits.json
DATA_DIR = Path("data")

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


# ===== ЛОГ ПЕРЕЗАПУСКА =====
print(f"\n🔄 Streamlit перезапущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")


# ===== Вспомогательные функции =====
@st.cache_data(ttl=10, show_spinner=False)
def load_df(start: str | None = None, end: str | None = None) -> pd.DataFrame:
    """Загружает операции из БД активного пользователя как DataFrame."""
    db_path = st.session_state.get("ACTIVE_DB_PATH", "data/default_expenses.db")
    df = get_expenses_df(
        db_path=db_path, start_date=start, end_date=end
    )  # ✅ фильтр по датам

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


# --- Устанавливаем язык интерфейса ---
if "lang" not in st.session_state:
    st.session_state["lang"] = "en"
lang = st.session_state["lang"]
msgs = messages[lang]

# 👉 Определяем активного пользователя (default при старте)
current_user = st.session_state.get("current_user", "default")

# чтение лимитов пользователя
limits = load_monthly_limits(user=current_user)

# ...изменили словарь limits на форме...

# сохранение лимитов пользователя
save_monthly_limits(limits, user=current_user)


def _fetch_categories() -> list[str]:
    # 1) если есть list_categories в db.py — используйте его
    try:
        from db import (
            list_categories as _list_categories,  # локальный импорт на случай отсутствия
        )

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


# ====== Add Expense: хелперы ключей (вставить один раз выше по файлу) ======


def _add_form_suffix() -> str:
    """Суффикс для ключей формы — по активному пользователю."""
    return st.session_state.get("current_user", "default")


def add_form_keys() -> dict[str, str]:
    """Единое место, где объявлены ВСЕ ключи формы Add Expense."""
    sfx = _add_form_suffix()
    return {
        "mode": f"add_cat_mode_{sfx}",
        "choose": f"add_cat_choose_{sfx}",
        "new": f"add_cat_new_{sfx}",
        "date": f"add_date_{sfx}",
        "amount": f"add_amount_{sfx}",
        "note": f"add_note_{sfx}",
        "reset": f"add_form_reset_{sfx}",  # внутренний флажок мягкого сброса
    }


def add_form_soft_reset() -> None:
    """Мягкий сброс значений ДО инстанса виджетов."""
    k = add_form_keys()
    if st.session_state.pop(k["reset"], False):
        st.session_state.pop(k["choose"], None)
        st.session_state.pop(k["new"], None)
        st.session_state.pop(k["amount"], None)
        st.session_state.pop(k["note"], None)
        st.session_state.pop(k["mode"], None)


# --- Меню ---
menu = ["Dashboard", "Add Expense", "Browse & Filter", "Charts", "Settings"]
choice = st.sidebar.radio("Menu", menu)

if choice == "Dashboard":
    st.title(msgs.get("dashboard", "Dashboard"))

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
            "Start",
            value=pd.to_datetime(st.session_state["dash_start"]).date(),
            key="dash_start_input",
        )
    with c2:
        end_d = st.date_input(
            "End",
            value=pd.to_datetime(st.session_state["dash_end"]).date(),
            key="dash_end_input",
        )
    with c3:
        refresh = st.button("Apply", key="dash_apply")

    # 3) При нажатии Apply переносим значения из виджетов в хранилище
    # и мягко перерисовываем страницу
    if refresh:
        st.session_state["dash_start"] = start_d.isoformat()
        st.session_state["dash_end"] = end_d.isoformat()
        st.session_state["_flash"] = ("Filters applied", "⚙️")
        st.rerun()

    # 4) Строки для загрузки данных
    start_s = st.session_state["dash_start"]  # 'YYYY-MM-DD'
    end_s = st.session_state["dash_end"]  # 'YYYY-MM-DD'

    # ----- Данные -----
    raw_df = load_df(start_s, end_s)
    if raw_df.empty:
        st.info(msgs.get("no_expenses_found", "No expenses found for selected period."))
        st.stop()

    # Очистка и сортировка через хелпер
    df = prepare_df_for_view(raw_df, remove_dups=True, newest_first=True)

    # ----- KPI -----
    total = float(df["amount"].sum())
    count = len(df)
    avg = float(df["amount"].mean())
    cats = int(df["category"].nunique())

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total", f"{total:.2f}")
    k2.metric("Operations", f"{count}")
    k3.metric("Average", f"{avg:.2f}")
    k4.metric("Categories", f"{cats}")

    st.divider()

    # ----- Топ последних операций -----
    st.subheader("Last operations")
    show_cols = ["date", "category", "amount", "description"]

    # проверяем, есть ли колонка id для надёжной сортировки
    sort_cols = ["date"] + (["id"] if "id" in df.columns else [])

    last5 = (
        df.sort_values(sort_cols, ascending=[False] * len(sort_cols))
        .loc[:, show_cols]
        .head(5)
    )

    st.dataframe(
        last5,
        width="stretch",
        hide_index=True,
        height=220,  # немного увеличим высоту для наглядности
    )

    # ----- Диаграмма по категориям -----
    st.subheader("By category")
    cat_totals = (
        df.groupby("category", dropna=False)["amount"]
        .sum()
        .sort_values(ascending=False)
        .rename("total")
        .to_frame()
    )
    st.bar_chart(cat_totals, use_container_width=True)

# ======================= Add Expense =======================
elif choice == "Add Expense":
    st.title(msgs.get("add_expense", "Add Expense"))

    # ---- активный пользователь и БД ----
    user = get_active_user()
    db_path = get_db_path(user)
    ensure_db(db_path)

    # категории (объединённый список из БД и лимитов)
    cats = list_categories(db_path=db_path)

    # ключи формы + мягкий сброс (ВАЖНО вызвать до виджетов!)
    keys = add_form_keys()
    add_form_soft_reset()

    # (необязательно) отладочный префикс
    from pathlib import Path

    db_name = Path(db_path).name if db_path else str(db_path)
    st.caption(f"DBG ➜ user={user} | db={db_name} | cats={cats!r}")

    # ---------- форма ввода ----------
    with st.form("add_expense_form", clear_on_submit=False):
        d = st.date_input(
            msgs.get("date", "Date"),
            value=date.today(),
            format="YYYY/MM/DD",
            key=keys["date"],
        )

        mode = st.radio(
            msgs.get("category", "Category"),
            options=["choose", "new"],
            index=0 if cats else 1,
            horizontal=True,
            captions=[
                msgs.get("choose_existing", "Choose existing"),
                msgs.get("enter_new", "Enter new"),
            ],
            key=keys["mode"],
        )

        if mode == "choose":
            cat_val = st.selectbox(
                msgs.get("choose_category", "Choose category"),
                options=cats,
                index=None,
                placeholder=msgs.get("choose_placeholder", "Choose an option"),
                key=keys["choose"],
            )
        else:
            cat_val = st.text_input(
                msgs.get("new_category", "New category"),
                key=keys["new"],
            )

        amount = st.number_input(
            msgs.get("amount", "Amount"),
            min_value=0.0,
            step=0.01,
            format="%.2f",
            key=keys["amount"],
        )

        note = st.text_area(
            msgs.get("description", "Description"),
            key=keys["note"],
        )

        submit = st.form_submit_button(msgs.get("submit", "Submit"))

    # ---------- обработка сабмита ----------
    if submit:
        has_error = False

        # валидация категории
        cat_val = (cat_val or "").strip()
        if not cat_val:
            st.error(msgs.get("error_category", "Please enter / choose a category."))
            has_error = True

        # валидация суммы
        try:
            amt_f = float(amount)
            if amt_f <= 0:
                raise ValueError
        except Exception:
            st.error(msgs.get("error_amount", "Amount must be greater than zero."))
            has_error = True

        if not has_error:
            try:
                add_expense(
                    date=str(d),
                    category=cat_val,  # <-- cat_val_s НЕ используем
                    amount=amt_f,
                    description=(note or "").strip(),
                    db_path=db_path,
                )
                # флеш-тост + плановый мягкий сброс на следующем рендере
                st.session_state["_flash"] = (
                    msgs.get("expense_added", "Expense added successfully!"),
                    "✅",
                )
                st.session_state[keys["reset"]] = True

                st.cache_data.clear()
                st.rerun()

            except Exception as ex:
                st.error(msgs.get("save_error", "Could not save expense."))
                st.exception(ex)

elif choice == "Browse & Filter":
    st.title(msgs.get("browse_filter", "Browse & Filter"))

    # Загружаем все данные
    db_path = st.session_state.get("ACTIVE_DB_PATH", "data/default_expenses.db")
    base_df = get_expenses_df(db_path=db_path)
    if base_df.empty:
        st.info(msgs.get("no_expenses_found", "No expenses found."))
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
        run = fcol1.form_submit_button(msgs.get("apply", "Apply"))
        reset = fcol2.form_submit_button(msgs.get("reset", "Reset"))

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
            msgs.get("no_expenses_found", "No expenses found for selected filters.")
        )
        st.stop()

    # --- KPI ---
    k1, k2, k3 = st.columns(3)
    with k1:
        st.metric(msgs.get("total", "Total"), f"{f['amount'].sum():.2f}")
    with k2:
        st.metric(msgs.get("operations", "Operations"), len(f))
    with k3:
        st.metric(msgs.get("average", "Average"), f"{f['amount'].mean():.2f}")

    st.divider()

    # --- Опции отображения/очистки ---
    st.subheader("View options")
    col_opts, _ = st.columns([1, 3])
    with col_opts:
        rm_dups = st.checkbox(
            "Remove exact duplicates",
            value=True,
            help="Убирает полностью совпадающие строки (date, category, amount, description).",
        )
        newest_first = st.checkbox("Newest first", value=True)

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
    st.subheader("Export Data")
    csv_bytes = f_disp.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="📥 Download CSV",
        data=csv_bytes,
        file_name="expenses_filtered.csv",
        mime="text/csv",
    )

    export_df_to_excel_button(f_disp, filename="expenses_filtered.xlsx")


elif choice == "Charts":
    st.title(msgs.get("charts", "Charts"))

    # ---- Контроли периода ----
    colp1, colp2, colp3 = st.columns([1.4, 1.4, 1])

    start_c = colp1.date_input(
        msgs.get("start", "Start"),
        value=_date(_date.today().year, _date.today().month, 1),
        key="charts_start",  # уникальный ключ
    )

    end_c = colp2.date_input(
        msgs.get("end", "End"), value=_date.today(), key="charts_end"  # уникальный ключ
    )

    apply_c = colp3.button(msgs.get("apply", "Apply"), width="stretch")

    # Грузим данные по периоду
    df_raw = load_df(str(start_c), str(end_c)) if apply_c or True else load_df()
    if df_raw.empty:
        st.info(msgs.get("no_expenses_found", "No expenses found for selected period."))
        st.stop()

    # Очищаем/нормализуем и удаляем возможные дубликаты показа
    df = prepare_df_for_view(df_raw, remove_dups=True)

    # ---- Быстрые фильтры по категориям ----
    cats_all = sorted(df["category"].astype(str).unique().tolist())
    sel_cats = st.multiselect(
        msgs.get("category", "Category"),
        options=cats_all,
        default=cats_all,
    )
    if sel_cats:
        df = df[df["category"].isin(sel_cats)]
    if df.empty:
        st.info(msgs.get("no_expenses_found", "No expenses found for selected period."))
        st.stop()

    st.divider()

    # =========================
    #  A) Бар-чарт по категориям
    # =========================
    st.subheader(msgs.get("by_category", "By category"))
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
            x=alt.X("category:N", title=msgs.get("category", "Category"), sort="-y"),
            y=alt.Y("amount:Q", title=msgs.get("total", "Total")),
            tooltip=[
                alt.Tooltip("category:N", title=msgs.get("category", "Category")),
                alt.Tooltip("amount:Q", title=msgs.get("total", "Total"), format=".2f"),
            ],
        )
        .properties(height=360)
    )
    st.altair_chart(cat_chart, use_container_width=True)

    # =========================
    #  B) Линия: динамика по датам
    # =========================
    st.subheader(msgs.get("by_date", "By date"))
    daily = (
        df.groupby("date", as_index=False)
        .agg(amount=("amount", "sum"))  # DataFrame
        .sort_values("date")  # сортировка по дате
    )

    line_chart = (
        alt.Chart(daily)
        .mark_line(point=True)
        .encode(
            x=alt.X("date:T", title=msgs.get("date", "Date")),
            y=alt.Y("amount:Q", title=msgs.get("total", "Total")),
            tooltip=[
                alt.Tooltip("date:T", title=msgs.get("date", "Date")),
                alt.Tooltip("amount:Q", title=msgs.get("total", "Total"), format=".2f"),
            ],
        )
        .properties(height=360)
    )
    st.altair_chart(line_chart, use_container_width=True)

    # =========================
    #  C) (опционально) Pie-chart
    # =========================
    with st.expander(msgs.get("share_by_category", "Share by category (pie)")):
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
                    legend=alt.Legend(title=msgs.get("category", "Category")),
                ),
                tooltip=[
                    alt.Tooltip("category:N", title=msgs.get("category", "Category")),
                    alt.Tooltip(
                        "amount:Q", title=msgs.get("total", "Total"), format=",.2f"
                    ),
                    alt.Tooltip(
                        "share:Q", title=msgs.get("share", "Share"), format=".1%"
                    ),
                ],
            )
            .properties(height=320)
        )
        st.altair_chart(pie, use_container_width=True)

    # =========================
    #  D) Показ таблицы (без дублей)
    # =========================
    with st.expander(msgs.get("show_table", "Show data")):
        st.dataframe(
            (df.sort_values("date", ascending=False)).reset_index(drop=True),
            width="stretch",
            hide_index=True,
            column_config={
                "amount": st.column_config.NumberColumn(
                    msgs.get("amount", "Amount"), format="%.2f"
                ),
                "date": st.column_config.DatetimeColumn(
                    msgs.get("date", "Date"), format="YYYY-MM-DD"
                ),
            },
        )


elif choice == "Settings":
    st.title(msgs.get("settings", "Settings"))

    # ---- язык интерфейса ----
    lang = st.selectbox(
        "Language",
        options=["en", "fr", "es"],
        index=["en", "fr", "es"].index(st.session_state.get("lang", "en")),
        help="UI language",
    )
    st.session_state["lang"] = lang
    msgs = messages[lang]

    st.divider()

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
st.subheader("User / Profile")

# текущее значение
current = st.session_state.setdefault("current_user", "default")
users = list_users()
if current not in users:
    # если активного нет в списке (после ручных манипуляций с файлами) — приводим в порядок
    current = users[0]
    st.session_state["current_user"] = current

# первая строка: Active user + Create / rename user + Create
c1, c2, c3 = st.columns([1.2, 1.2, 0.6])
with c1:
    sel = st.selectbox(
        "Active user", users, index=users.index(current), key="settings_active_user"
    )
with c2:
    new_name = st.text_input(
        "Create / rename user",
        value="",
        placeholder="Type name",
        key="settings_new_name",
    ).strip()
with c3:
    if st.button("Create", key="settings_btn_create"):
        if not new_name:
            st.warning("Please enter a name.")
        elif new_name in users:
            st.warning("User with this name already exists.")
        else:
            # лениво создаём БД (ensure_db) и переключаемся
            ensure_db(get_db_path(new_name))
            switch_user(new_name, toast="Created & switched")

# подпись с файлами активного пользователя
dbf, limf = files_for(sel)
st.caption(f"DB:  {dbf.name}  —  Limits:  {limf.name}")

# вторая строка: архивирование + Delete + Rename
c4, c5, c6 = st.columns([0.9, 0.7, 0.7])
with c4:
    do_archive = st.checkbox(
        "Archive before delete", value=True, key="settings_archive_before_delete"
    )

with c5:
    disable_delete = len(users) <= 1
    if st.button("Delete user", disabled=disable_delete, key="settings_btn_delete"):
        if disable_delete:
            st.info("You cannot delete the last remaining user.")
        else:
            try:
                if do_archive:
                    archive_user(sel)
                else:
                    # удаляем файлы без архивации
                    if dbf.exists():
                        dbf.unlink()
                    if limf.exists():
                        limf.unlink()
                # после удаления выбираем другого юзера
                remaining = [u for u in list_users() if u != sel]
                switch_user(
                    remaining[0] if remaining else "default", toast="Deleted, switched"
                )
            except Exception as e:
                st.error("Deletion failed.")
                st.exception(e)

with c6:
    # отдельная кнопка Rename для текущего sel → new_name
    if st.button("Rename", key="settings_btn_rename"):
        if not new_name:
            st.warning("Please enter a new name.")
        elif new_name in users:
            st.warning("User with this name already exists.")
        elif sel == "default":
            # при желании можно запретить переименование default — уберите этот блок, если не нужно
            try:
                rename_user(sel, new_name)
                switch_user(new_name, toast="Renamed & switched")
            except Exception as e:
                st.error("Rename failed.")
                st.exception(e)
        else:
            try:
                rename_user(sel, new_name)
                switch_user(new_name, toast="Renamed & switched")
            except Exception as e:
                st.error("Rename failed.")
                st.exception(e)

# быстрый свитч, если пользователь в select изменён
if sel != current and st.session_state.get("settings_active_user") == sel:
    switch_user(sel, toast="Switched")

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


# ---------------- UI ----------------

st.subheader("Monthly limits")

# Показываем активные файлы
db_path_str, limits_file = _active_paths()
st.caption(f"DB: {db_path_str} — Limits: {limits_file.name}")

# 1) Выбор месяца (уникальный key обязателен)
month = st.date_input(
    "Month",
    value=date.today().replace(day=1),
    format="YYYY/MM/DD",
    key="limits_month",
)
mk = _mk(month)

# 2) Загружаем лимиты и категории
cats = _categories_for_editor(db_path_str)
limits_now = _load_limits(mk, limits_file)

# 3) Редактор лимитов
st.write(f"User: {_active_user()} • Month: {mk}")

values: dict[str, float] = {}
for cat in cats:
    values[cat] = st.number_input(
        cat,
        min_value=0.0,
        step=10.0,
        value=float(limits_now.get(cat, 0.0)),
        key=f"limit_{mk}_{cat}",  # уникальные ключи на месяц+категорию
    )

# 4) Кнопки управления (Save / Clear)
col1, col2 = st.columns(2)
with col1:
    if st.button("Save", type="primary", key=f"save_limits_{mk}"):
        _save_limits(mk, values, limits_file)
        st.session_state["_flash"] = ("Limits saved", "✅")
        st.cache_data.clear()
        st.rerun()

with col2:
    if st.button("Clear month limits", key=f"clear_limits_{mk}"):
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
        label=msgs.get("download_csv", "Download CSV"),
        data=csv_bytes,
        file_name=f"{current_user}_{mk}_limits.csv",
        mime="text/csv",
        key=f"dl_limits_csv_{current_user}_{mk}",
        help=msgs.get("download_csv", "Download CSV"),
    )

# --- Import CSV
with exp_col2:
    up = st.file_uploader(
        msgs.get("upload_csv", "Upload CSV"),
        type=["csv"],
        key=f"ul_limits_csv_{current_user}_{mk}",
        help=msgs.get("upload_csv", "Upload CSV"),
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
            st.session_state["_flash"] = (msgs.get("saved", "Saved!"), "✅")
            st.cache_data.clear()
            st.rerun()

        except Exception:
            st.error(msgs.get("csv_import_failed", "CSV import failed"))

# ---- Change log (session) ----------------------------------------------------
st.markdown(f"#### {msgs.get('change_log', 'Change log (session)')}")

log_col1, log_col2, log_col3, log_col4 = st.columns(4)

audit_data = get_audit()  # список записей аудита за сессию

with log_col1:
    st.download_button(
        label=msgs.get("download_json", "Download JSON"),
        data=audit_to_json_bytes(audit_data),
        file_name=f"audit_{current_user}_{mk}.json",
        mime="application/json",
        key=f"dl_audit_json_{current_user}_{mk}",
    )

with log_col2:
    st.download_button(
        label=msgs.get("download_csv", "Download CSV"),
        data=audit_to_csv_bytes(audit_data),
        file_name=f"audit_{current_user}_{mk}.csv",
        mime="text/csv",
        key=f"dl_audit_csv_{current_user}_{mk}",
    )

with log_col4:
    if st.button(
        msgs.get("clear_audit", "Clear audit"),
        key=f"btn_clear_audit_{current_user}_{mk}",
    ):
        st.session_state.setdefault("__limits_audit__", [])
        st.session_state["__limits_audit__"].clear()
        st.success(msgs.get("cleared", "Cleared!"))

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
