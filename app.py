import json
import re
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

from db import add_expense, get_expenses_df, list_categories

# CSV/аудит для лимитов
from limits_tools import (
    append_audit_row,
    audit_to_csv_bytes,
    audit_to_json_bytes,
    csv_bytes_to_limits,
    limits_to_csv_bytes,
    get_audit,
)

from messages import messages
from utils import (
    DATA_DIR,
    create_user,
    db_path_for,
    delete_user,
    limits_path_for,
    list_users,
    load_monthly_limits,
    month_key,
    save_monthly_limits,
    user_files,
)

# --- aliases for tests (test_limits_io.py expects underscored names)
_limits_to_csv_bytes = limits_to_csv_bytes

st.session_state.setdefault("current_user", "default")
current_user = st.session_state["current_user"]

ACTIVE_DB_PATH = db_path_for(current_user)  # data/default_expenses.db
ACTIVE_LIMITS_PATH = limits_path_for(current_user)  # data/default/budget_limits.json

# делаем пути видимыми для других модулей через session_state
st.session_state["ACTIVE_DB_PATH"] = ACTIVE_DB_PATH
st.session_state["ACTIVE_LIMITS_PATH"] = str(ACTIVE_LIMITS_PATH)


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


# ---- flash-toast from previous run ----
_flash = st.session_state.pop("_flash", None)
if _flash:
    # _flash: tuple[str, str|None] -> (message, icon)
    msg, icon = (_flash + (None,))[:2]
    st.toast(msg, icon=icon)


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


# --- Меню ---
menu = ["Dashboard", "Add Expense", "Browse & Filter", "Charts", "Settings"]
choice = st.sidebar.radio("Menu", menu)

if choice == "Dashboard":
    st.title(msgs.get("dashboard", "Dashboard"))

    # ----- Период по умолчанию: текущий месяц -----
    today = date.today()
    month_start = today.replace(day=1)

    c1, c2, c3 = st.columns((1, 1, 0.5))
    with c1:
        start_d = st.date_input(
            "Start",
            value=st.session_state.get("dash_start", month_start),
            key="dash_start",  # ← уникальный ключ
        )
    with c2:
        end_d = st.date_input(
            "End",
            value=st.session_state.get("dash_end", today),
            key="dash_end",  # ← уникальный ключ
        )
    with c3:
        refresh = st.button(
            "Apply", key="dash_apply"
        )  # (не обязательно, но тоже даём ключ)

    # запомним выбор
    if refresh:
        st.session_state["dash_start"] = start_d
        st.session_state["dash_end"] = end_d

    start_s = st.session_state.get("dash_start", month_start).strftime("%Y-%m-%d")
    end_s = st.session_state.get("dash_end", today).strftime("%Y-%m-%d")

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

elif choice == "Add Expense":
    st.title(msgs.get("add_expense", "Add Expense"))
    # --- persistent success banner after rerun ---
    if st.session_state.pop("expense_added_banner", False):
        st.markdown(
            """
            <div style="
                background-color: #d4edda;
                color: #155724;
                padding: 10px 16px;
                border-radius: 8px;
                border: 1px solid #c3e6cb;
                font-size: 16px;
                margin: 10px 0;
                display: flex;
                align-items: center;
            ">
                ✅ <span style="margin-left: 10px;">Expense added successfully!</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # 1) активная БД (мы её уже кладём в session_state)
    db_path = st.session_state.get("ACTIVE_DB_PATH", "data/default_expenses.db")

    # 2) список категорий из активной БД (фоллбэк на прямой SQL)
    try:
        # если у вас есть хелпер list_categories(db_path=...), раскомментируйте:
        # from db import list_categories as _list_categories
        # cats = _list_categories(db_path=db_path)

        with sqlite3.connect(db_path) as _conn:
            rows = _conn.execute(
                "SELECT DISTINCT category FROM expenses ORDER BY category"
            ).fetchall()
        cats = [r[0] for r in rows if r and r[0]]
    except Exception:
        cats = []

    with st.form("add_expense_form", clear_on_submit=True):
        # дата
        d = st.date_input(
            msgs.get("date", "Date"),
            key=f"add_expense_date_{st.session_state.get('current_user', 'default')}",
        )
        date_err = st.empty()

        # режим ввода категории: выбрать/новая
        col_left, col_right = st.columns([1, 1])
        with col_left:
            mode = st.radio(
                msgs.get("category", "Category"),
                options=["choose", "new"],
                index=0 if cats else 1,
                horizontal=True,
                captions=[
                    msgs.get("choose_existing", "Choose existing"),
                    msgs.get("enter_new", "Enter new"),
                ],
            )

        cat_val = None
        cat_err = st.empty()
        if mode == "choose":
            cat_val = st.selectbox(
                msgs.get("choose_category", "Choose category"),
                options=cats,
                index=0 if cats else None,
                placeholder=msgs.get("placeholder_category", "No categories yet"),
            )
        else:
            cat_val = st.text_input(
                msgs.get("new_category", "New category"),
                placeholder=msgs.get("placeholder_new_category", "Type category name"),
            )

        amount = st.number_input(
            msgs.get("amount", "Amount"),
            min_value=0.0,
            format="%.2f",
        )
        amount_err = st.empty()

        note = st.text_area(msgs.get("description", "Description"))

        submit = st.form_submit_button(msgs.get("submit", "Submit"))

        if submit:
            has_error = False

            # валидации
            if not d:
                date_err.error(msgs.get("error_date", "Please select a date."))
                has_error = True
            else:
                date_err.empty()

            category_norm = (cat_val or "").strip()
            if not category_norm:
                cat_err.error(
                    msgs.get("error_category", "Please enter / choose a category.")
                )
                has_error = True
            else:
                cat_err.empty()

            amt = float(amount)
            if amt <= 0:
                amount_err.error(
                    msgs.get("error_amount", "Amount must be greater than zero.")
                )
                has_error = True
            else:
                amount_err.empty()

            # сохранение
            if not has_error:
                try:
                    # если ваша add_expense поддерживает явный путь — можно так:
                    # add_expense(
                    #     date=str(d),
                    #     category=category_norm,
                    #     amount=amt,
                    #     description=(note or "").strip(),
                    #     db_path=db_path,
                    # )

                    # у вас add_expense уже резолвит путь через session_state → достаточно так:
                    add_expense(
                        date=str(d),
                        category=category_norm,
                        amount=amt,
                        description=(note or "").strip(),
                    )

                    # уведомления + перерисовка страницы
                    st.success(msgs.get("expense_added", "Expense added successfully!"))

                    # тост через flash-механизм
                    st.session_state["_flash"] = (
                        msgs.get("expense_added", "Expense added successfully!"),
                        "✅",
                    )

                    # сброс кэша и мягкая перерисовка страницы
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

    # ---- flash от предыдущей операции ----
    flash = st.session_state.pop("flash", None)
    if flash:
        kind, text = flash  # "success"|"info"|"warning"|"error"
        {
            "success": st.success,
            "info": st.info,
            "warning": st.warning,
            "error": st.error,
        }.get(kind, st.info)(text)

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

    # --- Users / Profiles -------------------------------------------------
st.subheader("User / Profile")

# Текущий пользователь в сессии
current_user: str = st.session_state.get("current_user", "default")

# Список профилей
users = list_users()
if current_user not in users:
    current_user = users[0]
    st.session_state["current_user"] = current_user

col_u1, col_u2, col_u3 = st.columns([2, 2, 1])

with col_u1:
    sel = st.selectbox("Active user", users, index=users.index(current_user))
    if sel != current_user:
        st.session_state["current_user"] = sel
        st.rerun()

with col_u2:
    new_name = st.text_input("Create / rename user", value="")

with col_u3:
    st.caption("")  # выравнивание
    if st.button("Create", use_container_width=True):
        u = create_user(new_name or "user")
        st.session_state["current_user"] = u
        st.success(f"User '{u}' is ready.")
        st.rerun()

# Информация о файлах профиля
db_path, limits_path = user_files(current_user)
st.caption(f"DB: `{db_path.name}`  —  Limits: `{limits_path.name}`")

# Удаление/архивирование
del_c1, del_c2, del_c3 = st.columns([1, 1, 2])
with del_c1:
    archive_before = st.checkbox("Archive before delete", value=True)
with del_c2:
    danger = st.button("Delete user", type="secondary")
with del_c3:
    st.caption("You cannot delete the last remaining user.")

if danger:
    if len(users) <= 1:
        st.error("Cannot delete the only user.")
    else:
        key = f"confirm_del_{current_user}"
        st.session_state[key] = True

# второй шаг подтверждения
key = f"confirm_del_{current_user}"
if st.session_state.get(key):
    st.warning(f"Delete user '{current_user}'? This action cannot be undone.")
    c1, c2 = st.columns([1, 4])
    with c1:
        if st.button("Yes, delete", type="primary"):
            delete_user(current_user, archive=archive_before)
            # переключимся на оставшегося
            left = list_users()
            st.session_state.pop(key, None)
            st.session_state["current_user"] = left[0] if left else "default"
            st.success("User deleted.")
            st.rerun()
    with c2:
        if st.button("Cancel"):
            st.session_state.pop(key, None)
            st.info("Cancelled.")
            st.rerun()


# 1) собрать список профилей из папки data (ищем <user>_expenses.db и <user>_budget_limits.json)
def _list_users() -> list[str]:
    users: set[str] = set()
    DATA_DIR.mkdir(exist_ok=True)
    for p in Path(DATA_DIR).glob("*_expenses.db"):
        users.add(p.name.replace("_expenses.db", ""))
    for p in Path(DATA_DIR).glob("*_budget_limits.json"):
        users.add(p.name.replace("_budget_limits.json", ""))
    if not users:
        users.add("default")
    return sorted(users)


users = _list_users()

# 2) активный пользователь (в session_state хранится навсегда для сессии)
current_user: str = st.session_state.get("current_user", "default")
if current_user not in users:
    current_user = "default"

col_u1, col_u2 = st.columns([2, 1])

with col_u1:
    sel = st.selectbox(
        "Active user",
        users,
        index=users.index(current_user) if current_user in users else 0,
        help="Pick a profile to work with",
    )

with col_u2:
    # форма создания нового профиля
    with st.popover("New profile"):
        st.write("Allowed: letters, digits, _ and -")
        new_name = st.text_input("Profile name", "")
        create = st.button("Create", type="primary", use_container_width=True)
        if create:
            name = new_name.strip().lower()
            if not name:
                st.error("Empty name.")
            elif not re.fullmatch(r"[a-z0-9_-]{1,32}", name):
                st.error("Only [a-z0-9_-], up to 32 chars.")
            else:
                # создать пустые файлы путей (БД появится по первой записи)
                DATA_DIR.mkdir(exist_ok=True)
                # создаём пустой файл лимитов, если его нет
                limits_path_for(name).write_text("{}", encoding="utf-8")
                # расширяем список и сразу делаем активным
                users = sorted(set(users) | {name})
                st.session_state["current_user"] = name
                st.success(f"Profile '{name}' created and selected.")
                st.rerun()

# переключение активного пользователя
if sel != current_user:
    st.session_state["current_user"] = sel
    st.session_state["_flash"] = (f"Switched to '{sel}'", "🆕")
    st.rerun()

current_user = st.session_state["current_user"]
st.caption(
    f"Data files:  DB → `{db_path_for(current_user)}`,  limits → `{limits_path_for(current_user)}`"
)

st.divider()

# --- Monthly limits ----------------------------------------------------------


def _active_paths() -> tuple[str, Path]:
    """Берём активные пути из твоего get_active_paths();
    возвращаем: (db_path как str, limits_path как Path)."""
    db_p_obj, limits_p_obj = get_active_paths()
    db_p_str = str(db_p_obj)
    limits_p = limits_p_obj if isinstance(limits_p_obj, Path) else Path(limits_p_obj)
    return db_p_str, limits_p


def _categories_for_editor(db_path: str) -> list[str]:
    """Надёжно получаем список категорий из БД.
    Всегда возвращаем список (даже при ошибках)."""
    try:
        cats = list_categories(db_path=db_path) or []
    except Exception:
        cats = []
    # базовая подстраховка
    base = {"food", "transport", "groceries", "utilities", "entertainment", "other"}
    return sorted(set(cats) | base)


def _mk(d: date) -> str:
    """Ключ месяца вида 'YYYY-MM' (используем твою month_key)."""
    return month_key(d)


def _load_limits(mk: str, path: Path) -> dict[str, float]:
    """Читаем лимиты из JSON-файла; на ошибки реагируем мягко."""
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        raw = data.get(mk, {}) or {}
        return {k: float(v) for k, v in raw.items()}
    except Exception:
        return {}


def _save_limits(mk: str, values: dict[str, float], path: Path) -> None:
    """Сохраняем лимиты для выбранного месяца в JSON-файл."""
    data = {}
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8")) or {}
        except Exception:
            data = {}
    data[mk] = values
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


st.subheader("Monthly limits")

# показываем активные файлы (для уверенности)
db_path_str, limits_path = _active_paths()
st.caption(f"DB: {db_path_str} — Limits: {limits_path.name}")

# 1) Выбор месяца (уникальный key обязателен!)
month = st.date_input(
    "Month",
    value=date.today().replace(day=1),
    format="YYYY/MM/DD",
    key="limits_month",
)
mk = _mk(month)

# 2) Загружаем лимиты и категории
cats = _categories_for_editor(db_path_str)
limits_now = _load_limits(mk, limits_path)

# 3) Редактор лимитов (по всем категориям)
st.write(f"User: {get_active_user()} • Month: {mk}")

values: dict[str, float] = {}
for cat in cats:
    values[cat] = st.number_input(
        cat,
        min_value=0.0,
        step=10.0,
        value=float(limits_now.get(cat, 0.0)),
        key=f"limit_{mk}_{cat}",  # уникальные ключи на месяц+категорию
    )

# 4) Кнопки управления (Save / Clear) в две колонки
col1, col2 = st.columns(2)
with col1:
    if st.button("Save", type="primary", key=f"save_limits_{mk}"):
        _save_limits(mk, values, limits_path)
        st.session_state["_flash"] = ("Limits saved", "✅")
        st.cache_data.clear()
        st.rerun()

with col2:
    if st.button("Clear month limits", key=f"clear_limits_{mk}"):
        _save_limits(mk, {}, limits_path)
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
