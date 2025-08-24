import streamlit as st
import sqlite3
import io, csv
from io import BytesIO
from pathlib import Path
from datetime import datetime
import pandas as pd

from messages import messages
from db import (
    get_expenses_df,
    add_expense,
    list_categories as _list_categories,
    list_categories,
)

import pandas as pd
from datetime import date, timedelta
from project import check_budget_limits
from datetime import datetime
from datetime import date as _date
import altair as alt

import json
from datetime import date
from utils import load_monthly_limits, save_monthly_limits, month_key
from db import list_categories
from project import check_budget_limits


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
    for month_key, limits in obj.items():
        if not isinstance(month_key, str) or len(month_key) != 7 or month_key[4] != "-":
            raise ValueError(f"Invalid month key: {month_key!r}")
        if not isinstance(limits, dict):
            raise ValueError(
                f"Month {month_key} must map to an object of category->limit"
            )
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
        out[month_key] = inner
    return out


def _month_key(date_value):
    """
    Преобразует дату в строку формата YYYY-MM для работы с месячными лимитами
    """
    return date_value.strftime("%Y-%m")


AUDIT_FILE = Path("tools/limits_audit.csv")  # можно сменить путь при желании


def _limits_to_csv_bytes(limits: dict[str, float]) -> bytes:
    """Словарь лимитов -> CSV (bytes)."""
    df = pd.DataFrame(
        [(k, float(v)) for k, v in (limits or {}).items()],
        columns=["category", "limit"],
    )
    return df.to_csv(index=False).encode("utf-8")


def _parse_limits_csv(file) -> dict[str, float]:
    """CSV -> словарь лимитов {category: limit}."""
    df = pd.read_csv(file)
    out: dict[str, float] = {}
    for _, row in df.iterrows():
        if pd.isna(row.get("category")):
            continue
        cat = str(row["category"]).strip()
        try:
            out[cat] = float(row["limit"])
        except Exception:
            continue
    return out


def _append_audit_row(kind: str, month_key: str, before: dict, after: dict) -> None:
    """Мини-лог изменений лимитов в session_state."""
    log = st.session_state.setdefault("audit", [])
    log.append(
        {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "kind": kind,
            "month": month_key,
            "before": before,
            "after": after,
        }
    )


# ===== ЛОГ ПЕРЕЗАПУСКА =====
print(f"\n🔄 Streamlit перезапущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")


# ===== Вспомогательные функции =====
@st.cache_data(ttl=60)
def load_df(start: str | None = None, end: str | None = None) -> pd.DataFrame:
    """
    Загружает операции из базы данных как DataFrame.
    Параметры: start, end — строки в формате 'YYYY-MM-DD' или None.
    """
    df = get_expenses_df(start_date=start, end_date=end)
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


# --- Состояние сеанса ---
if "lang" not in st.session_state:
    st.session_state["lang"] = "en"

msgs = messages[st.session_state["lang"]]

st.set_page_config(page_title="ExpenseAnalyzer", layout="wide")


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
        df = get_expenses_df()
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

    c1, c2, c3 = st.columns([1, 1, 0.5])
    with c1:
        start_d = st.date_input(
            "Start", value=st.session_state.get("dash_start", month_start)
        )
    with c2:
        end_d = st.date_input("End", value=st.session_state.get("dash_end", today))
    with c3:
        refresh = st.button("Apply")

    # запомним выбор
    if refresh:
        st.session_state["dash_start"] = start_d
        st.session_state["dash_end"] = end_d

    start_s = (st.session_state.get("dash_start", month_start)).strftime("%Y-%m-%d")
    end_s = (st.session_state.get("dash_end", today)).strftime("%Y-%m-%d")

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
    st.dataframe(
        df.sort_values("date", ascending=False)[show_cols].head(5),
        use_container_width=True,
        hide_index=True,
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

    # Получаем список категорий из БД
    try:
        conn = sqlite3.connect("expenses.db")
        cats = [
            row[0]
            for row in conn.execute("SELECT DISTINCT category FROM expenses").fetchall()
        ]
        conn.close()
    except Exception:
        cats = []  # fallback, если таблица пуста

    with st.form("add_expense_form", clear_on_submit=True):
        d = st.date_input(msgs.get("date", "Date"))
        date_err = st.empty()

        # Меняем текстовое поле на selectbox с возможностью ввода
        category = st.selectbox(
            msgs.get("category", "Category"),
            options=cats if cats else ["(Enter new category)"],
            index=0 if cats else None,
            placeholder="Choose or type a category",
        )
        category_err = st.empty()

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

            # Дата
            if not d:
                date_err.error(msgs.get("error_date", "Please select a date."))
                has_error = True
            else:
                date_err.empty()

            # Категория
            cat_norm = (category or "").strip()
            if not cat_norm or cat_norm == "(Enter new category)":
                category_err.error(
                    msgs.get("error_category", "Please enter a valid category.")
                )
                has_error = True
            else:
                category_err.empty()

            # Сумма
            amt = float(amount)
            if amt <= 0:
                amount_err.error(
                    msgs.get("error_amount", "Amount must be greater than zero.")
                )
                has_error = True
            else:
                amount_err.empty()

            # Сохранение
            if not has_error:
                note_norm = (note or "").strip() or None
                try:
                    add_expense(
                        date=str(d),
                        category=cat_norm,
                        amount=amt,
                        description=note_norm,
                    )
                    st.success(msgs.get("expense_added", "Expense added successfully!"))
                    st.toast(
                        msgs.get("expense_added", "Expense added successfully!"),
                        icon="✅",
                    )
                    st.rerun()
                except Exception as ex:
                    st.error(msgs.get("save_error", "Could not save expense."))
                    st.exception(ex)

elif choice == "Browse & Filter":
    st.title(msgs.get("browse_filter", "Browse & Filter"))

    # Загружаем все данные
    base_df = get_expenses_df()
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
                "Start", value=min_date, min_value=min_date, max_value=max_date
            )
        with c2:
            end = st.date_input(
                "End", value=max_date, min_value=min_date, max_value=max_date
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
        use_container_width=True,
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

    # ---- Контролы периода ----
    colp1, colp2, colp3 = st.columns([1.4, 1.4, 1])
    start_c = colp1.date_input(
        msgs.get("start", "Start"),
        value=_date(_date.today().year, _date.today().month, 1),
    )
    end_c = colp2.date_input(msgs.get("end", "End"), value=_date.today())
    apply_c = colp3.button(msgs.get("apply", "Apply"), use_container_width=True)

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

    # A) Бар-чарт по категориям
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

    # B) Линия: динамика по датам
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
            use_container_width=True,
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

    # ===== UI Language Selector =====
    lang = st.selectbox(
        "Language",
        options=["en", "fr", "es"],
        index=["en", "fr", "es"].index(st.session_state.get("lang", "en")),
        help="Select the interface language",
    )
    st.session_state["lang"] = lang
    msgs = messages[lang]

    st.divider()

    # ===== Month and Categories =====
    col1, col2 = st.columns(2)

    with col1:
        month = st.date_input(
            msgs.get("month", "Month"),
            value=date.today().replace(day=1),
            format="YYYY/MM/DD",
        )
        # Генерация ключа месяца, например "2025-08"
        month_key_value = month_key(month)

    with col2:
        try:
            categories = list_categories() or []
        except Exception:
            categories = []

        st.caption(
            msgs.get("categories", "Categories")
            + f": {', '.join(categories) if categories else '-'}"
        )

    st.divider()

    # ===== Edit Monthly Limits =====
    st.subheader(msgs.get("edit_limits", "Edit Monthly Limits"))

    # Загружаем текущие лимиты
    limits = load_monthly_limits().get(month_key_value, {})
    new_limits = {}

    for cat in categories:
        new_limits[cat] = st.number_input(
            f"{cat}",
            min_value=0.0,
            value=float(limits.get(cat, 0.0)),
            step=10.0,
            format="%.2f",
        )

    # Кнопки управления
    col1, col2 = st.columns(2)
    with col1:
        if st.button(
            msgs.get("save", "Save"), type="primary", use_container_width=True
        ):
            all_limits = load_monthly_limits()
            all_limits[month_key_value] = new_limits
            save_monthly_limits(all_limits)
            st.success(msgs.get("saved", "Saved!"))
            st.rerun()

    with col2:
        if st.button(
            msgs.get("clear_limits", "Clear month limits"), use_container_width=True
        ):
            all_limits = load_monthly_limits()
            if month_key_value in all_limits:
                del all_limits[month_key_value]
                save_monthly_limits(all_limits)
                st.success(msgs.get("cleared", "Cleared!"))
                st.rerun()

    st.divider()

    # ===== Import / Export =====
    st.subheader("Import / Export (JSON)")

    col1, col2 = st.columns(2)

    with col1:
        all_limits = load_monthly_limits()
        json_data = json.dumps(all_limits, indent=2, ensure_ascii=False)
        st.download_button(
            label="Export limits (JSON)",
            data=json_data,
            file_name="budget_limits.json",
            mime="application/json",
        )

    with col2:
        uploaded_file = st.file_uploader("Import limits (JSON)", type="json")
        if uploaded_file:
            try:
                imported = json.load(uploaded_file)
                save_monthly_limits(imported)
                st.success("Limits imported successfully!")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to import limits: {e}")
