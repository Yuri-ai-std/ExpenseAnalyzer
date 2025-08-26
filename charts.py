# charts.py — чистая версия без db.totals_by_*
from __future__ import annotations

# --- i18n ---
from messages import messages as M


def t(key: str, lang: str = "en") -> str:
    return M.get(lang, M.get("en", {})).get(key, key)


# --- stdlib ---
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional

# --- third-party ---
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import pandas as pd

# --- project db helpers ---
import db  # используем только db.get_expenses_df(...)

# ---------------------------
# Validation & helpers
# ---------------------------
REQUIRED_COLUMNS = {"date", "category", "amount"}


def _require_dataframe(df: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError("Отсутствуют колонки: " + ", ".join(sorted(missing)))
    if df.empty:
        raise ValueError("Нет данных для построения графика (после фильтра).")


def _sig(
    start: Optional[str] = None,
    end: Optional[str] = None,
    category: Optional[str] = None,
) -> str:
    parts = []
    if start or end:
        parts.append(f"{start or '1900-01-01'}_{end or '9999-12-31'}")
    if category:
        parts.append(str(category))
    return ("_" + "_".join(parts)) if parts else ""


def _parse_date(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    datetime.strptime(s, "%Y-%m-%d")
    return s


# ---------------------------
# Aggregations (pandas-only)
# ---------------------------
def _totals_by_month(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    df2["date"] = pd.to_datetime(df2["date"], errors="coerce")
    df2 = df2.dropna(subset=["date"])
    df2["month"] = df2["date"].dt.strftime("%Y-%m")
    agg = (
        df2.groupby("month", as_index=False)
        .agg(amount=("amount", "sum"))
        .sort_values(by="month")
        .loc[:, ["month", "amount"]]
    )
    return agg


def _totals_by_category(df: pd.DataFrame) -> pd.DataFrame:
    agg = (
        df.groupby("category", as_index=False)
        .agg(amount=("amount", "sum"))
        .sort_values(by="amount", ascending=False)
        .loc[:, ["category", "amount"]]
    )
    return agg


# ---------------------------
# Plot functions
# ---------------------------
def plot_monthly_totals(
    df: pd.DataFrame,
    outdir: Path,
    *,
    start: Optional[str] = None,
    end: Optional[str] = None,
    category: Optional[str] = None,
    lang: str = "en",
) -> Path:
    _require_dataframe(df)
    monthly = _totals_by_month(df)
    if monthly.empty:
        raise ValueError("Нет данных для графика по месяцам (после фильтра).")

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(monthly["month"], monthly["amount"], marker="o")
    ax.set_title(t("chart_monthly_title", lang))
    ax.set_xlabel(t("chart_month", lang))
    ax.set_ylabel(t("chart_amount", lang))
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, p: f"{x:,.0f}"))
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    name = f"monthly{_sig(start, end, category)}.png"
    out_path = outdir / name
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def plot_category_totals(
    df: pd.DataFrame,
    outdir: Path,
    *,
    start: Optional[str] = None,
    end: Optional[str] = None,
    category: Optional[str] = None,
    lang: str = "en",
) -> Path:
    _require_dataframe(df)
    cat = _totals_by_category(df)
    if cat.empty:
        raise ValueError("Нет данных для графика по категориям (после фильтра).")

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(cat["category"], cat["amount"])
    ax.set_title(t("chart_categories_title", lang))
    ax.set_xlabel(t("chart_category", lang))
    ax.set_ylabel(t("chart_amount", lang))
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, p: f"{x:,.0f}"))
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    name = f"categories{_sig(start, end, category)}.png"
    out_path = outdir / name
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


# ---------------------------
# Facade
# ---------------------------
def show_charts(
    out_dir: Path,
    *,
    start: Optional[str] = None,
    end: Optional[str] = None,
    category: Optional[str] = None,
    lang: str = "en",
    do_monthly: bool = True,
    do_category: bool = True,
) -> list[Path]:
    df = db.get_expenses_df(
        db_path=getattr(db, "DB_PATH", "expenses.db"),
        start_date=start,
        end_date=end,
        category=category,
    )
    if df.empty:
        raise ValueError("Нет данных для построения графика (после фильтра).")
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    saved: list[Path] = []
    if do_monthly:
        saved.append(
            plot_monthly_totals(
                df, out_dir, start=start, end=end, category=category, lang=lang
            )
        )
    if do_category:
        saved.append(
            plot_category_totals(
                df, out_dir, start=start, end=end, category=category, lang=lang
            )
        )
    return saved


# ---------------------------
# CLI
# ---------------------------
def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Generate ExpenseAnalyzer charts")
    p.add_argument(
        "--start", dest="start_date", type=str, default=None, help="YYYY-MM-DD"
    )
    p.add_argument("--end", dest="end_date", type=str, default=None, help="YYYY-MM-DD")
    p.add_argument("--category", type=str, default=None, help="Filter by category")
    p.add_argument("--lang", type=str, default="en", choices=("en", "fr", "es"))
    p.add_argument("--outdir", type=Path, default=Path("reports/plots"))
    p.add_argument("--no-monthly", action="store_true", help="Skip monthly chart")
    p.add_argument("--no-category", action="store_true", help="Skip category chart")
    return p


def main() -> None:
    args = _build_argparser().parse_args()
    start = _parse_date(args.start_date)
    end = _parse_date(args.end_date)
    dated_dir = Path(args.outdir) / datetime.now().strftime("%Y-%m-%d")
    saved = show_charts(
        out_dir=dated_dir,
        start=start,
        end=end,
        category=args.category,
        lang=args.lang,
        do_monthly=not args.no_monthly,
        do_category=not args.no_category,
    )
    print("Saved charts:")
    for p in saved:
        print("  ", p)


if __name__ == "__main__":
    main()
