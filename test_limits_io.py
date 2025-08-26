# test_limits_io.py
import io
import json
from datetime import date

import pytest

from utils import load_monthly_limits, month_key, save_monthly_limits


def test_month_key_basic():
    # helper должен давать YYYY-MM
    assert month_key(date(2025, 8, 23)) == "2025-08"
    assert month_key(date(2024, 1, 1)) == "2024-01"


def test_limits_json_roundtrip(tmp_path):
    """Сохранение/загрузка лимитов в JSON через utils.*"""
    limits = {"food": 70.0, "transport": 50, "entertainment": 0}
    p = tmp_path / "limits.json"

    # пишем
    save_monthly_limits(limits, filename=str(p))
    assert p.exists()

    # читаем обратно
    loaded = load_monthly_limits(filename=str(p))
    # типы значений могут стать float — нормализуем
    loaded = {k: float(v) for k, v in loaded.items()}
    want = {k: float(v) for k, v in limits.items()}

    assert loaded == want


@pytest.mark.skipif(
    pytest.importorskip("app", reason="app.py not importable").__dict__.get(
        "_limits_to_csv_bytes"
    )
    is None,
    reason="CSV helpers not found in app.py",
)
def test_limits_csv_roundtrip():
    """
    Проверяем наши streamlit-хелперы из app.py:
      _limits_to_csv_bytes  -> сериализация в CSV
      _parse_limits_csv     -> парсинг CSV обратно в dict
    """
    from app import _limits_to_csv_bytes, _parse_limits_csv  # type: ignore

    src = {
        k: float(v) for k, v in {"food": 70, "transport": 50, "groceries": 30}.items()
    }

    # в CSV
    csv_bytes = _limits_to_csv_bytes(src)
    assert isinstance(csv_bytes, (bytes, bytearray))
    assert b"category" in csv_bytes and b"limit" in csv_bytes  # есть заголовок

    # обратно из CSV
    parsed = _parse_limits_csv(io.BytesIO(csv_bytes))
    assert set(parsed.keys()) == set(src.keys())
    for k, v in src.items():
        assert parsed[k] == pytest.approx(float(v), rel=1e-9)
