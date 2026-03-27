"""Tests for pure logic in app (charts, parsing, messages)."""

from __future__ import annotations

import pandas as pd
import pytest

import app as matilda_app


def test_palette_for_n_cycles() -> None:
    assert matilda_app._palette_for_n(0) == matilda_app.MATILDA_CHART_PALETTE
    n = len(matilda_app.MATILDA_CHART_PALETTE)
    assert matilda_app._palette_for_n(n + 2)[0] == matilda_app.MATILDA_CHART_PALETTE[0]
    # Index n+1 wraps to palette[1] when len == n+2
    assert matilda_app._palette_for_n(n + 2)[n + 1] == matilda_app.MATILDA_CHART_PALETTE[1]


def test_dataframe_for_pie_no_merge_when_small() -> None:
    df = pd.DataFrame({"categoria": ["a", "b"], "valor": [3.0, 7.0]})
    out = matilda_app._dataframe_for_pie(df, max_slices=10)
    assert len(out) == 2


def test_dataframe_for_pie_merges_tail_as_otros() -> None:
    rows = [{"categoria": f"c{i}", "valor": float(i)} for i in range(12)]
    df = pd.DataFrame(rows)
    out = matilda_app._dataframe_for_pie(df, max_slices=10)
    assert len(out) == 10
    assert out.iloc[-1]["categoria"] == "Otros"
    # Top 9 by value are c11..c3; remainder c2+c1+c0
    assert out.iloc[-1]["valor"] == pytest.approx(3.0)


def test_add_percent_column() -> None:
    df = pd.DataFrame({"categoria": ["x"], "valor": [25.0]})
    out = matilda_app._add_percent_column(df)
    assert out["pct_total"].iloc[0] == pytest.approx(100.0)


def test_extract_insight_headline_strips_markdown() -> None:
    text = "**Hola.** Segunda frase."
    assert matilda_app.extract_insight_headline(text) == "Hola."


def test_extract_insight_headline_empty_fallback() -> None:
    assert matilda_app.extract_insight_headline("") == "Insight listo para revisión."


def test_extract_metric_teaser_empty_data() -> None:
    tc: dict = {"tool_name": "obtener_paginas_top"}
    assert matilda_app.extract_metric_teaser(tc, None) == "Métrica principal"
    empty = {"data": pd.DataFrame()}
    assert matilda_app.extract_metric_teaser(tc, empty) == "Métrica principal"


def test_extract_metric_teaser_known_label() -> None:
    df = pd.DataFrame({"page_views": [1, 2]}, index=["a", "b"])
    tc = {"tool_name": "x"}
    assert matilda_app.extract_metric_teaser(tc, {"data": df}) == "Vistas"


def test_parse_copilot_output_splits_sections() -> None:
    content = (
        "📊 **El Dato:** Aquí van números.\n\n"
        "💡 **Interpretación:** Aquí va el análisis."
    )
    parsed = matilda_app.parse_copilot_output(content)
    assert "Aquí van números" in parsed["dato"]
    assert "Aquí va el análisis" in parsed["interpretacion"]


def test_parse_copilot_output_fallback_when_no_pattern() -> None:
    raw = "Texto libre sin secciones."
    parsed = matilda_app.parse_copilot_output(raw)
    assert parsed["dato"] == raw
    assert parsed["interpretacion"] == ""


def test_build_chart_payload_unknown_tool() -> None:
    assert matilda_app.build_chart_payload({"tool_name": "no_existe", "result": {}}) is None


def test_build_chart_payload_empty_records() -> None:
    tc = {"tool_name": "obtener_paginas_top", "result": {"resultados": []}}
    assert matilda_app.build_chart_payload(tc) is None


def test_build_chart_payload_ok() -> None:
    records = [
        {"pagina": "/home", "page_views": 100, "total_interactions": 5},
    ]
    tc = {"tool_name": "obtener_paginas_top", "result": {"resultados": records}}
    payload = matilda_app.build_chart_payload(tc)
    assert payload is not None
    assert payload["title"]
    assert "/home" in payload["data"].index


def test_last_assistant_index() -> None:
    msgs = [
        {"role": "user", "content": "hola"},
        {"role": "assistant", "content": "a"},
        {"role": "user", "content": "b"},
    ]
    assert matilda_app._last_assistant_index(msgs) == 1
    assert matilda_app._last_assistant_index([{"role": "user", "content": "x"}]) == -1


def test_build_error_message_shape() -> None:
    err = RuntimeError("boom")
    msg = matilda_app.build_error_message(err)
    assert msg["role"] == "assistant"
    assert "boom" in msg["content"]
    assert msg["tool_calls"] == []
