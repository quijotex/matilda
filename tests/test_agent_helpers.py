"""Tests for agent configuration helpers (no live Gemini calls)."""

from __future__ import annotations

import pytest

from src import agent


def test_configurar_gemini_requires_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    with pytest.raises(ValueError, match="GEMINI_API_KEY"):
        agent.configurar_gemini()


def test_configurar_gemini_returns_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "test-key-123")
    assert agent.configurar_gemini() == "test-key-123"


def test_candidate_models_dedupes_and_orders(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GEMINI_MODEL", "gemini-2.5-flash")
    out = agent._candidate_models("gemini-2.5-flash")
    assert out[0] == "gemini-2.5-flash"
    assert out == list(dict.fromkeys(out))


def test_build_tool_arguments_binds_defaults() -> None:
    def sample(a: int, b: str = "x") -> None:
        pass

    args = agent._build_tool_arguments(sample, 1)
    assert args == {"a": 1, "b": "x"}


def test_obtener_configuracion_modelo_lists_tools(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GEMINI_MODEL", "gemini-2.5-flash")
    cfg = agent.obtener_configuracion_modelo()
    assert cfg["model_name"] == "gemini-2.5-flash"
    assert cfg["tool_count"] == len(cfg["tools_registradas"])
    assert cfg["tools_registradas"]


def test_consultar_matilda_rejects_empty_prompt() -> None:
    with pytest.raises(ValueError, match="no puede estar vacio"):
        agent.consultar_matilda("   ")
