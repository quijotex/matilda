from __future__ import annotations

import html
import re
from typing import Any

import pandas as pd
import streamlit as st

from src.agent import consultar_matilda, obtener_configuracion_modelo


st.set_page_config(
    page_title="Matilda - Data Hub",
    page_icon="🎀",
    layout="wide",
    initial_sidebar_state="expanded",
)

ROLE_QUICK_QUESTIONS: dict[str, list[str]] = {
    "Marketing": [
        "¿Qué canales traen el tráfico de mayor calidad y qué acción recomendarías?",
        "¿Cuáles son las páginas top por vistas e interacciones y qué campañas deberíamos priorizar?",
        "¿Qué patrones observas en las sesiones que llegan a pricing?",
    ],
    "Ventas": [
        "¿Qué páginas llevan más usuarios hacia pricing y cómo aprovecharlo comercialmente?",
        "¿Qué flujos de navegación aparecen con más frecuencia antes de una intención de conversión?",
        "¿Qué segmentos muestran mejor engagement y podrían tener mayor intención de compra?",
    ],
    "Management": [
        "Dame un resumen ejecutivo del abandono rápido y sus principales riesgos de negocio.",
        "¿Dónde vemos más frustración del usuario y qué impacto estratégico podría tener?",
        "¿Qué hallazgos accionables explicarían mejor la salud del funnel digital hoy?",
    ],
}


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&display=swap');

        :root {
            --m-navy: #0B192C;
            --m-surface: #F8F9FA;
            --m-accent: #D2143A;
            --m-ink: #1E293B;
            --m-border: #D9E1EA;
            --m-muted: #64748B;
        }

        html, body,
        [data-testid="stMarkdownContainer"],
        [data-testid="stMarkdownContainer"] p,
        [data-testid="stMarkdownContainer"] li,
        [data-testid="stMarkdownContainer"] span,
        [data-testid="stChatInput"] textarea,
        [data-testid="stSelectbox"],
        .stButton > button,
        input, textarea, select, label, h1, h2, h3, h4, h5, h6 {
            font-family: "JetBrains Mono", monospace !important;
        }

        .stApp {
            background-color: var(--m-surface) !important;
        }

        header[data-testid="stHeader"] {
            background-color: var(--m-surface) !important;
            border-bottom: 1px solid var(--m-border);
        }

        section[data-testid="stSidebar"] {
            background-color: var(--m-navy) !important;
        }

        section[data-testid="stSidebar"] h1,
        section[data-testid="stSidebar"] h2,
        section[data-testid="stSidebar"] h3,
        section[data-testid="stSidebar"] h4,
        section[data-testid="stSidebar"] p,
        section[data-testid="stSidebar"] span,
        section[data-testid="stSidebar"] label,
        section[data-testid="stSidebar"] small,
        section[data-testid="stSidebar"] hr {
            color: #CBD5E1 !important;
            -webkit-text-fill-color: #CBD5E1 !important;
        }

        section[data-testid="stSidebar"] .stButton > button {
            background-color: rgba(255,255,255,0.08) !important;
            background-image: none !important;
            color: #E2E8F0 !important;
            -webkit-text-fill-color: #E2E8F0 !important;
            border: 1px solid rgba(255,255,255,0.15) !important;
            border-radius: 10px !important;
            text-align: left !important;
            padding: 0.55rem 0.75rem !important;
            font-size: 0.82rem !important;
            line-height: 1.4 !important;
            white-space: normal !important;
            height: auto !important;
            transition: all 0.2s ease !important;
        }

        section[data-testid="stSidebar"] .stButton > button p,
        section[data-testid="stSidebar"] .stButton > button span {
            color: #E2E8F0 !important;
            -webkit-text-fill-color: #E2E8F0 !important;
        }

        section[data-testid="stSidebar"] .stButton > button:hover {
            background-color: rgba(210, 20, 58, 0.18) !important;
            background-image: none !important;
            border-color: var(--m-accent) !important;
            color: #FFFFFF !important;
            -webkit-text-fill-color: #FFFFFF !important;
        }

        section[data-testid="stSidebar"] .stButton > button:hover p,
        section[data-testid="stSidebar"] .stButton > button:hover span {
            color: #FFFFFF !important;
            -webkit-text-fill-color: #FFFFFF !important;
        }

        [data-testid="collapsedControl"],
        [data-testid="stSidebarCollapsedControl"] {
            display: none !important;
        }

        section[data-testid="stSidebar"] > div:first-child > button:first-child {
            font-size: 0 !important;
            width: 2rem !important;
            height: 2rem !important;
            min-height: 0 !important;
            padding: 0 !important;
            position: relative;
            background: transparent !important;
            border: none !important;
        }

        section[data-testid="stSidebar"] > div:first-child > button:first-child::after {
            content: "✕";
            font-size: 1rem;
            color: #94A3B8;
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
        }

        .matilda-hero {
            border-top: 6px solid var(--m-accent);
            background: #FFFFFF;
            border-radius: 16px;
            padding: 1.5rem 1.75rem 1.25rem 1.75rem;
            margin-bottom: 1.25rem;
            border: 1px solid var(--m-border);
            box-shadow: 0 4px 20px rgba(11, 25, 44, 0.06);
        }

        .matilda-hero-badge {
            display: inline-block;
            background: rgba(210, 20, 58, 0.07);
            color: var(--m-accent);
            border: 1px solid rgba(210, 20, 58, 0.18);
            padding: 0.25rem 0.6rem;
            border-radius: 999px;
            font-size: 0.75rem;
            font-weight: 500;
            margin-bottom: 0.6rem;
        }

        .matilda-hero-title {
            color: var(--m-navy);
            font-size: 1.75rem;
            font-weight: 700;
            margin: 0 0 0.4rem 0;
        }

        .matilda-hero-sub {
            color: var(--m-muted);
            font-size: 0.88rem;
            line-height: 1.6;
            margin: 0;
        }

        .matilda-info-card {
            background: #FFFFFF;
            border: 1px solid var(--m-border);
            border-radius: 14px;
            padding: 1rem 1.1rem;
            margin-bottom: 0.75rem;
            box-shadow: 0 2px 10px rgba(11, 25, 44, 0.03);
        }

        .matilda-info-card-label {
            color: var(--m-muted);
            font-size: 0.72rem;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            font-weight: 600;
            margin-bottom: 0.5rem;
        }

        .matilda-info-card p,
        .matilda-info-card strong {
            color: var(--m-ink);
            font-size: 0.85rem;
            line-height: 1.65;
        }

        .matilda-response-card {
            background: #FFFFFF;
            border: 1px solid var(--m-border);
            border-left: 4px solid var(--m-accent);
            border-radius: 14px;
            padding: 1.1rem 1.15rem 0.9rem 1.15rem;
            margin-top: 0.5rem;
        }

        .matilda-response-section {
            margin-bottom: 0.85rem;
        }

        .matilda-response-section:last-child {
            margin-bottom: 0;
        }

        .matilda-response-label {
            color: var(--m-navy);
            font-weight: 700;
            font-size: 0.92rem;
            margin-bottom: 0.3rem;
        }

        .matilda-response-body {
            color: var(--m-ink);
            font-size: 0.88rem;
            line-height: 1.75;
        }

        .matilda-chart-caption {
            color: var(--m-muted);
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            margin-bottom: 0.25rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def initialize_state() -> None:
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "selected_role" not in st.session_state:
        st.session_state.selected_role = "Management"
    if "pending_prompt" not in st.session_state:
        st.session_state.pending_prompt = None


def render_header() -> None:
    st.markdown(
        """
        <div class="matilda-hero">
            <div class="matilda-hero-badge">Matilda / Data Hub Corporativo</div>
            <div class="matilda-hero-title">Matilda — Data Hub</div>
            <p class="matilda-hero-sub">
                Analiza comportamiento digital, identifica fricción y transforma datos
                de navegación en decisiones accionables. Visualización inmediata, lectura
                ejecutiva y recomendación de negocio.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar() -> None:
    config = obtener_configuracion_modelo()
    with st.sidebar:
        st.markdown("## 🎀 Matilda")
        st.caption("Asistente analítico para Marketing, Ventas y Management")

        selected_role = st.selectbox(
            "Rol de análisis",
            options=list(ROLE_QUICK_QUESTIONS.keys()),
            index=list(ROLE_QUICK_QUESTIONS.keys()).index(st.session_state.selected_role),
        )
        st.session_state.selected_role = selected_role

        st.markdown("#### Preguntas rápidas")
        for idx, question in enumerate(ROLE_QUICK_QUESTIONS[selected_role]):
            if st.button(question, key=f"quick_{selected_role}_{idx}", use_container_width=True):
                st.session_state.pending_prompt = question

        st.divider()
        st.markdown("#### Motor")
        st.caption(f"Modelo: {config['model_name']}")
        st.caption(f"Fallbacks: {', '.join(config['fallback_models']) or 'ninguno'}")
        st.caption(f"Tools activas: {config['tool_count']}")


def _escape_with_breaks(value: str) -> str:
    return html.escape(value).replace("\n", "<br>")


def parse_copilot_output(content: str) -> dict[str, str]:
    pattern = re.compile(
        r"📊\s*(?:\*\*)?El Dato(?:\*\*)?:?\s*(?P<dato>.*?)(?:💡\s*(?:\*\*)?Interpretación(?:\*\*)?:?\s*(?P<interpretacion>.*))?$",
        re.DOTALL,
    )
    match = pattern.search(content.strip())
    if not match:
        return {"dato": content.strip(), "interpretacion": ""}

    dato = (match.group("dato") or "").strip()
    interpretacion = (match.group("interpretacion") or "").strip()
    return {"dato": dato, "interpretacion": interpretacion}


def build_chart_payload(tool_call: dict[str, Any]) -> dict[str, Any] | None:
    tool_name = tool_call.get("tool_name")
    result = tool_call.get("result", {})

    chart_configs: dict[str, dict[str, Any]] = {
        "obtener_paginas_top": {
            "key": "resultados",
            "cols": ["pagina", "page_views", "total_interactions"],
            "index": "pagina",
            "title": "Páginas top por vistas e interacciones",
        },
        "calcular_tasas_abandono": {
            "key": "paginas_criticas",
            "cols": ["pagina", "tasa_abandono_pct"],
            "index": "pagina",
            "title": "Páginas con mayor tasa de abandono rápido",
        },
        "analizar_patrones_conversion": {
            "key": "paginas_entrada_top",
            "cols": ["pagina_entrada", "tasa_pricing_pct"],
            "index": "pagina_entrada",
            "title": "Páginas de entrada con mayor llegada a pricing",
        },
        "obtener_flujos_frecuentes": {
            "key": "resultados",
            "cols": ["ruta_flujo", "sesiones"],
            "index": "ruta_flujo",
            "title": "Flujos de navegación más frecuentes",
        },
        "obtener_interaccion_promedio": {
            "key": "paginas_destacadas",
            "cols": ["pagina", "interaccion_promedio", "scroll_promedio_pct"],
            "index": "pagina",
            "title": "Páginas destacadas por interacción promedio",
        },
        "obtener_insight_frustracion": {
            "key": "resultados",
            "cols": ["device", "tasa_frustracion_pct"],
            "index": "device",
            "title": "Frustración detectada por dispositivo",
        },
        "obtener_insight_calidad_trafico": {
            "key": "resumen_por_canal",
            "cols": ["canal_origen", "engagement_promedio"],
            "index": "canal_origen",
            "title": "Calidad de tráfico por canal",
        },
        "obtener_insight_anatomia_abandono": {
            "key": "abandono_por_sistema_operativo",
            "cols": ["sistema_operativo", "tasa_abandono_pct"],
            "index": "sistema_operativo",
            "title": "Abandono rápido por sistema operativo",
        },
    }

    config = chart_configs.get(tool_name)
    if not config:
        return None

    records = result.get(config["key"], [])
    if not records:
        return None

    available_cols = [c for c in config["cols"] if c in records[0]]
    if not available_cols or config["index"] not in available_cols:
        return None

    df = pd.DataFrame(records)[available_cols].set_index(config["index"])
    return {"title": config["title"], "data": df}


def render_chart(tool_calls: list[dict[str, Any]]) -> None:
    if not tool_calls:
        return

    latest = tool_calls[-1]
    payload = build_chart_payload(latest)
    if payload is None:
        return

    st.markdown(
        f"<p class='matilda-chart-caption'>Generado desde {latest['tool_name']}</p>",
        unsafe_allow_html=True,
    )
    st.markdown(f"**{payload['title']}**")
    st.bar_chart(payload["data"], use_container_width=True)


def render_assistant_message(message: dict[str, Any]) -> None:
    render_chart(message.get("tool_calls", []))

    parsed = parse_copilot_output(message["content"])
    dato_html = _escape_with_breaks(parsed["dato"])
    interpretacion_html = _escape_with_breaks(parsed["interpretacion"])

    st.markdown(
        f"""
        <div class="matilda-response-card">
            <div class="matilda-response-section">
                <div class="matilda-response-label">📊 El Dato</div>
                <div class="matilda-response-body">{dato_html}</div>
            </div>
            <div class="matilda-response-section">
                <div class="matilda-response-label">💡 Interpretación</div>
                <div class="matilda-response-body">{interpretacion_html or "Sin interpretación adicional."}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def build_error_message(exc: Exception) -> dict[str, Any]:
    return {
        "role": "assistant",
        "content": (
            "📊 El Dato: No fue posible completar el análisis solicitado.\n\n"
            f"💡 Interpretación: Se detectó un error técnico: `{exc}`. "
            "Valida la API key, el modelo y la disponibilidad de los datos procesados."
        ),
        "tool_calls": [],
    }


def process_prompt(prompt: str) -> None:
    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Matilda está analizando los datos..."):
            try:
                response = consultar_matilda(prompt, devolver_contexto=True)
                assistant_message = {
                    "role": "assistant",
                    "content": response["answer_markdown"],
                    "tool_calls": response.get("tool_calls", []),
                    "model_name": response.get("model_name"),
                }
            except Exception as exc:
                assistant_message = build_error_message(exc)

            render_assistant_message(assistant_message)
            st.session_state.messages.append(assistant_message)


def render_chat_history() -> None:
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            if message["role"] == "assistant":
                render_assistant_message(message)
            else:
                st.markdown(message["content"])


def consume_pending_prompt(chat_prompt: str | None) -> str | None:
    pending = st.session_state.pending_prompt
    if pending:
        st.session_state.pending_prompt = None
        return pending
    return chat_prompt


def main() -> None:
    inject_styles()
    initialize_state()
    render_sidebar()
    render_header()

    col_chat, col_info = st.columns([5, 2], gap="medium")

    with col_chat:
        render_chat_history()
        prompt = st.chat_input("Pregunta algo como: ¿qué canales traen el tráfico de mayor calidad?")
        final_prompt = consume_pending_prompt(prompt)
        if final_prompt:
            process_prompt(final_prompt)

    with col_info:
        st.markdown(
            """
            <div class="matilda-info-card">
                <div class="matilda-info-card-label">Cómo usar Matilda</div>
                <p>
                    <strong>1.</strong> Elige un rol en la barra lateral.<br>
                    <strong>2.</strong> Usa una pregunta rápida o escribe la tuya.<br>
                    <strong>3.</strong> Revisa la gráfica, el dato y la recomendación.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown(
            """
            <div class="matilda-info-card">
                <div class="matilda-info-card-label">Formato de salida</div>
                <p>
                    <strong>📊 El Dato</strong> — Resumen cuantitativo.<br>
                    <strong>💡 Interpretación</strong> — Lectura de negocio con acción sugerida.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )


if __name__ == "__main__":
    main()
