from __future__ import annotations

import html
import re
from typing import Any

import pandas as pd
import streamlit as st

from src.agent import consultar_matilda, obtener_configuracion_modelo


st.set_page_config(page_title="Matilda - Data Hub", page_icon="🎀", layout="wide")

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


def normalize_label(value: Any, max_len: int = 34) -> str:
    """Normaliza etiquetas para graficas/reportes (limpia ids y texto ruidoso)."""
    text = str(value or "").strip()
    if not text:
        return "(sin dato)"

    text = text.replace('"', "").replace("'", "")
    text = re.sub(r"/\d+(?=/|$)", "/:id", text)
    text = re.sub(r"\s+", " ", text).strip()

    if len(text) > max_len:
        return f"{text[: max_len - 1]}..."
    return text


def normalize_markdown_paths(content: str) -> str:
    """Normaliza paths dentro de respuestas para mejorar legibilidad ejecutiva."""
    return re.sub(r"/[a-zA-Z0-9_\-\/\.]+", lambda match: normalize_label(match.group(0), max_len=42), content)


def inject_styles(theme_mode: str, font_scale: str) -> None:
    is_dark = theme_mode == "dark"
    is_large_font = font_scale == "large"
    base_font_size = "18px" if is_large_font else "16px"
    hero_title_size = "2.25rem" if is_large_font else "2rem"
    subtitle_size = "1.05rem" if is_large_font else "0.95rem"
    response_text_size = "1.02rem" if is_large_font else "0.95rem"
    quick_button_size = "1.0rem" if is_large_font else "0.92rem"
    app_bg = "#0B1220" if is_dark else "#F4F7FB"
    app_text = "#E6EDF7" if is_dark else "#0F1D31"
    panel_bg = "rgba(15, 23, 42, 0.9)" if is_dark else "rgba(255, 255, 255, 0.96)"
    panel_border = "rgba(148, 163, 184, 0.35)" if is_dark else "#D1DBE8"
    assistant_bg = "#111C31" if is_dark else "#EEF3F9"
    user_bg = "rgba(15, 23, 42, 0.8)" if is_dark else "rgba(255, 255, 255, 0.96)"
    user_border = "rgba(148, 163, 184, 0.3)" if is_dark else "rgba(11, 25, 44, 0.12)"
    response_card_bg = "#0F1A2E" if is_dark else "#F4F8FD"
    response_text = "#D1D9E6" if is_dark else "#2A3A4F"
    response_label = "#E2E8F0" if is_dark else "#0F1D31"
    caption_color = "#94A3B8" if is_dark else "#5B6E86"
    hero_bg = "linear-gradient(135deg, rgba(15, 23, 42, 0.96), rgba(30, 41, 59, 0.96))" if is_dark else "linear-gradient(135deg, rgba(255,255,255,0.98), rgba(240,244,248,0.98))"
    hero_title = "#E6EDF7" if is_dark else "var(--matilda-navy)"
    hero_subtitle = "#B8C2D6" if is_dark else "#4B5563"
    sidebar_card_bg = "#C6D4E8" if is_dark else "#FFFFFF"
    sidebar_card_text = "#09182B"
    sidebar_card_border = "rgba(148, 163, 184, 0.38)" if is_dark else "rgba(11, 25, 44, 0.15)"
    sidebar_card_hover_bg = "#D7E2F1" if is_dark else "#F2F6FC"
    sidebar_card_hover_text = "#061324" if is_dark else "#0B192C"
    sidebar_select_bg = "#0F172A" if is_dark else "#0A1220"
    sidebar_select_border = "rgba(148, 163, 184, 0.4)" if is_dark else "rgba(255, 255, 255, 0.22)"
    sidebar_select_text = "#E2E8F0" if is_dark else "#F8FAFC"

    style_template = """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&display=swap');

        :root {
            --matilda-navy: #0B192C;
            --matilda-surface: #F8F9FA;
            --matilda-accent: #D2143A;
            --matilda-ink: #102033;
            --matilda-soft: #EAF0F6;
            --matilda-border: #D9E1EA;
        }

        html, body, [class*="st-"], [data-testid="stMarkdownContainer"], button, input, textarea, select {
            font-family: "JetBrains Mono", monospace !important;
            font-size: __BASE_FONT_SIZE__;
        }

        .stApp {
            background: __APP_BG__;
            color: __APP_TEXT__;
        }

        [data-testid="stSidebar"] {
            background: var(--matilda-navy);
            border-right: 1px solid rgba(255, 255, 255, 0.08);
        }

        [data-testid="stSidebar"] * {
            color: #F8FAFC;
        }

        [data-testid="stSidebar"] [data-baseweb="select"] > div {
            background: __SIDEBAR_SELECT_BG__ !important;
            border: 1px solid __SIDEBAR_SELECT_BORDER__ !important;
            color: __SIDEBAR_SELECT_TEXT__ !important;
        }

        [data-testid="stSidebar"] [data-baseweb="select"] span {
            color: __SIDEBAR_SELECT_TEXT__ !important;
        }

        .matilda-hero {
            border-top: 6px solid var(--matilda-accent);
            background: __HERO_BG__;
            border-radius: 18px;
            padding: 1.25rem 1.35rem;
            margin-bottom: 1rem;
            box-shadow: 0 18px 45px rgba(11, 25, 44, 0.08);
            border-left: 1px solid rgba(210, 20, 58, 0.12);
            border-right: 1px solid rgba(11, 25, 44, 0.06);
            border-bottom: 1px solid rgba(11, 25, 44, 0.06);
        }

        .matilda-hero-title {
            color: __HERO_TITLE__;
            font-size: __HERO_TITLE_SIZE__;
            font-weight: 700;
            margin-bottom: 0.35rem;
        }

        .matilda-hero-subtitle {
            color: __HERO_SUBTITLE__;
            font-size: __SUBTITLE_SIZE__;
            line-height: 1.55;
        }

        .matilda-badge {
            display: inline-block;
            background: rgba(210, 20, 58, 0.08);
            color: var(--matilda-accent);
            border: 1px solid rgba(210, 20, 58, 0.22);
            padding: 0.3rem 0.55rem;
            border-radius: 999px;
            font-size: 0.78rem;
            margin-bottom: 0.8rem;
        }

        .matilda-panel {
            background: __PANEL_BG__;
            border: 1px solid __PANEL_BORDER__;
            border-radius: 16px;
            padding: 1rem;
            margin-bottom: 1rem;
            box-shadow: 0 8px 24px rgba(11, 25, 44, 0.04);
        }

        .stButton > button,
        [data-testid="baseButton-secondary"],
        [data-testid="baseButton-primary"] {
            width: 100%;
            border-radius: 12px !important;
            border: 1px solid rgba(11, 25, 44, 0.18) !important;
            background: #FFFFFF !important;
            color: var(--matilda-navy) !important;
            transition: all 0.2s ease !important;
        }

        [data-testid="stSidebar"] .stButton > button,
        [data-testid="stSidebar"] [data-testid="baseButton-secondary"],
        [data-testid="stSidebar"] [data-testid="baseButton-primary"] {
            background: __SIDEBAR_CARD_BG__ !important;
            color: __SIDEBAR_CARD_TEXT__ !important;
            border: 1px solid __SIDEBAR_CARD_BORDER__ !important;
            text-align: left !important;
            white-space: normal !important;
            line-height: 1.35 !important;
            min-height: 86px !important;
            padding: 0.7rem 0.8rem !important;
            overflow-wrap: break-word !important;
            font-weight: 600 !important;
            font-size: __QUICK_BUTTON_SIZE__ !important;
            opacity: 1 !important;
        }

        [data-testid="stSidebar"] .stButton > button p {
            color: __SIDEBAR_CARD_TEXT__ !important;
            opacity: 1 !important;
        }

        [data-testid="stSidebar"] .stButton > button:hover {
            background: __SIDEBAR_CARD_HOVER_BG__ !important;
            color: __SIDEBAR_CARD_HOVER_TEXT__ !important;
        }

        [data-testid="stSidebar"] .stButton > button:hover p {
            color: __SIDEBAR_CARD_HOVER_TEXT__ !important;
        }

        .stButton > button:hover,
        [data-testid="baseButton-secondary"]:hover,
        [data-testid="baseButton-primary"]:hover {
            border-color: var(--matilda-accent) !important;
            color: var(--matilda-accent) !important;
            box-shadow: 0 8px 20px rgba(210, 20, 58, 0.12) !important;
            transform: translateY(-1px);
        }

        [data-testid="stChatMessage"] {
            border-radius: 18px;
        }

        [data-testid="stChatMessage"]:has(.matilda-assistant-shell) {
            background: __ASSISTANT_BG__;
            border-left: 4px solid var(--matilda-accent);
            padding: 0.35rem 0.5rem 0.5rem 0.5rem;
        }

        [data-testid="stChatMessage"]:has(.matilda-user-shell) {
            background: __USER_BG__;
            border: 1px solid __USER_BORDER__;
        }

        .matilda-assistant-shell, .matilda-user-shell {
            width: 100%;
        }

        .matilda-response-card {
            background: __RESPONSE_CARD_BG__;
            border: 1px solid rgba(11, 25, 44, 0.08);
            border-left: 4px solid var(--matilda-accent);
            border-radius: 14px;
            padding: 1rem 1rem 0.8rem 1rem;
            margin-top: 0.6rem;
        }

        .matilda-response-section {
            margin-bottom: 0.9rem;
        }

        .matilda-response-label {
            color: __RESPONSE_LABEL__;
            font-weight: 700;
            margin-bottom: 0.25rem;
        }

        .matilda-response-text {
            color: __RESPONSE_TEXT__;
            line-height: 1.7;
            font-size: __RESPONSE_TEXT_SIZE__;
            white-space: normal;
        }

        .matilda-caption {
            color: __CAPTION_COLOR__;
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            margin-bottom: 0.4rem;
        }

        [data-testid="stSelectbox"] label,
        [data-testid="stChatInput"] label {
            color: inherit !important;
        }

        .quick-questions-title {
            margin-top: 0.2rem;
            margin-bottom: 0.35rem;
            font-weight: 700;
        }

        @media (max-width: 1024px) {
            .matilda-hero {
                padding: 1rem 1rem;
            }
            .matilda-hero-title {
                font-size: 1.6rem;
            }
            .matilda-panel {
                padding: 0.85rem;
            }
        }

        @media (max-width: 768px) {
            .matilda-hero {
                border-radius: 14px;
                margin-bottom: 0.7rem;
            }
            .matilda-hero-title {
                font-size: 1.5rem;
            }
            .matilda-hero-subtitle {
                font-size: 0.92rem;
                line-height: 1.45;
            }
            [data-testid="stSidebar"] .stButton > button {
                min-height: 70px !important;
                font-size: 0.95rem !important;
                padding: 0.55rem 0.65rem !important;
            }
            .matilda-response-card {
                padding: 0.8rem 0.75rem 0.7rem 0.75rem;
                margin-top: 0.45rem;
            }
            .matilda-response-text {
                line-height: 1.55;
                font-size: 0.95rem;
            }
        }
        </style>
        """
    style = (
        style_template.replace("__APP_BG__", app_bg)
        .replace("__APP_TEXT__", app_text)
        .replace("__BASE_FONT_SIZE__", base_font_size)
        .replace("__HERO_BG__", hero_bg)
        .replace("__HERO_TITLE__", hero_title)
        .replace("__HERO_TITLE_SIZE__", hero_title_size)
        .replace("__HERO_SUBTITLE__", hero_subtitle)
        .replace("__SUBTITLE_SIZE__", subtitle_size)
        .replace("__PANEL_BG__", panel_bg)
        .replace("__PANEL_BORDER__", panel_border)
        .replace("__ASSISTANT_BG__", assistant_bg)
        .replace("__USER_BG__", user_bg)
        .replace("__USER_BORDER__", user_border)
        .replace("__RESPONSE_CARD_BG__", response_card_bg)
        .replace("__RESPONSE_TEXT__", response_text)
        .replace("__RESPONSE_TEXT_SIZE__", response_text_size)
        .replace("__RESPONSE_LABEL__", response_label)
        .replace("__CAPTION_COLOR__", caption_color)
        .replace("__SIDEBAR_CARD_BG__", sidebar_card_bg)
        .replace("__SIDEBAR_CARD_TEXT__", sidebar_card_text)
        .replace("__SIDEBAR_CARD_BORDER__", sidebar_card_border)
        .replace("__SIDEBAR_CARD_HOVER_BG__", sidebar_card_hover_bg)
        .replace("__SIDEBAR_CARD_HOVER_TEXT__", sidebar_card_hover_text)
        .replace("__SIDEBAR_SELECT_BG__", sidebar_select_bg)
        .replace("__SIDEBAR_SELECT_BORDER__", sidebar_select_border)
        .replace("__SIDEBAR_SELECT_TEXT__", sidebar_select_text)
        .replace("__QUICK_BUTTON_SIZE__", quick_button_size)
    )
    st.markdown(
        style,
        unsafe_allow_html=True,
    )


def initialize_state() -> None:
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "selected_role" not in st.session_state:
        st.session_state.selected_role = "Management"
    if "pending_prompt" not in st.session_state:
        st.session_state.pending_prompt = None
    if "theme_mode" not in st.session_state:
        st.session_state.theme_mode = "light"
    if "font_scale" not in st.session_state:
        st.session_state.font_scale = "normal"


def render_header() -> None:
    st.markdown(
        """
        <div class="matilda-hero">
            <div class="matilda-badge">Matilda / Data Hub Corporativo</div>
            <div class="matilda-hero-title">Matilda - Data Hub</div>
            <div class="matilda-hero-subtitle">
                Analiza comportamiento digital, identifica fricción y transforma datos de navegación en decisiones accionables
                con una experiencia tipo Copilot: visualización inmediata + lectura ejecutiva + recomendación de negocio.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar() -> None:
    config = obtener_configuracion_modelo()
    with st.sidebar:
        st.markdown("## 🎀 Matilda")
        st.caption("Asistente analítico para Marketing, Ventas y Management")

        dark_mode_enabled = st.toggle(
            "Modo oscuro",
            value=st.session_state.theme_mode == "dark",
        )
        st.session_state.theme_mode = "dark" if dark_mode_enabled else "light"
        font_scale_label = st.radio(
            "Tamaño de fuente",
            options=["Normal", "Grande"],
            index=1 if st.session_state.font_scale == "large" else 0,
            horizontal=True,
        )
        st.session_state.font_scale = "large" if font_scale_label == "Grande" else "normal"

        selected_role = st.selectbox(
            "Rol de análisis",
            options=list(ROLE_QUICK_QUESTIONS.keys()),
            index=list(ROLE_QUICK_QUESTIONS.keys()).index(st.session_state.selected_role),
        )
        st.session_state.selected_role = selected_role

        st.markdown("<div class='quick-questions-title'>Preguntas rápidas</div>", unsafe_allow_html=True)
        for idx, question in enumerate(ROLE_QUICK_QUESTIONS[selected_role]):
            if st.button(question, key=f"quick_{selected_role}_{idx}", use_container_width=True):
                st.session_state.pending_prompt = question

        st.markdown("---")
        st.markdown("### Configuración")
        st.write(f"Modelo principal: `{config['model_name']}`")
        st.write(f"Fallbacks: `{', '.join(config['fallback_models']) or 'sin fallback'}`")
        st.write(f"Herramientas activas: `{config['tool_count']}`")


def _escape_with_breaks(value: str) -> str:
    return html.escape(value).replace("\n", "<br>")


def parse_copilot_output(content: str) -> dict[str, str]:
    pattern = re.compile(
        r"📊\s*El Dato:\s*(?P<dato>.*?)(?:💡\s*Interpretación:\s*(?P<interpretacion>.*))?$",
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

    if tool_name == "obtener_paginas_top":
        records = result.get("resultados", [])
        if records:
            df = pd.DataFrame(records)[["pagina", "page_views", "total_interactions"]]
            df["pagina"] = df["pagina"].apply(normalize_label)
            df = df.set_index("pagina")
            return {"kind": "bar", "title": "Páginas top por vistas e interacciones", "data": df}

    if tool_name == "calcular_tasas_abandono":
        records = result.get("paginas_criticas", [])
        if records:
            df = pd.DataFrame(records)[["pagina", "tasa_abandono_pct"]]
            df["pagina"] = df["pagina"].apply(normalize_label)
            df = df.set_index("pagina")
            return {"kind": "bar", "title": "Páginas con mayor tasa de abandono rápido", "data": df}

    if tool_name == "analizar_patrones_conversion":
        records = result.get("paginas_entrada_top", [])
        if records:
            df = pd.DataFrame(records)[["pagina_entrada", "tasa_pricing_pct"]]
            df["pagina_entrada"] = df["pagina_entrada"].apply(normalize_label)
            df = df.set_index("pagina_entrada")
            return {"kind": "bar", "title": "Páginas de entrada con mayor llegada a pricing", "data": df}

    if tool_name == "obtener_flujos_frecuentes":
        records = result.get("resultados", [])
        if records:
            df = pd.DataFrame(records)[["ruta_flujo", "sesiones"]]
            df["ruta_flujo"] = df["ruta_flujo"].apply(normalize_label)
            df = df.set_index("ruta_flujo")
            return {"kind": "bar", "title": "Flujos de navegación más frecuentes", "data": df}

    if tool_name == "obtener_interaccion_promedio":
        records = result.get("paginas_destacadas", [])
        if records:
            df = pd.DataFrame(records)[["pagina", "interaccion_promedio", "scroll_promedio_pct"]]
            df["pagina"] = df["pagina"].apply(normalize_label)
            df = df.set_index("pagina")
            return {"kind": "bar", "title": "Páginas destacadas por interacción promedio", "data": df}

    if tool_name == "obtener_insight_frustracion":
        records = result.get("resultados", [])
        if records:
            df = pd.DataFrame(records)[["device", "tasa_frustracion_pct"]].set_index("device")
            return {"kind": "bar", "title": "Frustración detectada por dispositivo", "data": df}

    if tool_name == "obtener_insight_calidad_trafico":
        records = result.get("resumen_por_canal", [])
        if records:
            df = pd.DataFrame(records)[["canal_origen", "engagement_promedio"]].set_index("canal_origen")
            return {"kind": "bar", "title": "Calidad de tráfico por canal", "data": df}

    if tool_name == "obtener_insight_anatomia_abandono":
        records = result.get("abandono_por_sistema_operativo", [])
        if records:
            df = pd.DataFrame(records)[["sistema_operativo", "tasa_abandono_pct"]].set_index("sistema_operativo")
            return {"kind": "bar", "title": "Abandono rápido por sistema operativo", "data": df}

    return None


def render_chart(tool_calls: list[dict[str, Any]]) -> None:
    if not tool_calls:
        return

    latest_tool_call = tool_calls[-1]
    chart_payload = build_chart_payload(latest_tool_call)
    if chart_payload is None:
        return

    st.markdown(
        f"<div class='matilda-caption'>Visualización generada desde `{latest_tool_call['tool_name']}`</div>",
        unsafe_allow_html=True,
    )
    st.markdown(f"#### {chart_payload['title']}")

    if chart_payload["kind"] == "line":
        st.line_chart(chart_payload["data"], use_container_width=True)
    else:
        st.bar_chart(chart_payload["data"], use_container_width=True)


def render_assistant_message(message: dict[str, Any]) -> None:
    st.markdown("<div class='matilda-assistant-shell'>", unsafe_allow_html=True)
    render_chart(message.get("tool_calls", []))

    normalized_content = normalize_markdown_paths(message["content"])
    parsed = parse_copilot_output(normalized_content)
    dato_html = _escape_with_breaks(parsed["dato"])
    interpretacion_html = _escape_with_breaks(parsed["interpretacion"])

    st.markdown(
        f"""
        <div class="matilda-response-card">
            <div class="matilda-response-section">
                <div class="matilda-response-label">📊 El Dato</div>
                <div class="matilda-response-text">{dato_html}</div>
            </div>
            <div class="matilda-response-section">
                <div class="matilda-response-label">💡 Interpretación</div>
                <div class="matilda-response-text">{interpretacion_html or "Sin interpretación adicional."}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)


def render_user_message(content: str) -> None:
    st.markdown("<div class='matilda-user-shell'>", unsafe_allow_html=True)
    st.markdown(content)
    st.markdown("</div>", unsafe_allow_html=True)


def build_error_message(exc: Exception) -> dict[str, Any]:
    return {
        "role": "assistant",
        "content": (
            "📊 El Dato: No fue posible completar el análisis solicitado.\n\n"
            f"💡 Interpretación: Se detectó un error técnico al consultar Matilda: `{exc}`. "
            "Conviene validar la API key, el modelo configurado y la disponibilidad de los datos procesados antes de reintentar."
        ),
        "tool_calls": [],
    }


def process_prompt(prompt: str) -> None:
    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.chat_message("user", avatar="🙂"):
        render_user_message(prompt)

    with st.chat_message("assistant", avatar="🤖"):
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
        avatar = "🤖" if message["role"] == "assistant" else "🙂"
        with st.chat_message(message["role"], avatar=avatar):
            if message["role"] == "assistant":
                render_assistant_message(message)
            else:
                render_user_message(message["content"])


def consume_pending_prompt(chat_prompt: str | None) -> str | None:
    pending_prompt = st.session_state.pending_prompt
    if pending_prompt:
        st.session_state.pending_prompt = None
        return pending_prompt
    return chat_prompt


def main() -> None:
    initialize_state()
    inject_styles(st.session_state.theme_mode, st.session_state.font_scale)
    render_sidebar()
    inject_styles(st.session_state.theme_mode, st.session_state.font_scale)
    render_header()

    col_a, col_b = st.columns([2.3, 1], gap="large")
    with col_a:
        render_chat_history()
        prompt = st.chat_input("Pregunta algo como: ¿qué canales traen el tráfico de mayor calidad?")
        final_prompt = consume_pending_prompt(prompt)
        if final_prompt:
            process_prompt(final_prompt)
    with col_b:
        st.markdown(
            """
            <div class="matilda-panel">
                <div class="matilda-caption">Modo de uso</div>
                <strong>1.</strong> Elige un rol en la barra lateral.<br>
                <strong>2.</strong> Lanza una pregunta rápida o escribe una propia.<br>
                <strong>3.</strong> Revisa la gráfica, el dato y la interpretación estratégica.
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown(
            """
            <div class="matilda-panel">
                <div class="matilda-caption">Salida esperada</div>
                Cada respuesta de Matilda sigue un formato tipo Copilot:
                <br><br>
                <strong>📊 El Dato</strong><br>
                Resumen factual y cuantitativo.
                <br><br>
                <strong>💡 Interpretación</strong><br>
                Lectura de negocio con una acción sugerida.
            </div>
            """,
            unsafe_allow_html=True,
        )


if __name__ == "__main__":
    main()
