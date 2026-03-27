from __future__ import annotations

import html
import re
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st

from src.agent import consultar_matilda

# Paleta corporativa para torta y barras (evita el look genérico “arcoíris”)
MATILDA_CHART_PALETTE: list[str] = [
    "#0B192C",
    "#D2143A",
    "#1e5f7a",
    "#b8860b",
    "#4a6670",
    "#922035",
    "#2a7a8f",
    "#8b6914",
    "#5c7a8a",
    "#6b3d4a",
    "#3d6b5c",
    "#7a4a6b",
    "#4a5a8a",
]


def _palette_for_n(n: int) -> list[str]:
    if n <= 0:
        return MATILDA_CHART_PALETTE
    out: list[str] = []
    for i in range(n):
        out.append(MATILDA_CHART_PALETTE[i % len(MATILDA_CHART_PALETTE)])
    return out


def _dataframe_for_pie(plot_df: pd.DataFrame, max_slices: int = 10) -> pd.DataFrame:
    """Top categorías + 'Otros' si hay muchas filas (torta legible)."""
    d = plot_df.sort_values("valor", ascending=False).reset_index(drop=True)
    if len(d) <= max_slices:
        return d
    top = d.head(max_slices - 1)
    rest = float(d.iloc[max_slices - 1 :]["valor"].sum())
    others = pd.DataFrame([{"categoria": "Otros", "valor": rest}])
    return pd.concat([top, others], ignore_index=True)


def _add_percent_column(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    total = float(out["valor"].sum())
    out["pct_total"] = (out["valor"] / total * 100.0) if total else 0.0
    return out


st.set_page_config(
    page_title="Matilda - Data Hub",
    page_icon="🎀",
    layout="wide",
    initial_sidebar_state="expanded",
)

QUICK_QUESTIONS_PER_PAGE = 4

ROLE_QUICK_QUESTIONS: dict[str, list[str]] = {
    "Marketing": [
        "¿Qué canales traen el tráfico de mayor calidad y qué acción recomendarías?",
        "¿Cuáles son las páginas top por vistas e interacciones y qué campañas deberíamos priorizar?",
        "¿Qué patrones observas en las sesiones que llegan a pricing?",
        "¿Qué sitios referentes aportan tráfico con mejor engagement y menos abandono?",
        "¿Cómo se diferencia el abandono rápido entre tráfico externo e interno?",
        "¿Qué páginas tienen muchas vistas pero poca interacción real?",
        "¿Qué combinación de canal y referente conviene priorizar en inversión?",
        "Resume en una lectura ejecutiva: calidad de tráfico y riesgos para la marca.",
    ],
    "Ventas": [
        "¿Qué páginas llevan más usuarios hacia pricing y cómo aprovecharlo comercialmente?",
        "¿Qué flujos de navegación aparecen con más frecuencia antes de una intención de conversión?",
        "¿Qué segmentos muestran mejor engagement y podrían tener mayor intención de compra?",
        "¿Qué dispositivos convierten mejor hacia pricing y cómo adaptar el pitch?",
        "¿Qué rutas repetidas indican intención seria frente a solo curiosidad?",
        "¿Qué páginas de entrada deberían nutrirse mejor desde comercial o partners?",
        "¿Dónde perdemos oportunidades entre buen engagement y poca llegada a pricing?",
        "Dame el resumen que llevaría a una reunión de forecast comercial.",
    ],
    "Management": [
        "Dame un resumen ejecutivo del abandono rápido y sus principales riesgos de negocio.",
        "¿Dónde vemos más frustración del usuario y qué impacto estratégico podría tener?",
        "¿Qué hallazgos accionables explicarían mejor la salud del funnel digital hoy?",
        "¿En qué sistemas operativos o contextos se concentra el abandono y qué implica?",
        "¿Qué canales aportan volumen vs calidad para los objetivos del board?",
        "¿Cuáles son las tres prioridades de riesgo digital que deberían estar en la agenda?",
        "¿Cómo está la relación entre frustración, dispositivo y sesiones con rage click?",
        "Un párrafo de síntesis: salud del embudo y decisión que recomendarías tomar ya.",
    ],
    "Product": [
        "¿Dónde hay más frustración o rage clicks por dispositivo y qué hipótesis de UX probarías?",
        "¿Qué flujos de navegación son los más frecuentes y con qué abandono o llegada a pricing?",
        "¿Qué páginas tienen peor tasa de abandono rápido con volumen suficiente para priorizar fixes?",
        "¿Cómo se reparte el abandono rápido por sistema operativo y tiempo en página?",
        "¿Qué páginas combinan buen volumen con interacción baja o scroll pobre?",
        "¿Qué rutas hacia pricing están infrautilizadas o se cortan demasiado pronto?",
        "¿Qué señales de calidad de tráfico deberíamos mirar antes de rediseñar un flujo?",
        "Prioriza backlog: tres mejoras de producto con datos detrás y por qué.",
    ],
}


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700;1,9..40,400&family=Fraunces:ital,opsz,wght@0,9..144,500;0,9..144,600;0,9..144,700;1,9..144,500&display=swap');

        :root {
            --m-navy: #0B192C;
            --m-surface: #F4F6F9;
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
            font-family: "DM Sans", system-ui, sans-serif !important;
        }

        .stApp {
            background: var(--m-surface)
                radial-gradient(ellipse 120% 80% at 100% -20%, rgba(210, 20, 58, 0.06), transparent 50%),
                radial-gradient(ellipse 80% 50% at -10% 50%, rgba(11, 25, 44, 0.04), transparent 45%) !important;
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

        section[data-testid="stSidebar"] .stButton > button,
        section[data-testid="stSidebar"] [data-testid="baseButton-secondary"] {
            background-color: rgba(255,255,255,0.08) !important;
            background-image: none !important;
            border: 1px solid rgba(255,255,255,0.15) !important;
            color: #E2E8F0 !important;
            -webkit-text-fill-color: #E2E8F0 !important;
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

        section[data-testid="stSidebar"] .stButton > button:hover,
        section[data-testid="stSidebar"] [data-testid="baseButton-secondary"]:hover {
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

        section[data-testid="stSidebar"] [data-testid="stHorizontalBlock"] {
            background: rgba(0, 0, 0, 0.22) !important;
            border-radius: 12px !important;
            padding: 6px 8px !important;
            margin-bottom: 0.65rem !important;
            border: 1px solid rgba(255,255,255, 0.08) !important;
            align-items: center !important;
        }

        [data-testid="stChatInput"] {
            border: 2px solid var(--m-border) !important;
            border-radius: 14px !important;
            background: #FFFFFF !important;
            box-shadow: 0 2px 14px rgba(11, 25, 44, 0.06) !important;
        }

        [data-testid="stChatInput"]:focus-within {
            border-color: var(--m-accent) !important;
            box-shadow: 0 0 0 3px rgba(210, 20, 58, 0.12) !important;
        }

        section[data-testid="stMain"] > div {
            max-width: 780px;
            margin-left: auto;
            margin-right: auto;
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
            position: relative;
            border: 1px solid var(--m-border);
            background: #FFFFFF;
            border-radius: 16px;
            padding: 1.5rem 1.75rem 1.25rem 1.75rem;
            margin-bottom: 1.25rem;
            box-shadow: 0 4px 20px rgba(11, 25, 44, 0.06);
            overflow: hidden;
        }

        .matilda-hero::before {
            content: "";
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 6px;
            border-radius: 15px 15px 0 0;
            background: linear-gradient(90deg, #0B192C 0%, #D2143A 45%, #2a6a7a 100%);
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
            font-family: "Fraunces", Georgia, serif !important;
            color: var(--m-navy);
            font-size: 1.85rem;
            font-weight: 700;
            margin: 0 0 0.4rem 0;
            letter-spacing: -0.02em;
        }

        .matilda-hero-sub {
            color: var(--m-muted);
            font-size: 0.9rem;
            line-height: 1.65;
            margin: 0;
        }

        .matilda-insight-hero {
            background: linear-gradient(145deg, #0B192C 0%, #152a45 55%, #0B192C 100%);
            color: #F8FAFC;
            border-radius: 14px;
            padding: 1.15rem 1.35rem 1.2rem 1.35rem;
            margin: 0.35rem 0 1rem 0;
            border: 1px solid rgba(255,255,255,0.08);
            box-shadow: 0 12px 40px rgba(11, 25, 44, 0.35);
            position: relative;
            overflow: visible !important;
            max-height: none !important;
        }

        .matilda-insight-hero::before {
            content: "";
            position: absolute;
            top: 0;
            right: 0;
            width: 55%;
            height: 100%;
            border-radius: 0 14px 14px 0;
            background: radial-gradient(circle at 80% 40%, rgba(210, 20, 58, 0.30) 0%, transparent 65%);
            pointer-events: none;
            z-index: 0;
        }

        .matilda-insight-headline {
            font-family: "Fraunces", Georgia, serif !important;
            font-size: 1.22rem;
            font-weight: 600;
            line-height: 1.35;
            margin: 0 0 0.4rem 0;
            position: relative;
            z-index: 1;
            color: #F8FAFC;
        }

        .matilda-insight-metric {
            font-size: 0.82rem;
            color: rgba(248, 250, 252, 0.75);
            position: relative;
            z-index: 1;
        }

        .matilda-response-card {
            background: #FFFFFF;
            border: 1px solid var(--m-border);
            border-left: 4px solid var(--m-accent);
            border-radius: 14px;
            padding: 1.1rem 1.15rem 0.9rem 1.15rem;
            margin-top: 0.5rem;
            overflow: visible !important;
            max-height: none !important;
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

        .matilda-response-body ol {
            padding-left: 1.1rem;
            margin: 0.35rem 0 0 0;
        }
        .matilda-response-body ol li {
            margin-bottom: 0.3rem;
        }

        [data-testid="stChatMessage"] {
            overflow: visible !important;
            max-height: none !important;
        }

        .matilda-export-bar {
            display: flex;
            gap: 6px;
            margin-top: 0.6rem;
            justify-content: flex-end;
        }
        .matilda-export-bar button {
            background: rgba(11,25,44,0.05);
            border: 1px solid var(--m-border);
            border-radius: 8px;
            padding: 4px 10px;
            font-size: 0.76rem;
            color: var(--m-muted);
            cursor: pointer;
            font-family: "DM Sans", system-ui, sans-serif;
            transition: all 0.15s ease;
        }
        .matilda-export-bar button:hover {
            background: rgba(210,20,58,0.08);
            color: var(--m-accent);
            border-color: var(--m-accent);
        }

        .matilda-empty-stage {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            min-height: 55vh;
            text-align: center;
            position: relative;
            overflow: hidden;
            border-radius: 20px;
            background:
                radial-gradient(ellipse 60% 55% at 50% 45%, rgba(210, 20, 58, 0.045), transparent 70%),
                radial-gradient(ellipse 80% 60% at 25% 60%, rgba(11, 25, 44, 0.04), transparent 60%),
                radial-gradient(ellipse 70% 50% at 80% 35%, rgba(42, 106, 122, 0.035), transparent 55%);
        }

        .matilda-empty-watermark {
            font-family: "Fraunces", Georgia, serif;
            font-size: 8rem;
            font-weight: 700;
            letter-spacing: -0.04em;
            background: linear-gradient(160deg, rgba(11,25,44,0.08) 0%, rgba(210,20,58,0.10) 50%, rgba(42,106,122,0.07) 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            user-select: none;
            line-height: 1;
            margin-bottom: 1rem;
        }

        .matilda-empty-hint {
            color: var(--m-muted);
            font-size: 0.92rem;
            opacity: 0.55;
            max-width: 340px;
            line-height: 1.6;
        }

        .matilda-empty-bar {
            width: 48px;
            height: 3px;
            border-radius: 3px;
            background: linear-gradient(90deg, var(--m-navy), var(--m-accent));
            opacity: 0.18;
            margin-bottom: 1.4rem;
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
    if st.session_state.selected_role not in ROLE_QUICK_QUESTIONS:
        st.session_state.selected_role = "Management"
    if "pending_prompt" not in st.session_state:
        st.session_state.pending_prompt = None
    if "qq_page" not in st.session_state:
        st.session_state.qq_page = 0


def extract_insight_headline(dato: str, max_len: int = 160) -> str:
    """First punchy line for the insight hero (editorial headline)."""
    plain = re.sub(r"\*\*([^*]+)\*\*", r"\1", dato)
    plain = re.sub(r"^#+\s*", "", plain, flags=re.MULTILINE)
    plain = re.sub(r"\s+", " ", plain).strip()
    if not plain:
        return "Insight listo para revisión."
    parts = re.split(r"(?<=[.!?])\s+", plain, maxsplit=1)
    first = parts[0]
    if len(first) > max_len:
        cut = first[: max_len - 1].rsplit(" ", 1)[0]
        return cut + "…"
    return first


def extract_metric_teaser(tool_call: dict[str, Any], payload: dict[str, Any] | None) -> str:
    """Short subtitle under headline: primary metric name from chart."""
    if not payload or payload["data"].empty:
        return "Métrica principal"
    col = payload["data"].columns[0]
    labels = {"page_views": "Vistas", "total_interactions": "Interacciones", "tasa_abandono_pct": "Abandono %"}
    return labels.get(col, col.replace("_", " "))


def render_header() -> None:
    st.markdown(
        """
        <div class="matilda-hero">
            <div class="matilda-hero-badge">Matilda / Data Hub Corporativo</div>
            <div class="matilda-hero-title">Matilda — Data Hub</div>
            <p class="matilda-hero-sub">
                Pregunta como en una reunión: números reales, gráfico y una lectura para decidir
                —sin exportar CSV ni cruzar hojas.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_quick_question_buttons(selected_role: str) -> None:
    """Preguntas por página con flechas DEBAJO y rerun para evitar que se pegue."""
    questions = ROLE_QUICK_QUESTIONS[selected_role]
    n = len(questions)
    total_pages = max(1, (n + QUICK_QUESTIONS_PER_PAGE - 1) // QUICK_QUESTIONS_PER_PAGE)

    if st.session_state.get("_qq_role_cache") != selected_role:
        st.session_state.qq_page = 0
    st.session_state._qq_role_cache = selected_role

    page = int(st.session_state.get("qq_page", 0))
    page = max(0, min(page, total_pages - 1))
    st.session_state.qq_page = page

    start = page * QUICK_QUESTIONS_PER_PAGE
    chunk = questions[start : start + QUICK_QUESTIONS_PER_PAGE]

    for rel_idx, question in enumerate(chunk):
        idx = start + rel_idx
        if st.button(question, key=f"quick_{selected_role}_{idx}", use_container_width=True):
            st.session_state.pending_prompt = question
            st.rerun()

    if total_pages > 1:
        p1, p2, p3 = st.columns([1, 2.4, 1])
        with p1:
            if st.button("◀", key=f"qq_prev_{selected_role}", disabled=page <= 0, use_container_width=True):
                st.session_state.qq_page = page - 1
                st.rerun()
        with p2:
            st.markdown(
                f"<p style='text-align:center;margin:0;padding:6px 4px;font-weight:600;font-size:0.88rem;color:#cbd5e1'>{page + 1} / {total_pages}</p>",
                unsafe_allow_html=True,
            )
        with p3:
            if st.button("▶", key=f"qq_next_{selected_role}", disabled=page >= total_pages - 1, use_container_width=True):
                st.session_state.qq_page = page + 1
                st.rerun()


def render_empty_stage() -> None:
    st.markdown(
        """
        <div class="matilda-empty-stage">
            <div class="matilda-empty-watermark">Matilda</div>
            <div class="matilda-empty-bar"></div>
            <div class="matilda-empty-hint">
                Selecciona un rol y haz una pregunta para comenzar el análisis.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar() -> None:
    with st.sidebar:
        st.markdown("## 🎀 Matilda")

        selected_role = st.selectbox(
            "Rol",
            options=list(ROLE_QUICK_QUESTIONS.keys()),
            index=list(ROLE_QUICK_QUESTIONS.keys()).index(st.session_state.selected_role),
            key="sidebar_role_select",
        )
        st.session_state.selected_role = selected_role

        st.markdown("#### Preguntas sugeridas")
        render_quick_question_buttons(selected_role)


def _escape_with_breaks(value: str) -> str:
    return html.escape(value).replace("\n", "<br>")


def _format_recomendaciones(raw: str) -> str:
    """Convert numbered markdown list into HTML <ol>."""
    lines = [l.strip() for l in raw.strip().splitlines() if l.strip()]
    items: list[str] = []
    for line in lines:
        cleaned = re.sub(r"^\d+[\.\)\-]\s*", "", line)
        cleaned = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", cleaned)
        if cleaned:
            items.append(f"<li>{cleaned}</li>")
    if not items:
        return _escape_with_breaks(raw)
    return "<ol>" + "".join(items) + "</ol>"


def _render_download_buttons(message: dict[str, Any]) -> None:
    """Streamlit native download button for markdown export."""
    content = message.get("content", "")
    st.download_button(
        label="⬇ Descargar respuesta (.md)",
        data=content,
        file_name="matilda_respuesta.md",
        mime="text/markdown",
        key=f"dl_{id(message)}",
    )


def parse_copilot_output(content: str) -> dict[str, str]:
    pattern = re.compile(
        r"📊\s*(?:\*\*)?El Dato(?:\*\*)?:?\s*(?P<dato>.*?)"
        r"(?:💡\s*(?:\*\*)?Interpretaci[oó]n(?:\*\*)?:?\s*(?P<interpretacion>.*?))?"
        r"(?:🛠️\s*(?:\*\*)?Recomendaciones(?:\*\*)?:?\s*(?P<recomendaciones>.*?))?$",
        re.DOTALL,
    )
    match = pattern.search(content.strip())
    if not match:
        return {"dato": content.strip(), "interpretacion": "", "recomendaciones": ""}

    dato = (match.group("dato") or "").strip()
    interpretacion = (match.group("interpretacion") or "").strip()
    recomendaciones = (match.group("recomendaciones") or "").strip()
    return {"dato": dato, "interpretacion": interpretacion, "recomendaciones": recomendaciones}


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


def _altair_title(metric_label: str, chart_title: str) -> alt.TitleParams:
    return alt.TitleParams(
        text=chart_title,
        subtitle=f"Métrica · {metric_label}",
        fontSize=15,
        subtitleFontSize=11,
        subtitleColor="#64748B",
        color="#0B192C",
        anchor="start",
        offset=10,
    )


def _bar_chart_categorical(plot_df: pd.DataFrame, value_col: str, title: str, metric_label: str) -> str:
    """Barras horizontales con color por categoría (misma paleta que la torta)."""
    n = len(plot_df)
    colors = _palette_for_n(n)
    bar = (
        alt.Chart(plot_df)
        .mark_bar(cornerRadiusEnd=6)
        .encode(
            x=alt.X("valor:Q", title=None, axis=alt.Axis(format="~s", grid=True)),
            y=alt.Y("categoria:N", sort="-x", title=None),
            color=alt.Color(
                "categoria:N",
                scale=alt.Scale(domain=plot_df["categoria"].tolist(), range=colors),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("categoria", title="Item"),
                alt.Tooltip("valor:Q", title=value_col.replace("_", " "), format=".2f"),
                alt.Tooltip("pct_total:Q", title="% del total", format=".1f"),
            ],
        )
        .properties(
            height=min(26 * n, 420) + 72,
            padding={"top": 14},
            title=_altair_title(metric_label, title),
        )
        .configure_axis(labelFontSize=11, labelLimit=280)
        .configure_view(stroke=None)
    )
    st.altair_chart(bar, use_container_width=True)
    return "barras"


def _pie_chart_donut(plot_df: pd.DataFrame, value_col: str, title: str, metric_label: str) -> str:
    """Donut con colores por segmento y porcentaje en tooltip."""
    n = len(plot_df)
    colors = _palette_for_n(n)
    pie = (
        alt.Chart(plot_df)
        .mark_arc(
            innerRadius=70,
            outerRadius=128,
            stroke="#FFFFFF",
            strokeWidth=2,
        )
        .encode(
            theta=alt.Theta("valor:Q", stack=True),
            color=alt.Color(
                "categoria:N",
                scale=alt.Scale(domain=plot_df["categoria"].tolist(), range=colors),
                legend=alt.Legend(orient="right", title=None, labelLimit=200),
            ),
            tooltip=[
                alt.Tooltip("categoria:N", title="Categoría"),
                alt.Tooltip("valor:Q", title=value_col.replace("_", " "), format=".2f"),
                alt.Tooltip("pct_total:Q", title="% del total", format=".1f"),
            ],
        )
        .properties(
            width=380,
            height=380,
            padding={"top": 12},
            title=_altair_title(metric_label, title),
        )
        .configure_view(stroke=None)
        .configure_legend(labelFontSize=11, symbolStrokeWidth=0)
    )
    st.altair_chart(pie, use_container_width=True)
    return "torta"


def render_altair_insight_chart(payload: dict[str, Any]) -> None:
    """Torta (donut) + barras con paleta corporativa; pestañas para alternar."""
    df = payload["data"]
    title = payload["title"]
    if df is None or df.empty:
        return

    value_col = df.columns[0]
    plot_df = df[[value_col]].reset_index()
    cat_col = plot_df.columns[0]
    plot_df = plot_df.rename(columns={cat_col: "categoria", value_col: "valor"})
    plot_df["categoria"] = plot_df["categoria"].astype(str).map(lambda x: (x[:52] + "…") if len(x) > 52 else x)
    plot_df = plot_df.sort_values("valor", ascending=False).head(14)

    plot_df = _add_percent_column(plot_df)
    metric_label = value_col.replace("_", " ")

    pie_df = _dataframe_for_pie(plot_df[["categoria", "valor"]], max_slices=10)
    pie_df = _add_percent_column(pie_df)

    can_pie = (
        len(pie_df) >= 2
        and float(pie_df["valor"].sum()) > 0
        and (pie_df["valor"] >= 0).all()
    )

    _bar_chart_categorical(plot_df, value_col, title, metric_label)
    if can_pie:
        _pie_chart_donut(pie_df, value_col, title, metric_label)


def render_chart(tool_calls: list[dict[str, Any]]) -> None:
    if not tool_calls:
        return

    latest = tool_calls[-1]
    payload = build_chart_payload(latest)
    if payload is None:
        return

    render_altair_insight_chart(payload)


def _last_assistant_index(messages: list[dict[str, Any]]) -> int:
    for i in range(len(messages) - 1, -1, -1):
        if messages[i].get("role") == "assistant":
            return i
    return -1


def render_assistant_message(message: dict[str, Any], *, prominent_insight: bool = True) -> None:
    tool_calls = message.get("tool_calls", [])
    parsed = parse_copilot_output(message["content"])

    if prominent_insight and tool_calls:
        latest = tool_calls[-1]
        payload = build_chart_payload(latest)
        headline = extract_insight_headline(parsed["dato"])
        teaser = extract_metric_teaser(latest, payload)
        st.markdown(
            f"""
            <div class="matilda-insight-hero">
                <div class="matilda-insight-headline">{html.escape(headline)}</div>
                <div class="matilda-insight-metric">{html.escape(teaser)}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    render_chart(tool_calls)

    dato_html = _escape_with_breaks(parsed["dato"])
    interpretacion_html = _escape_with_breaks(parsed["interpretacion"])
    recomendaciones_raw = parsed.get("recomendaciones", "")
    reco_html = _format_recomendaciones(recomendaciones_raw) if recomendaciones_raw else ""

    reco_block = ""
    if reco_html:
        reco_block = f"""
            <div class="matilda-response-section">
                <div class="matilda-response-label">🛠️ Recomendaciones</div>
                <div class="matilda-response-body">{reco_html}</div>
            </div>"""

    card_id = f"card_{id(message)}"

    st.markdown(
        f"""
        <div class="matilda-response-card" id="{card_id}">
            <div class="matilda-response-section">
                <div class="matilda-response-label">📊 El Dato</div>
                <div class="matilda-response-body">{dato_html}</div>
            </div>
            <div class="matilda-response-section">
                <div class="matilda-response-label">💡 Interpretación</div>
                <div class="matilda-response-body">{interpretacion_html or "Sin interpretación adicional."}</div>
            </div>{reco_block}
        </div>
        <div class="matilda-export-bar">
            <button onclick="var c=document.getElementById('{card_id}');navigator.clipboard.writeText(c.innerText).then(()=>this.textContent='✓ Copiado').catch(()=>{{}})">📋 Copiar</button>
        </div>
        """,
        unsafe_allow_html=True,
    )

    _render_download_buttons(message)


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

            render_assistant_message(assistant_message, prominent_insight=True)
            st.session_state.messages.append(assistant_message)


def render_chat_history() -> None:
    msgs = st.session_state.messages
    last_ai = _last_assistant_index(msgs)
    for i, message in enumerate(msgs):
        with st.chat_message(message["role"]):
            if message["role"] == "assistant":
                render_assistant_message(message, prominent_insight=(i == last_ai))
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
    if not st.session_state.messages:
        render_empty_stage()
    else:
        render_chat_history()
    prompt = st.chat_input("Escribe tu pregunta…")
    final_prompt = consume_pending_prompt(prompt)
    if final_prompt:
        process_prompt(final_prompt)


if __name__ == "__main__":
    main()
