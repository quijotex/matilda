from __future__ import annotations

import inspect
import os
from copy import deepcopy
from functools import wraps
from pathlib import Path
from typing import Any

from google import genai
from google.genai import types
from dotenv import load_dotenv

from src.data_engine import ANALYTIC_TOOLS


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

FALLBACK_MODEL_NAMES = ("gemini-2.5-flash", "gemini-2.5-pro")
DEFAULT_MODEL_NAME = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
ToolCallRecord = dict[str, Any]
_TOOL_CALL_LOG: list[ToolCallRecord] = []
SYSTEM_PROMPT = """
Eres Matilda, un motor analitico conversacional para datos de navegacion web.

Tu trabajo es responder preguntas de negocio usando SIEMPRE las herramientas
analiticas disponibles cuando la pregunta requiera datos, comparaciones,
rankings, patrones o explicaciones cuantitativas.

Reglas:
- Usa las tools antes de concluir cuando la consulta dependa de evidencia.
- Todas tus respuestas deben estar en Markdown estricto y usar exactamente esta estructura:
  📊 El Dato: una explicacion clara, directa y basada en los numeros devueltos por las tools.
  💡 Interpretación: un unico parrafo de consultoria de negocio que explique que significa el dato y proponga una accion concreta.
  🛠️ Recomendaciones: una lista numerada de 2 a 4 acciones concretas que el equipo puede tomar para mejorar, basadas en los datos. Cada accion debe mencionar la pagina, dispositivo, canal o flujo especifico al que aplica y que cambio implementar (ejemplo: revisar el tiempo de carga de /pricing en movil, simplificar el formulario de /register, A/B test en la landing X).
- No cambies esos titulos, no agregues secciones extra y no omitas ninguna de las tres.
- Sintetiza hallazgos de forma ejecutiva y accionable.
- No inventes metricas ni columnas que no existan.
- Si faltan datos o una pregunta no puede responderse con las tools, dilo con claridad.
""".strip()


def configurar_gemini() -> str:
    """Validate and return the Gemini API key from environment variables.

    Raises:
        ValueError: If no API key is available in the environment.
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("Define GEMINI_API_KEY en tu entorno o en el archivo .env.")

    return api_key


def _candidate_models(model_name: str | None = None) -> list[str]:
    """Build a deduplicated model preference list."""
    preferred = model_name or os.getenv("GEMINI_MODEL") or DEFAULT_MODEL_NAME
    ordered_candidates = [preferred, *FALLBACK_MODEL_NAMES]

    candidates: list[str] = []
    for candidate in ordered_candidates:
        if candidate and candidate not in candidates:
            candidates.append(candidate)
    return candidates


def _reset_tool_call_log() -> None:
    _TOOL_CALL_LOG.clear()


def _get_tool_call_log() -> list[ToolCallRecord]:
    return deepcopy(_TOOL_CALL_LOG)


def _build_tool_arguments(func: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
    signature = inspect.signature(func)
    bound_args = signature.bind_partial(*args, **kwargs)
    bound_args.apply_defaults()
    return dict(bound_args.arguments)


def _track_tool_call(func: Any) -> Any:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        arguments = _build_tool_arguments(func, *args, **kwargs)
        result = func(*args, **kwargs)
        _TOOL_CALL_LOG.append(
            {
                "tool_name": func.__name__,
                "arguments": arguments,
                "result": result,
            }
        )
        return result

    wrapper.__signature__ = inspect.signature(func)
    return wrapper


TRACKED_ANALYTIC_TOOLS = [_track_tool_call(tool) for tool in ANALYTIC_TOOLS]


def crear_cliente() -> genai.Client:
    """Create a Google GenAI client configured with the project API key."""
    return genai.Client(api_key=configurar_gemini())


def crear_configuracion_modelo(model_name: str | None = None) -> tuple[str, types.GenerateContentConfig]:
    """Create the model name and config object for Google GenAI.

    Args:
        model_name: Preferred Gemini model identifier, for example
            `gemini-1.5-flash` or `gemini-2.5-pro`. If omitted, the function
            will use `GEMINI_MODEL` from the environment.

    Returns:
        A tuple containing the selected model name and a GenerateContentConfig
        ready for automatic function calling with the registered tools.
    """
    selected_model = model_name or os.getenv("GEMINI_MODEL") or DEFAULT_MODEL_NAME

    config = types.GenerateContentConfig(
        systemInstruction=SYSTEM_PROMPT,
        tools=TRACKED_ANALYTIC_TOOLS,
        temperature=0.2,
        topP=0.9,
        automaticFunctionCalling=types.AutomaticFunctionCallingConfig(disable=False),
    )
    return selected_model, config


def consultar_matilda(
    prompt_usuario: str,
    model_name: str | None = None,
    devolver_contexto: bool = False,
) -> str | dict[str, Any]:
    """Send a business question to Gemini with analytical tools enabled.

    Args:
        prompt_usuario: Natural-language business question asked by the user.
        model_name: Preferred Gemini model identifier. If omitted, the value
            from `GEMINI_MODEL` will be used first and then the configured
            Gemini fallback models will be tried automatically.
        devolver_contexto: When `True`, returns the final answer plus metadata
            about the tools Gemini invoked so the UI can render charts or
            additional context.

    Returns:
        A natural-language answer generated by Gemini after invoking the
        registered Python tools when needed. If `devolver_contexto=True`,
        returns a dictionary with the final markdown answer, the selected model
        and the list of tool calls executed.

    Raises:
        ValueError: If the prompt is empty.
        RuntimeError: If Gemini fails with every configured model or never
            returns a final text response.
    """
    if not prompt_usuario or not prompt_usuario.strip():
        raise ValueError("`prompt_usuario` no puede estar vacio.")

    errors: list[str] = []
    for candidate_model in _candidate_models(model_name):
        try:
            _reset_tool_call_log()
            client = crear_cliente()
            selected_model, config = crear_configuracion_modelo(model_name=candidate_model)
            response = client.models.generate_content(
                model=selected_model,
                contents=prompt_usuario.strip(),
                config=config,
            )

            text = getattr(response, "text", "") or ""
            if text.strip():
                if devolver_contexto:
                    return {
                        "answer_markdown": text.strip(),
                        "model_name": selected_model,
                        "tool_calls": _get_tool_call_log(),
                    }
                return text.strip()

            errors.append(f"{selected_model}: respuesta vacia")
        except Exception as exc:
            errors.append(f"{candidate_model}: {exc}")

    raise RuntimeError(
        "Gemini no pudo responder con ninguno de los modelos configurados. "
        f"Intentos: {' | '.join(errors)}"
    )


def obtener_configuracion_modelo(model_name: str | None = None) -> dict[str, Any]:
    """Expose a lightweight summary of the configured agent.

    Args:
        model_name: Preferred Gemini model identifier to inspect.

    Returns:
        A simple dictionary describing the selected model and registered tools.
    """
    selected_model = model_name or os.getenv("GEMINI_MODEL") or DEFAULT_MODEL_NAME
    return {
        "model_name": selected_model,
        "fallback_models": [candidate for candidate in _candidate_models(selected_model) if candidate != selected_model],
        "tools_registradas": [tool.__name__ for tool in TRACKED_ANALYTIC_TOOLS],
        "tool_count": len(TRACKED_ANALYTIC_TOOLS),
    }
