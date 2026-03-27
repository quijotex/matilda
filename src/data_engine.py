from __future__ import annotations

import json
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit, urlunsplit
from datetime import datetime

import polars as pl


JsonDict = dict[str, Any]

# Pipeline de calidad + exportacion parquet:
# 1) Estandariza y limpia datos de recordings/metrics.
# 2) Exporta parquet optimizado (zstd + estadisticas).
# 3) Calcula comparacion before/after, calidad y alertas.
# 4) Emite reporte JSON para monitoreo y trazabilidad.
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

RECORDINGS_PATH = RAW_DIR / "1_Data_Recordings.csv"
METRICS_PATH = RAW_DIR / "2_Data_Metrics.csv"

RECORDINGS_OUTPUT = PROCESSED_DIR / "recordings_clean.parquet"
METRICS_OUTPUT = PROCESSED_DIR / "metrics_clean.parquet"
PARQUET_REPORT_OUTPUT = PROCESSED_DIR / "parquet_quality_report.json"

PARQUET_COMPRESSION = os.getenv("PARQUET_COMPRESSION", "zstd")
PARQUET_COMPRESSION_LEVEL = int(os.getenv("PARQUET_COMPRESSION_LEVEL", "6"))
PARQUET_ROW_GROUP_SIZE = int(os.getenv("PARQUET_ROW_GROUP_SIZE", "50000"))
PARQUET_VERSIONED_EXPORT = os.getenv("PARQUET_VERSIONED_EXPORT", "1").strip() in {"1", "true", "True", "yes", "YES"}

MAX_NULL_RATIO_ALERT = float(os.getenv("MAX_NULL_RATIO_ALERT", "0.35"))
MAX_DUPLICATE_RATIO_ALERT = float(os.getenv("MAX_DUPLICATE_RATIO_ALERT", "0.2"))
MAX_ROW_DROP_RATIO_ALERT = float(os.getenv("MAX_ROW_DROP_RATIO_ALERT", "0.05"))

METRICS_DROP_NULL_URL = os.getenv("METRICS_DROP_NULL_URL", "1").strip() in {"1", "true", "True", "yes", "YES"}
METRICS_DEDUP_ENABLED = os.getenv("METRICS_DEDUP_ENABLED", "1").strip() in {"1", "true", "True", "yes", "YES"}
METRICS_DEDUP_KEYS_RAW = os.getenv("METRICS_DEDUP_KEYS", "url,device,os,metric_name")
METRICS_NUMERIC_FILL_NULL_WITH_ZERO = os.getenv("METRICS_NUMERIC_FILL_NULL_WITH_ZERO", "0").strip() in {
    "1",
    "true",
    "True",
    "yes",
    "YES",
}


def standardize_column_name(column_name: str) -> str:
    """Normalize a column name to resilient snake_case."""
    cleaned = column_name.replace("\ufeff", "").strip()
    cleaned = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", cleaned)
    cleaned = re.sub(r"[\s\-/]+", "_", cleaned)
    cleaned = re.sub(r"[^\w]+", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned)
    return cleaned.strip("_").lower()


def ensure_directories() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def normalize_url(value: Any) -> str | None:
    """Remove query params/fragments and normalize trailing slashes."""
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        parsed = urlsplit(text)
    except ValueError:
        return text

    path = parsed.path or ""
    if path not in ("", "/"):
        path = path.rstrip("/")

    normalized = urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), path, "", ""))
    return normalized.rstrip("/") if path == "" else normalized


def extract_url_path(value: Any) -> str | None:
    """Return only the URL path so the LLM can reason about navigation routes."""
    normalized = normalize_url(value)
    if not normalized:
        return None

    parsed = urlsplit(normalized)
    path = parsed.path or "/"
    return path if path else "/"


def extract_domain(value: Any) -> str | None:
    """Extract the domain from a URL-like value."""
    normalized = normalize_url(value)
    if not normalized:
        return None

    parsed = urlsplit(normalized)
    domain = parsed.netloc.lower().strip()
    return domain or None


def parse_bool(value: Any) -> bool | None:
    """Convert common truthy and falsy textual values into real booleans."""
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        if value == 1:
            return True
        if value == 0:
            return False

    text = str(value).strip().lower()
    if text == "":
        return None

    truthy = {"1", "1.0", "true", "t", "yes", "y", "si", "sí"}
    falsy = {"0", "0.0", "false", "f", "no", "n"}

    if text in truthy:
        return True
    if text in falsy:
        return False
    return None


def read_csv_with_standardized_columns(csv_path: Path) -> pl.DataFrame:
    df = pl.read_csv(
        csv_path,
        infer_schema_length=10_000,
        try_parse_dates=False,
        null_values=["", "null", "NULL", "None", "NaN", "nan"],
        ignore_errors=True,
        encoding="utf8-lossy",
    )
    rename_map = {column: standardize_column_name(column) for column in df.columns}
    return df.rename(rename_map)


def trim_string_columns(df: pl.DataFrame) -> pl.DataFrame:
    string_columns = [column for column, dtype in df.schema.items() if dtype == pl.String]
    if not string_columns:
        return df

    return df.with_columns(pl.col(string_columns).str.strip_chars())


def cast_numeric_columns(df: pl.DataFrame, numeric_columns: set[str]) -> pl.DataFrame:
    available_columns = [column for column in numeric_columns if column in df.columns]
    if not available_columns:
        return df

    return df.with_columns(
        [
            pl.col(column)
            .cast(pl.String)
            .str.replace_all(",", "")
            .cast(pl.Float64, strict=False)
            .alias(column)
            for column in available_columns
        ]
    )


def cast_boolean_columns(df: pl.DataFrame, boolean_columns: set[str]) -> pl.DataFrame:
    available_columns = [column for column in boolean_columns if column in df.columns]
    if not available_columns:
        return df

    return df.with_columns(
        [pl.col(column).map_elements(parse_bool, return_dtype=pl.Boolean).alias(column) for column in available_columns]
    )


def clean_url_columns(df: pl.DataFrame, url_columns: set[str]) -> pl.DataFrame:
    available_columns = [column for column in url_columns if column in df.columns]
    if not available_columns:
        return df

    return df.with_columns(
        [pl.col(column).map_elements(normalize_url, return_dtype=pl.String).alias(column) for column in available_columns]
    )


def clean_recordings(df: pl.DataFrame) -> pl.DataFrame:
    numeric_columns = {
        "recuento_paginas",
        "clics_sesion",
        "duracion_sesion_segundos",
        "clicks_por_pagina",
        "tiempo_por_pagina",
        "interaccion_total",
        "standarized_engagement_score",
    }
    boolean_columns = {
        "abandono_rapido",
        "posible_frustracion",
        "entrada_es_home",
        "trafico_externo",
    }
    url_columns = {"direccion_url_entrada", "direccion_url_salida", "referente"}

    df = trim_string_columns(df)
    df = cast_numeric_columns(df, numeric_columns)
    df = cast_boolean_columns(df, boolean_columns)
    df = clean_url_columns(df, url_columns)

    if "fecha" in df.columns:
        df = df.with_columns(pl.col("fecha").str.strptime(pl.Date, format="%m/%d/%Y", strict=False).alias("fecha"))

    if "hora" in df.columns:
        df = df.with_columns(pl.col("hora").str.strptime(pl.Time, format="%H:%M", strict=False).alias("hora"))

    if {"fecha", "hora"}.issubset(df.columns):
        df = df.with_columns(
            pl.concat_str(
                [
                    pl.col("fecha").dt.strftime("%Y-%m-%d"),
                    pl.lit(" "),
                    pl.col("hora").dt.strftime("%H:%M:%S"),
                ]
            )
            .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False)
            .alias("fecha_hora")
        )

    if "duracion_sesion" in df.columns:
        df = df.with_columns(
            pl.col("duracion_sesion")
            .str.extract(r"(?:(\d+):)?(\d+):(\d+)", 1)
            .cast(pl.Int64, strict=False)
            .fill_null(0)
            .mul(3600)
            .add(
                pl.col("duracion_sesion")
                .str.extract(r"(?:(\d+):)?(\d+):(\d+)", 2)
                .cast(pl.Int64, strict=False)
                .fill_null(0)
                .mul(60)
            )
            .add(
                pl.col("duracion_sesion")
                .str.extract(r"(?:(\d+):)?(\d+):(\d+)", 3)
                .cast(pl.Int64, strict=False)
                .fill_null(0)
            )
            .alias("duracion_sesion_total_segundos")
        )

    if "direccion_url_entrada" in df.columns:
        df = df.with_columns(
            pl.col("direccion_url_entrada")
            .map_elements(extract_url_path, return_dtype=pl.String)
            .alias("ruta_entrada")
        )

    if "direccion_url_salida" in df.columns:
        df = df.with_columns(
            pl.col("direccion_url_salida")
            .map_elements(extract_url_path, return_dtype=pl.String)
            .alias("ruta_salida")
        )

    if {"ruta_entrada", "ruta_salida"}.issubset(df.columns):
        df = df.with_columns(
            pl.concat_str(
                [
                    pl.coalesce([pl.col("ruta_entrada"), pl.lit("(sin entrada)")]),
                    pl.lit(" -> "),
                    pl.coalesce([pl.col("ruta_salida"), pl.lit("(sin salida)")]),
                ]
            ).alias("ruta_flujo")
        )

    pricing_expr = pl.lit(False)
    if "direccion_url_entrada" in df.columns:
        pricing_expr = pricing_expr | pl.col("direccion_url_entrada").fill_null("").str.to_lowercase().str.contains("pricing")
    if "direccion_url_salida" in df.columns:
        pricing_expr = pricing_expr | pl.col("direccion_url_salida").fill_null("").str.to_lowercase().str.contains("pricing")

    df = df.with_columns(pricing_expr.alias("llego_a_pricing"))

    return df.sort("fecha_hora", descending=False, nulls_last=True) if "fecha_hora" in df.columns else df


def clean_metrics(df: pl.DataFrame) -> pl.DataFrame:
    numeric_columns = {
        "sessions_count",
        "sessions_with_metric_percentage",
        "sessions_without_metric_percentage",
        "pages_views",
        "sub_total",
        "average_scroll_depth",
        "total_session_count",
        "total_bot_session_count",
        "distinct_user_count",
        "pages_per_session_percentage",
        "total_time",
        "active_time",
    }
    url_columns = {"url"}

    original_rows = df.height
    df = trim_string_columns(df)
    df = cast_numeric_columns(df, numeric_columns)
    df = clean_url_columns(df, url_columns)

    if "url" in df.columns:
        df = df.with_columns(pl.col("url").map_elements(extract_url_path, return_dtype=pl.String).alias("ruta_url"))

    # Descarta filas sin URL cuando la URL es dimension analitica principal.
    if METRICS_DROP_NULL_URL and "url" in df.columns:
        df = df.filter(pl.col("url").is_not_null() & (pl.col("url").str.len_chars() > 0))

    dedup_removed = 0
    if METRICS_DEDUP_ENABLED:
        dedup_keys = [standardize_column_name(part) for part in METRICS_DEDUP_KEYS_RAW.split(",") if part.strip()]
        available_keys = [column for column in dedup_keys if column in df.columns]
        if available_keys:
            before_dedup = df.height
            # keep='first' preserva la primera observacion por combinacion de llaves.
            df = df.unique(subset=available_keys, keep="first", maintain_order=True)
            dedup_removed = before_dedup - df.height

    if METRICS_NUMERIC_FILL_NULL_WITH_ZERO:
        available_numeric = [column for column in numeric_columns if column in df.columns]
        if available_numeric:
            # Util para modelos/visuales que no toleran nulos en numericos.
            df = df.with_columns([pl.col(column).fill_null(0.0).alias(column) for column in available_numeric])

    # Adjunta metadatos de transformacion para trazabilidad en el reporte.
    df = df.with_columns(
        [
            pl.lit(original_rows).alias("_source_rows"),
            pl.lit(dedup_removed).alias("_dedup_removed_rows"),
        ]
    )

    return df


def export_parquet(df: pl.DataFrame, output_path: Path) -> None:
    df.write_parquet(output_path)


def run_pipeline() -> tuple[pl.DataFrame, pl.DataFrame]:
    ensure_directories()
    before_recordings = summarize_parquet(RECORDINGS_OUTPUT) if RECORDINGS_OUTPUT.exists() else None
    before_metrics = summarize_parquet(METRICS_OUTPUT) if METRICS_OUTPUT.exists() else None

    recordings_df = read_csv_with_standardized_columns(RECORDINGS_PATH)
    metrics_df = read_csv_with_standardized_columns(METRICS_PATH)

    recordings_clean = clean_recordings(recordings_df)
    metrics_clean = clean_metrics(metrics_df)
    metrics_export = metrics_clean.drop([column for column in ["_source_rows", "_dedup_removed_rows"] if column in metrics_clean.columns])

    export_parquet(recordings_clean, RECORDINGS_OUTPUT)
    export_parquet(metrics_clean, METRICS_OUTPUT)

    return recordings_clean, metrics_clean


def _ensure_parquet_exists(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(
            f"No se encontro {path.name}. Ejecuta `python src/data_engine.py` para regenerar los parquet limpios."
        )


@lru_cache(maxsize=1)
def load_recordings() -> pl.DataFrame:
    """Load the cleaned session-level parquet once and reuse it."""
    _ensure_parquet_exists(RECORDINGS_OUTPUT)
    return pl.read_parquet(RECORDINGS_OUTPUT)


@lru_cache(maxsize=1)
def load_metrics() -> pl.DataFrame:
    """Load the cleaned aggregated parquet once and reuse it."""
    _ensure_parquet_exists(METRICS_OUTPUT)
    return pl.read_parquet(METRICS_OUTPUT)


def clear_caches() -> None:
    """Clear cached parquet reads."""
    load_recordings.cache_clear()
    load_metrics.cache_clear()


def _safe_round(value: Any, digits: int = 2) -> float | None:
    if value is None:
        return None
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def _to_records(df: pl.DataFrame) -> list[JsonDict]:
    return [{key: _safe_round(value) if isinstance(value, float) else value for key, value in row.items()} for row in df.to_dicts()]


def _time_bucket_expr(column_name: str) -> pl.Expr:
    return (
        pl.when(pl.col(column_name).is_null())
        .then(pl.lit("sin_dato"))
        .when(pl.col(column_name) < 5)
        .then(pl.lit("0_a_5s"))
        .when(pl.col(column_name) < 15)
        .then(pl.lit("5_a_15s"))
        .when(pl.col(column_name) < 30)
        .then(pl.lit("15_a_30s"))
        .when(pl.col(column_name) < 60)
        .then(pl.lit("30_a_60s"))
        .otherwise(pl.lit("60s_o_mas"))
    )


def _canal_origen_expr() -> pl.Expr:
    return (
        pl.when(pl.col("trafico_externo") == True)
        .then(
            pl.when(pl.col("referente").is_not_null())
            .then(pl.lit("externo_con_referente"))
            .otherwise(pl.lit("externo_sin_referente"))
        )
        .otherwise(
            pl.when(pl.col("referente").is_not_null())
            .then(pl.lit("interno_o_referido"))
            .otherwise(pl.lit("directo_o_desconocido"))
        )
        .alias("canal_origen")
    )


def obtener_paginas_top(limite: int = 10) -> JsonDict:
    """Obtiene el ranking de las paginas mas vistas y con mayor interaccion.

    Args:
        limite: Numero maximo de paginas a devolver. Debe estar entre 1 y 50.

    Returns:
        Un diccionario simple con el resumen del ranking por pagina. Cada registro
        incluye la ruta, volumen de vistas, sesiones detectadas en recordings,
        interacciones totales, clics totales, tiempo promedio por pagina y la
        tasa de sesiones que alcanzaron una pagina de pricing desde esa entrada.

    Raises:
        ValueError: Si el limite solicitado esta fuera del rango permitido.
        FileNotFoundError: Si los parquet limpios aun no existen.
    """
    if not 1 <= limite <= 50:
        raise ValueError("`limite` debe estar entre 1 y 50.")

    recordings = load_recordings()
    metrics = load_metrics()

    page_views = (
        metrics.lazy()
        .group_by("ruta_url")
        .agg(
            [
                pl.col("pages_views").sum().fill_null(0).alias("page_views"),
                pl.col("sessions_count").sum().fill_null(0).alias("metric_sessions"),
                pl.col("average_scroll_depth").mean().alias("avg_scroll_depth"),
            ]
        )
        .rename({"ruta_url": "pagina"})
    )

    page_interactions = (
        recordings.lazy()
        .group_by("ruta_entrada")
        .agg(
            [
                pl.len().alias("recording_sessions"),
                pl.col("clics_sesion").sum().alias("total_clicks"),
                pl.col("interaccion_total").sum().alias("total_interactions"),
                pl.col("tiempo_por_pagina").mean().alias("avg_time_per_page_seconds"),
                pl.col("llego_a_pricing").mean().mul(100).alias("pricing_reach_rate_pct"),
            ]
        )
        .rename({"ruta_entrada": "pagina"})
    )

    ranking = (
        page_views.join(page_interactions, on="pagina", how="outer_coalesce")
        .with_columns(
            [
                pl.coalesce([pl.col("page_views"), pl.col("recording_sessions"), pl.lit(0.0)]).alias("page_views"),
                pl.coalesce([pl.col("recording_sessions"), pl.lit(0)]).alias("recording_sessions"),
                pl.coalesce([pl.col("total_clicks"), pl.lit(0.0)]).alias("total_clicks"),
                pl.coalesce([pl.col("total_interactions"), pl.lit(0.0)]).alias("total_interactions"),
            ]
        )
        .filter(pl.col("pagina").is_not_null())
        .sort(["page_views", "total_interactions", "recording_sessions"], descending=[True, True, True])
        .limit(limite)
        .collect()
    )

    return {
        "metrica": "paginas_top",
        "limite": limite,
        "criterio_orden": ["page_views", "total_interactions", "recording_sessions"],
        "resultados": _to_records(ranking),
    }


def calcular_tasas_abandono() -> JsonDict:
    """Calcula los puntos criticos de abandono rapido a nivel de pagina.

    Returns:
        Un diccionario con el abandono global y una lista de paginas criticas.
        Cada pagina incluye sesiones observadas, volumen de abandonos, porcentaje
        de abandono rapido, tiempo promedio por pagina y engagement promedio.
        El resultado filtra paginas con suficiente volumen para evitar ruido.

    Raises:
        FileNotFoundError: Si los parquet limpios aun no existen.
    """
    recordings = load_recordings()

    global_summary = (
        recordings.lazy()
        .select(
            [
                pl.len().alias("sesiones_totales"),
                pl.col("abandono_rapido").sum().alias("abandono_rapido_total"),
                pl.col("abandono_rapido").mean().mul(100).alias("tasa_abandono_global_pct"),
            ]
        )
        .collect()
        .to_dicts()[0]
    )

    critical_pages = (
        recordings.lazy()
        .group_by("ruta_entrada")
        .agg(
            [
                pl.len().alias("sesiones"),
                pl.col("abandono_rapido").sum().alias("abandono_rapido"),
                pl.col("abandono_rapido").mean().mul(100).alias("tasa_abandono_pct"),
                pl.col("tiempo_por_pagina").mean().alias("tiempo_promedio_por_pagina_seg"),
                pl.col("standarized_engagement_score").mean().alias("engagement_promedio"),
            ]
        )
        .filter(pl.col("sesiones") >= 25)
        .sort(["tasa_abandono_pct", "sesiones"], descending=[True, True])
        .limit(10)
        .rename({"ruta_entrada": "pagina"})
        .collect()
    )

    return {
        "metrica": "tasas_abandono",
        "resumen_global": {key: _safe_round(value) if isinstance(value, float) else value for key, value in global_summary.items()},
        "paginas_criticas": _to_records(critical_pages),
    }


def analizar_patrones_conversion() -> JsonDict:
    """Analiza el comportamiento de sesiones que llegan a paginas tipo pricing.

    Returns:
        Un diccionario con la tasa global de llegada a pricing, el rendimiento
        por dispositivo, el rendimiento por canal externo/interno, las paginas
        de entrada que mas conducen a pricing y los flujos hacia pricing mas
        repetidos. Esta salida ayuda al LLM a explicar conversiones y caminos
        de navegacion de alto valor.

    Raises:
        FileNotFoundError: Si los parquet limpios aun no existen.
    """
    recordings = load_recordings()

    resumen_global = (
        recordings.lazy()
        .select(
            [
                pl.len().alias("sesiones_totales"),
                pl.col("llego_a_pricing").sum().alias("sesiones_con_pricing"),
                pl.col("llego_a_pricing").mean().mul(100).alias("tasa_llegada_pricing_pct"),
            ]
        )
        .collect()
        .to_dicts()[0]
    )

    rendimiento_dispositivo = (
        recordings.lazy()
        .group_by("dispositivo")
        .agg(
            [
                pl.len().alias("sesiones"),
                pl.col("llego_a_pricing").sum().alias("sesiones_con_pricing"),
                pl.col("llego_a_pricing").mean().mul(100).alias("tasa_pricing_pct"),
                pl.col("standarized_engagement_score").mean().alias("engagement_promedio"),
            ]
        )
        .sort(["tasa_pricing_pct", "sesiones"], descending=[True, True])
        .collect()
    )

    rendimiento_canal = (
        recordings.lazy()
        .with_columns(_canal_origen_expr())
        .group_by("canal_origen")
        .agg(
            [
                pl.len().alias("sesiones"),
                pl.col("llego_a_pricing").sum().alias("sesiones_con_pricing"),
                pl.col("llego_a_pricing").mean().mul(100).alias("tasa_pricing_pct"),
                pl.col("standarized_engagement_score").mean().alias("engagement_promedio"),
            ]
        )
        .sort(["tasa_pricing_pct", "sesiones"], descending=[True, True])
        .collect()
    )

    top_entry_pages = (
        recordings.lazy()
        .group_by("ruta_entrada")
        .agg(
            [
                pl.len().alias("sesiones"),
                pl.col("llego_a_pricing").sum().alias("sesiones_con_pricing"),
                pl.col("llego_a_pricing").mean().mul(100).alias("tasa_pricing_pct"),
            ]
        )
        .filter(pl.col("sesiones") >= 20)
        .sort(["tasa_pricing_pct", "sesiones"], descending=[True, True])
        .limit(10)
        .rename({"ruta_entrada": "pagina_entrada"})
        .collect()
    )

    top_conversion_flows = (
        recordings.lazy()
        .filter(pl.col("llego_a_pricing") == True)
        .group_by("ruta_flujo")
        .agg(pl.len().alias("sesiones"))
        .sort("sesiones", descending=True)
        .limit(10)
        .collect()
    )

    return {
        "metrica": "patrones_conversion",
        "resumen_global": {key: _safe_round(value) if isinstance(value, float) else value for key, value in resumen_global.items()},
        "rendimiento_por_dispositivo": _to_records(rendimiento_dispositivo),
        "rendimiento_por_canal": _to_records(rendimiento_canal),
        "paginas_entrada_top": _to_records(top_entry_pages),
        "flujos_hacia_pricing": _to_records(top_conversion_flows),
    }


def obtener_flujos_frecuentes() -> JsonDict:
    """Devuelve las secuencias de navegacion mas repetidas y su calidad.

    Returns:
        Un diccionario con los flujos de navegacion mas frecuentes construidos
        desde la columna `ruta_flujo`. Cada flujo incluye numero de sesiones,
        tasa de abandono, tasa de llegada a pricing y duracion media de sesion
        para que el LLM pueda describir recorridos saludables o problematicos.

    Raises:
        FileNotFoundError: Si los parquet limpios aun no existen.
    """
    recordings = load_recordings()

    flows = (
        recordings.lazy()
        .group_by("ruta_flujo")
        .agg(
            [
                pl.len().alias("sesiones"),
                pl.col("abandono_rapido").mean().mul(100).alias("tasa_abandono_pct"),
                pl.col("llego_a_pricing").mean().mul(100).alias("tasa_pricing_pct"),
                pl.col("duracion_sesion_segundos").mean().alias("duracion_promedio_sesion_seg"),
            ]
        )
        .sort(["sesiones", "tasa_pricing_pct"], descending=[True, True])
        .limit(15)
        .collect()
    )

    return {
        "metrica": "flujos_frecuentes",
        "resultados": _to_records(flows),
    }


def obtener_interaccion_promedio() -> JsonDict:
    """Calcula promedios globales y por pagina de clics, scroll y tiempo.

    Returns:
        Un diccionario con metricas promedio globales de interaccion tomadas de
        recordings y metrics, junto con un ranking por pagina que combina vistas,
        scroll medio, clics por sesion, tiempo por pagina e interaccion total.
        La salida esta pensada para que Gemini responda preguntas generales de
        engagement sin tener que recalcular nada.

    Raises:
        FileNotFoundError: Si los parquet limpios aun no existen.
    """
    recordings = load_recordings()
    metrics = load_metrics()

    resumen_global_recordings = (
        recordings.lazy()
        .select(
            [
                pl.col("clics_sesion").mean().alias("clics_promedio_sesion"),
                pl.col("tiempo_por_pagina").mean().alias("tiempo_promedio_por_pagina_seg"),
                pl.col("duracion_sesion_segundos").mean().alias("duracion_promedio_sesion_seg"),
                pl.col("interaccion_total").mean().alias("interaccion_promedio_total"),
            ]
        )
        .collect()
        .to_dicts()[0]
    )

    resumen_global_metrics = (
        metrics.lazy()
        .select(
            [
                pl.col("average_scroll_depth").mean().alias("scroll_promedio_pct"),
                pl.col("pages_views").sum().alias("page_views_totales"),
                pl.col("sessions_count").sum().alias("metric_sessions_totales"),
            ]
        )
        .collect()
        .to_dicts()[0]
    )

    per_page_recordings = (
        recordings.lazy()
        .group_by("ruta_entrada")
        .agg(
            [
                pl.len().alias("sesiones"),
                pl.col("clics_sesion").mean().alias("clics_promedio_sesion"),
                pl.col("tiempo_por_pagina").mean().alias("tiempo_promedio_por_pagina_seg"),
                pl.col("interaccion_total").mean().alias("interaccion_promedio"),
            ]
        )
        .rename({"ruta_entrada": "pagina"})
    )

    per_page_metrics = (
        metrics.lazy()
        .group_by("ruta_url")
        .agg(
            [
                pl.col("pages_views").sum().fill_null(0).alias("page_views"),
                pl.col("average_scroll_depth").mean().alias("scroll_promedio_pct"),
            ]
        )
        .rename({"ruta_url": "pagina"})
    )

    per_page = (
        per_page_recordings.join(per_page_metrics, on="pagina", how="outer_coalesce")
        .filter(pl.col("pagina").is_not_null())
        .sort(["interaccion_promedio", "page_views"], descending=[True, True])
        .limit(10)
        .collect()
    )

    return {
        "metrica": "interaccion_promedio",
        "resumen_global": {
            **{key: _safe_round(value) if isinstance(value, float) else value for key, value in resumen_global_recordings.items()},
            **{key: _safe_round(value) if isinstance(value, float) else value for key, value in resumen_global_metrics.items()},
        },
        "paginas_destacadas": _to_records(per_page),
    }


def obtener_insight_frustracion() -> JsonDict:
    """Cruza frustracion del usuario con el dispositivo utilizado.

    Returns:
        Un diccionario con la incidencia de `posible_frustracion` por tipo de
        dispositivo y, adicionalmente, una señal agregada desde metrics usando
        la metrica `RageClickCount`. Esto permite detectar si el hardware movil,
        desktop u otros entornos concentran mas sintomas de frustracion.

    Raises:
        FileNotFoundError: Si los parquet limpios aun no existen.
    """
    recordings = load_recordings()
    metrics = load_metrics()

    frustration_by_device = (
        recordings.lazy()
        .group_by("dispositivo")
        .agg(
            [
                pl.len().alias("sesiones"),
                pl.col("posible_frustracion").sum().alias("sesiones_con_frustracion"),
                pl.col("posible_frustracion").mean().mul(100).alias("tasa_frustracion_pct"),
                pl.col("clics_sesion").mean().alias("clics_promedio"),
                pl.col("tiempo_por_pagina").mean().alias("tiempo_promedio_por_pagina_seg"),
            ]
        )
        .rename({"dispositivo": "device"})
    )

    rage_metric = (
        metrics.lazy()
        .filter(pl.col("metric_name") == "RageClickCount")
        .group_by("device")
        .agg(pl.col("sessions_count").sum().fill_null(0).alias("rage_click_sessions_metric"))
    )

    results = (
        frustration_by_device.join(rage_metric, on="device", how="left")
        .sort(["tasa_frustracion_pct", "sesiones"], descending=[True, True])
        .collect()
    )

    return {
        "metrica": "insight_frustracion",
        "resultados": _to_records(results),
    }


def obtener_insight_calidad_trafico() -> JsonDict:
    """Compara la calidad del trafico por canal y por referente principal.

    Returns:
        Un diccionario con el engagement promedio, abandono y llegada a pricing
        segmentados por tipo de canal (`trafico_externo` mas presencia de
        referente). Tambien devuelve los referentes mas relevantes ordenados por
        engagement promedio, filtrando volumen minimo para que el analisis sea
        util para Gemini en preguntas de marketing.

    Raises:
        FileNotFoundError: Si los parquet limpios aun no existen.
    """
    recordings = load_recordings()

    by_channel = (
        recordings.lazy()
        .with_columns(
            [
                _canal_origen_expr(),
                pl.col("referente").map_elements(extract_domain, return_dtype=pl.String).alias("dominio_referente"),
            ]
        )
        .group_by("canal_origen")
        .agg(
            [
                pl.len().alias("sesiones"),
                pl.col("standarized_engagement_score").mean().alias("engagement_promedio"),
                pl.col("abandono_rapido").mean().mul(100).alias("tasa_abandono_pct"),
                pl.col("llego_a_pricing").mean().mul(100).alias("tasa_pricing_pct"),
                pl.col("clics_sesion").mean().alias("clics_promedio"),
            ]
        )
        .sort(["engagement_promedio", "sesiones"], descending=[True, True])
        .collect()
    )

    top_referrers = (
        recordings.lazy()
        .with_columns(pl.col("referente").map_elements(extract_domain, return_dtype=pl.String).alias("dominio_referente"))
        .filter(pl.col("dominio_referente").is_not_null())
        .group_by("dominio_referente")
        .agg(
            [
                pl.len().alias("sesiones"),
                pl.col("standarized_engagement_score").mean().alias("engagement_promedio"),
                pl.col("abandono_rapido").mean().mul(100).alias("tasa_abandono_pct"),
                pl.col("llego_a_pricing").mean().mul(100).alias("tasa_pricing_pct"),
            ]
        )
        .filter(pl.col("sesiones") >= 20)
        .sort(["engagement_promedio", "sesiones"], descending=[True, True])
        .limit(10)
        .collect()
    )

    return {
        "metrica": "insight_calidad_trafico",
        "resumen_por_canal": _to_records(by_channel),
        "top_referentes": _to_records(top_referrers),
    }


def obtener_insight_anatomia_abandono() -> JsonDict:
    """Describe en que condiciones ocurre el abandono rapido.

    Returns:
        Un diccionario con el abandono segmentado por sistema operativo y por
        rangos de `tiempo_por_pagina`, ademas de una tabla de combinaciones de
        riesgo entre ambos factores. Esto le permite al LLM explicar si el
        abandono sucede por sesiones demasiado cortas, por problemas en un OS
        especifico o por ambas cosas al mismo tiempo.

    Raises:
        FileNotFoundError: Si los parquet limpios aun no existen.
    """
    recordings = load_recordings()

    by_os = (
        recordings.lazy()
        .group_by("sistema_operativo")
        .agg(
            [
                pl.len().alias("sesiones"),
                pl.col("abandono_rapido").sum().alias("abandono_rapido"),
                pl.col("abandono_rapido").mean().mul(100).alias("tasa_abandono_pct"),
                pl.col("tiempo_por_pagina").mean().alias("tiempo_promedio_por_pagina_seg"),
            ]
        )
        .sort(["tasa_abandono_pct", "sesiones"], descending=[True, True])
        .collect()
    )

    risk_matrix = (
        recordings.lazy()
        .with_columns(_time_bucket_expr("tiempo_por_pagina").alias("rango_tiempo"))
        .group_by(["sistema_operativo", "rango_tiempo"])
        .agg(
            [
                pl.len().alias("sesiones"),
                pl.col("abandono_rapido").sum().alias("abandono_rapido"),
                pl.col("abandono_rapido").mean().mul(100).alias("tasa_abandono_pct"),
                pl.col("interaccion_total").mean().alias("interaccion_promedio"),
                pl.col("standarized_engagement_score").mean().alias("engagement_promedio"),
            ]
        )
        .filter(pl.col("sesiones") >= 30)
        .sort(["tasa_abandono_pct", "sesiones"], descending=[True, True])
        .limit(15)
        .collect()
    )

    return {
        "metrica": "insight_anatomia_abandono",
        "abandono_por_sistema_operativo": _to_records(by_os),
        "combinaciones_de_riesgo": _to_records(risk_matrix),
    }


ANALYTIC_TOOLS: list[Callable[..., JsonDict]] = [
    obtener_paginas_top,
    calcular_tasas_abandono,
    analizar_patrones_conversion,
    obtener_flujos_frecuentes,
    obtener_interaccion_promedio,
    obtener_insight_frustracion,
    obtener_insight_calidad_trafico,
    obtener_insight_anatomia_abandono,
]


if __name__ == "__main__":
    recordings_df, metrics_df, parquet_summary = run_pipeline()
    print(f"Recordings procesado: {recordings_df.height:,} filas -> {RECORDINGS_OUTPUT}")
    print(f"Metrics procesado: {metrics_df.height:,} filas -> {METRICS_OUTPUT}")
    print(f"Reporte parquet: {PARQUET_REPORT_OUTPUT}")
    print(parquet_summary)
