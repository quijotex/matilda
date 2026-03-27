from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from datetime import datetime

import polars as pl


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
    """Normaliza nombres de columnas a snake_case tolerante a camelCase."""
    cleaned = column_name.replace("\ufeff", "").strip()
    cleaned = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", cleaned)
    cleaned = re.sub(r"[\s\-/]+", "_", cleaned)
    cleaned = re.sub(r"[^\w]+", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned)
    return cleaned.strip("_").lower()


def ensure_directories() -> None:
    """Garantiza que exista el directorio de salida para parquet/reportes."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def normalize_url(value: Any) -> str | None:
    """Elimina query params/fragments y unifica slash final para evitar duplicados."""
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
    """Devuelve solo la ruta para construir flujos como /home -> /pricing."""
    normalized = normalize_url(value)
    if not normalized:
        return None

    parsed = urlsplit(normalized)
    path = parsed.path or "/"
    return path if path else "/"


def parse_bool(value: Any) -> bool | None:
    """Convierte variantes 0/1, yes/no, true/false a booleanos reales."""
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
    """Lee CSV con tolerancia a errores y normaliza nombres de columnas."""
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
    """Aplica trim a todas las columnas String para evitar ruido por espacios."""
    string_columns = [column for column, dtype in df.schema.items() if dtype == pl.String]
    if not string_columns:
        return df

    return df.with_columns(pl.col(string_columns).str.strip_chars())


def cast_numeric_columns(df: pl.DataFrame, numeric_columns: set[str]) -> pl.DataFrame:
    """Convierte columnas candidatas a Float64 removiendo separadores de miles."""
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
    """Normaliza columnas booleanas heterogeneas (0/1, yes/no, true/false)."""
    available_columns = [column for column in boolean_columns if column in df.columns]
    if not available_columns:
        return df

    return df.with_columns(
        [pl.col(column).map_elements(parse_bool, return_dtype=pl.Boolean).alias(column) for column in available_columns]
    )


def clean_url_columns(df: pl.DataFrame, url_columns: set[str]) -> pl.DataFrame:
    """Normaliza URLs para reducir cardinalidad artificial y duplicados."""
    available_columns = [column for column in url_columns if column in df.columns]
    if not available_columns:
        return df

    return df.with_columns(
        [pl.col(column).map_elements(normalize_url, return_dtype=pl.String).alias(column) for column in available_columns]
    )


def clean_recordings(df: pl.DataFrame) -> pl.DataFrame:
    """
    Limpieza principal de recordings:
    - casting de tipos (numericos/booleanos),
    - parsing fecha/hora y datetime consolidado,
    - enriquecimiento de rutas y flags de negocio.
    """
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
    """
    Limpieza principal de metrics:
    - casting de tipos y normalizacion URL,
    - deduplicacion configurable por llaves,
    - filtros opcionales para mejorar calidad del dataset.
    """
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
    """Escribe parquet optimizado para almacenamiento y lectura analitica."""
    df.write_parquet(
        output_path,
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
        row_group_size=PARQUET_ROW_GROUP_SIZE,
        statistics=True,
    )


def summarize_parquet(parquet_path: Path) -> dict[str, Any]:
    """Resume metadata y calidad básica de un parquet exportado."""
    df = pl.read_parquet(parquet_path)
    total_cells = df.height * max(df.width, 1)
    null_cells = int(df.null_count().sum_horizontal().item())
    null_ratio = (null_cells / total_cells) if total_cells else 0.0
    duplicate_rows = int(df.is_duplicated().sum()) if df.height else 0
    duplicate_ratio = (duplicate_rows / df.height) if df.height else 0.0
    quality_score = compute_quality_score(df.height, null_ratio, duplicate_ratio)

    return {
        "file": parquet_path.name,
        "size_bytes": parquet_path.stat().st_size,
        "size_mb": round(parquet_path.stat().st_size / (1024 * 1024), 3),
        "rows": df.height,
        "columns": df.width,
        "schema": {column: str(dtype) for column, dtype in df.schema.items()},
        "null_cells": null_cells,
        "null_ratio": round(null_ratio, 6),
        "duplicate_rows": duplicate_rows,
        "duplicate_ratio": round(duplicate_ratio, 6),
        "quality_score": round(quality_score, 2),
    }


def compute_quality_score(rows: int, null_ratio: float, duplicate_ratio: float) -> float:
    """
    Score 0-100: penaliza nulos, duplicados y datasets vacios.
    """
    score = 100.0
    score -= min(null_ratio * 100, 45.0)
    score -= min(duplicate_ratio * 100, 45.0)
    if rows == 0:
        score -= 10.0
    return max(0.0, score)


def generate_validation_alerts(before: dict[str, Any] | None, after: dict[str, Any]) -> list[str]:
    """Genera alertas de calidad comparando umbrales y cambios de volumen."""
    alerts: list[str] = []
    file_name = after["file"]

    if after["null_ratio"] > MAX_NULL_RATIO_ALERT:
        alerts.append(
            f"{file_name}: null_ratio={after['null_ratio']:.4f} supera umbral={MAX_NULL_RATIO_ALERT:.4f}"
        )

    if after["duplicate_ratio"] > MAX_DUPLICATE_RATIO_ALERT:
        alerts.append(
            f"{file_name}: duplicate_ratio={after['duplicate_ratio']:.4f} supera umbral={MAX_DUPLICATE_RATIO_ALERT:.4f}"
        )

    if before and before.get("rows", 0) > 0:
        row_drop_ratio = (before["rows"] - after["rows"]) / before["rows"]
        if row_drop_ratio > MAX_ROW_DROP_RATIO_ALERT:
            alerts.append(
                f"{file_name}: row_drop_ratio={row_drop_ratio:.4f} supera umbral={MAX_ROW_DROP_RATIO_ALERT:.4f}"
            )

    return alerts


def export_versioned_parquet(df: pl.DataFrame, latest_output_path: Path) -> Path | None:
    """Guarda snapshot con timestamp para trazabilidad historica."""
    if not PARQUET_VERSIONED_EXPORT:
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    versioned_name = f"{latest_output_path.stem}_{timestamp}.parquet"
    versioned_output_path = latest_output_path.parent / versioned_name
    export_parquet(df, versioned_output_path)
    return versioned_output_path


def extract_business_kpis(recordings_df: pl.DataFrame, metrics_df: pl.DataFrame) -> dict[str, Any]:
    """Calcula KPIs de negocio y operacion para el reporte final."""
    llego_a_pricing_pct = None
    if "llego_a_pricing" in recordings_df.columns and recordings_df.height > 0:
        pricing_sessions = int(recordings_df.select(pl.col("llego_a_pricing").fill_null(False).cast(pl.Int64).sum()).item())
        llego_a_pricing_pct = round((pricing_sessions / recordings_df.height) * 100, 3)

    abandono_rapido_pct = None
    if "abandono_rapido" in recordings_df.columns and recordings_df.height > 0:
        rebote_sessions = int(recordings_df.select(pl.col("abandono_rapido").fill_null(False).cast(pl.Int64).sum()).item())
        abandono_rapido_pct = round((rebote_sessions / recordings_df.height) * 100, 3)

    metrics_source_rows = int(metrics_df.select(pl.col("_source_rows").max()).item()) if "_source_rows" in metrics_df.columns else metrics_df.height
    metrics_dedup_removed_rows = (
        int(metrics_df.select(pl.col("_dedup_removed_rows").max()).item()) if "_dedup_removed_rows" in metrics_df.columns else 0
    )
    metrics_row_retention_pct = round((metrics_df.height / metrics_source_rows) * 100, 3) if metrics_source_rows > 0 else None

    return {
        "recordings_rows": recordings_df.height,
        "metrics_rows": metrics_df.height,
        "metrics_source_rows": metrics_source_rows,
        "metrics_dedup_removed_rows": metrics_dedup_removed_rows,
        "metrics_row_retention_pct": metrics_row_retention_pct,
        "llego_a_pricing_pct": llego_a_pricing_pct,
        "abandono_rapido_pct": abandono_rapido_pct,
    }


def build_before_after_comparison(before: dict[str, Any] | None, after: dict[str, Any]) -> dict[str, Any]:
    """Construye deltas before/after para tamano, calidad y volumen."""
    size_before = before.get("size_bytes") if before else None
    size_after = after["size_bytes"]
    size_diff = (size_after - size_before) if isinstance(size_before, int) else None
    size_reduction_pct = (
        round(((size_before - size_after) / size_before) * 100, 3) if isinstance(size_before, int) and size_before > 0 else None
    )

    return {
        "file": after["file"],
        "before_exists": before is not None,
        "size_bytes_before": size_before,
        "size_bytes_after": size_after,
        "size_bytes_diff": size_diff,
        "size_reduction_pct": size_reduction_pct,
        "rows_before": before.get("rows") if before else None,
        "rows_after": after["rows"],
        "rows_diff": (after["rows"] - before["rows"]) if before else None,
        "null_ratio_before": before.get("null_ratio") if before else None,
        "null_ratio_after": after["null_ratio"],
        "null_ratio_diff": (after["null_ratio"] - before["null_ratio"]) if before else None,
        "duplicate_rows_before": before.get("duplicate_rows") if before else None,
        "duplicate_rows_after": after["duplicate_rows"],
        "duplicate_rows_diff": (after["duplicate_rows"] - before["duplicate_rows"]) if before else None,
        "quality_score_before": before.get("quality_score") if before else None,
        "quality_score_after": after["quality_score"],
        "quality_score_diff": (after["quality_score"] - before["quality_score"]) if before else None,
    }


def export_parquet_report(
    report_path: Path,
    entries: list[dict[str, Any]],
    comparisons: list[dict[str, Any]],
    validations: dict[str, Any],
    business_kpis: dict[str, Any],
    versioned_outputs: dict[str, str | None],
) -> None:
    """Serializa el reporte completo de calidad/optimizacion en JSON."""
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "compression": PARQUET_COMPRESSION,
        "compression_level": PARQUET_COMPRESSION_LEVEL,
        "row_group_size": PARQUET_ROW_GROUP_SIZE,
        "versioned_export_enabled": PARQUET_VERSIONED_EXPORT,
        "thresholds": {
            "max_null_ratio_alert": MAX_NULL_RATIO_ALERT,
            "max_duplicate_ratio_alert": MAX_DUPLICATE_RATIO_ALERT,
            "max_row_drop_ratio_alert": MAX_ROW_DROP_RATIO_ALERT,
        },
        "files": entries,
        "before_after_comparison": comparisons,
        "validations": validations,
        "business_kpis": business_kpis,
        "versioned_outputs": versioned_outputs,
    }
    report_path.write_text(json.dumps(report, ensure_ascii=True, indent=2), encoding="utf-8")


def build_console_summary(comparisons: list[dict[str, Any]]) -> str:
    """Genera resumen ejecutivo de ahorro total en consola."""
    total_before = sum(item["size_bytes_before"] for item in comparisons if isinstance(item.get("size_bytes_before"), int))
    total_after = sum(item["size_bytes_after"] for item in comparisons if isinstance(item.get("size_bytes_after"), int))
    total_diff = total_after - total_before
    total_reduction_pct = ((total_before - total_after) / total_before * 100) if total_before > 0 else 0.0

    return (
        "Resumen ahorro parquet | "
        f"antes={total_before:,} bytes ({total_before / (1024 * 1024):.3f} MB) | "
        f"despues={total_after:,} bytes ({total_after / (1024 * 1024):.3f} MB) | "
        f"delta={total_diff:,} bytes ({total_diff / (1024 * 1024):.3f} MB) | "
        f"reduccion={total_reduction_pct:.3f}%"
    )


def run_pipeline() -> tuple[pl.DataFrame, pl.DataFrame, str]:
    """Orquesta carga, limpieza, exportacion, validaciones y reporte."""
    ensure_directories()
    before_recordings = summarize_parquet(RECORDINGS_OUTPUT) if RECORDINGS_OUTPUT.exists() else None
    before_metrics = summarize_parquet(METRICS_OUTPUT) if METRICS_OUTPUT.exists() else None

    recordings_df = read_csv_with_standardized_columns(RECORDINGS_PATH)
    metrics_df = read_csv_with_standardized_columns(METRICS_PATH)

    recordings_clean = clean_recordings(recordings_df)
    metrics_clean = clean_metrics(metrics_df)
    metrics_export = metrics_clean.drop([column for column in ["_source_rows", "_dedup_removed_rows"] if column in metrics_clean.columns])

    export_parquet(recordings_clean, RECORDINGS_OUTPUT)
    export_parquet(metrics_export, METRICS_OUTPUT)
    recordings_versioned = export_versioned_parquet(recordings_clean, RECORDINGS_OUTPUT)
    metrics_versioned = export_versioned_parquet(metrics_export, METRICS_OUTPUT)

    after_recordings = summarize_parquet(RECORDINGS_OUTPUT)
    after_metrics = summarize_parquet(METRICS_OUTPUT)

    comparisons = [
        build_before_after_comparison(before_recordings, after_recordings),
        build_before_after_comparison(before_metrics, after_metrics),
    ]

    all_alerts = generate_validation_alerts(before_recordings, after_recordings) + generate_validation_alerts(
        before_metrics, after_metrics
    )
    validations = {"ok": len(all_alerts) == 0, "alerts": all_alerts}
    business_kpis = extract_business_kpis(recordings_clean, metrics_clean)
    versioned_outputs = {
        "recordings": str(recordings_versioned) if recordings_versioned else None,
        "metrics": str(metrics_versioned) if metrics_versioned else None,
    }

    export_parquet_report(
        PARQUET_REPORT_OUTPUT,
        [after_recordings, after_metrics],
        comparisons,
        validations,
        business_kpis,
        versioned_outputs,
    )

    return recordings_clean, metrics_clean, build_console_summary(comparisons)


if __name__ == "__main__":
    recordings_df, metrics_df, parquet_summary = run_pipeline()
    print(f"Recordings procesado: {recordings_df.height:,} filas -> {RECORDINGS_OUTPUT}")
    print(f"Metrics procesado: {metrics_df.height:,} filas -> {METRICS_OUTPUT}")
    print(f"Reporte parquet: {PARQUET_REPORT_OUTPUT}")
    print(parquet_summary)
