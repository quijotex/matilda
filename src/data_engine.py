from __future__ import annotations

import re
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import polars as pl


BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

RECORDINGS_PATH = RAW_DIR / "1_Data_Recordings.csv"
METRICS_PATH = RAW_DIR / "2_Data_Metrics.csv"

RECORDINGS_OUTPUT = PROCESSED_DIR / "recordings_clean.parquet"
METRICS_OUTPUT = PROCESSED_DIR / "metrics_clean.parquet"
PARQUET_REPORT_OUTPUT = PROCESSED_DIR / "parquet_quality_report.json"

PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 6
PARQUET_ROW_GROUP_SIZE = 50_000


def standardize_column_name(column_name: str) -> str:
    """Normaliza nombres de columnas a snake_case tolerante a camelCase."""
    cleaned = column_name.replace("\ufeff", "").strip()
    cleaned = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", cleaned)
    cleaned = re.sub(r"[\s\-/]+", "_", cleaned)
    cleaned = re.sub(r"[^\w]+", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned)
    return cleaned.strip("_").lower()


def ensure_directories() -> None:
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

    df = trim_string_columns(df)
    df = cast_numeric_columns(df, numeric_columns)
    df = clean_url_columns(df, url_columns)

    if "url" in df.columns:
        df = df.with_columns(pl.col("url").map_elements(extract_url_path, return_dtype=pl.String).alias("ruta_url"))

    return df


def export_parquet(df: pl.DataFrame, output_path: Path) -> None:
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
    }


def build_before_after_comparison(before: dict[str, Any] | None, after: dict[str, Any]) -> dict[str, Any]:
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
    }


def export_parquet_report(
    report_path: Path,
    entries: list[dict[str, Any]],
    comparisons: list[dict[str, Any]],
) -> None:
    report = {
        "compression": PARQUET_COMPRESSION,
        "compression_level": PARQUET_COMPRESSION_LEVEL,
        "row_group_size": PARQUET_ROW_GROUP_SIZE,
        "files": entries,
        "before_after_comparison": comparisons,
    }
    report_path.write_text(json.dumps(report, ensure_ascii=True, indent=2), encoding="utf-8")


def build_console_summary(comparisons: list[dict[str, Any]]) -> str:
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
    ensure_directories()
    before_recordings = summarize_parquet(RECORDINGS_OUTPUT) if RECORDINGS_OUTPUT.exists() else None
    before_metrics = summarize_parquet(METRICS_OUTPUT) if METRICS_OUTPUT.exists() else None

    recordings_df = read_csv_with_standardized_columns(RECORDINGS_PATH)
    metrics_df = read_csv_with_standardized_columns(METRICS_PATH)

    recordings_clean = clean_recordings(recordings_df)
    metrics_clean = clean_metrics(metrics_df)

    export_parquet(recordings_clean, RECORDINGS_OUTPUT)
    export_parquet(metrics_clean, METRICS_OUTPUT)

    after_recordings = summarize_parquet(RECORDINGS_OUTPUT)
    after_metrics = summarize_parquet(METRICS_OUTPUT)

    comparisons = [
        build_before_after_comparison(before_recordings, after_recordings),
        build_before_after_comparison(before_metrics, after_metrics),
    ]

    export_parquet_report(
        PARQUET_REPORT_OUTPUT,
        [after_recordings, after_metrics],
        comparisons,
    )

    return recordings_clean, metrics_clean, build_console_summary(comparisons)


if __name__ == "__main__":
    recordings_df, metrics_df, parquet_summary = run_pipeline()
    print(f"Recordings procesado: {recordings_df.height:,} filas -> {RECORDINGS_OUTPUT}")
    print(f"Metrics procesado: {metrics_df.height:,} filas -> {METRICS_OUTPUT}")
    print(f"Reporte parquet: {PARQUET_REPORT_OUTPUT}")
    print(parquet_summary)
