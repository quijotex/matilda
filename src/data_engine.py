from __future__ import annotations

import re
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
    df.write_parquet(output_path)


def run_pipeline() -> tuple[pl.DataFrame, pl.DataFrame]:
    ensure_directories()

    recordings_df = read_csv_with_standardized_columns(RECORDINGS_PATH)
    metrics_df = read_csv_with_standardized_columns(METRICS_PATH)

    recordings_clean = clean_recordings(recordings_df)
    metrics_clean = clean_metrics(metrics_df)

    export_parquet(recordings_clean, RECORDINGS_OUTPUT)
    export_parquet(metrics_clean, METRICS_OUTPUT)

    return recordings_clean, metrics_clean


if __name__ == "__main__":
    recordings_df, metrics_df = run_pipeline()
    print(f"Recordings procesado: {recordings_df.height:,} filas -> {RECORDINGS_OUTPUT}")
    print(f"Metrics procesado: {metrics_df.height:,} filas -> {METRICS_OUTPUT}")
