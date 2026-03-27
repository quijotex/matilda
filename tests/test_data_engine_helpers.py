"""Tests for pure helpers in src.data_engine."""

from __future__ import annotations

import pytest

from src.data_engine import (
    extract_domain,
    extract_url_path,
    normalize_url,
    parse_bool,
    standardize_column_name,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("User Name", "user_name"),
        ("  Foo-Bar  ", "foo_bar"),
        ("camelCaseHere", "camel_case_here"),
        ("\ufeffBOMColumn", "bomcolumn"),
    ],
)
def test_standardize_column_name(raw: str, expected: str) -> None:
    assert standardize_column_name(raw) == expected


def test_normalize_url_strips_query_and_fragment() -> None:
    assert normalize_url("https://Example.com/path/?q=1#frag") == "https://example.com/path"


def test_normalize_url_empty_returns_none() -> None:
    assert normalize_url("") is None
    assert normalize_url("   ") is None
    assert normalize_url(None) is None


def test_extract_url_path() -> None:
    assert extract_url_path("https://site.com/pricing") == "/pricing"


def test_extract_domain() -> None:
    assert extract_domain("HTTPS://WWW.Site.COM/x") == "www.site.com"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        (True, True),
        ("yes", True),
        ("SÍ", True),
        ("no", False),
        (1, True),
        (0, False),
        ("maybe", None),
    ],
)
def test_parse_bool(value: object, expected: bool | None) -> None:
    assert parse_bool(value) == expected
