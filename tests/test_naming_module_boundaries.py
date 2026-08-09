"""Focused ownership and golden-output contracts for artifact naming."""

from __future__ import annotations

from importlib import import_module

import pytest

import hhplab.artifacts.naming.naming as naming

NAMING_OWNER_CASES = [
    pytest.param("boundary_filename", "geography", id="geography-boundary"),
    pytest.param("tract_xwalk_filename", "crosswalks", id="crosswalk"),
    pytest.param("panel_filename", "panels", id="panel"),
    pytest.param("zori_filename", "rents", id="rent"),
    pytest.param("pit_filename", "sources", id="source"),
    pytest.param("metro_definitions_filename", "definitions", id="definition"),
    pytest.param("analysis_output_filename", "analysis", id="analysis"),
    pytest.param("expand_acs_vintage", "shared", id="shared-token"),
]


GOLDEN_FILENAME_CASES = [
    pytest.param(
        "boundary_filename", ("2025",), "boundaries__B2025.parquet", id="boundary"
    ),
    pytest.param(
        "tract_xwalk_filename",
        ("2025", 2020),
        "xwalk__B2025xT2020.parquet",
        id="tract-xwalk",
    ),
    pytest.param(
        "panel_filename", (2015, 2024, "2025"), "panel__Y2015-2024@B2025.parquet", id="panel"
    ),
    pytest.param(
        "analysis_output_filename",
        ("panel.parquet", "describe"),
        "panel__analysis_describe.parquet",
        id="analysis",
    ),
]


@pytest.mark.parametrize(("name", "owner"), NAMING_OWNER_CASES)
def test_naming_facade_reexports_family_owner(name: str, owner: str) -> None:
    facade_export = getattr(naming, name)
    owned_export = getattr(import_module(f"hhplab.artifacts.naming.{owner}"), name)

    assert facade_export is owned_export
    assert facade_export.__module__ == f"hhplab.artifacts.naming.{owner}"


@pytest.mark.parametrize(("name", "args", "expected"), GOLDEN_FILENAME_CASES)
def test_canonical_filename_golden_contracts(
    name: str, args: tuple[object, ...], expected: str
) -> None:
    assert getattr(naming, name)(*args) == expected


def test_naming_facade_declares_complete_public_surface() -> None:
    assert set(naming.__all__) >= {
        "boundary_filename",
        "tract_xwalk_filename",
        "panel_filename",
        "zori_filename",
        "analysis_output_filename",
        "metro_definitions_filename",
    }
