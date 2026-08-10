"""Contracts for explicit canonical package surfaces."""

from __future__ import annotations

from importlib import import_module

import pytest

CANONICAL_PACKAGE_CASES = [
    pytest.param("hhplab.analysis", id="analysis"),
    pytest.param("hhplab.artifacts", id="artifacts"),
    pytest.param("hhplab.artifacts.curated", id="artifacts-curated"),
    pytest.param("hhplab.artifacts.naming", id="artifacts-naming"),
    pytest.param("hhplab.diagnostics", id="diagnostics"),
    pytest.param("hhplab.geographies.boundaries", id="geographies-boundaries"),
    pytest.param(
        "hhplab.geographies.boundaries.census",
        id="geographies-boundaries-census",
    ),
    pytest.param("hhplab.sources.doj", id="sources-doj"),
    pytest.param("hhplab.sources.census", id="sources-census"),
    pytest.param("hhplab.sources.hud", id="sources-hud"),
    pytest.param("hhplab.sources.medsl", id="sources-medsl"),
    pytest.param("hhplab.storage", id="storage"),
]


@pytest.mark.parametrize("package_name", CANONICAL_PACKAGE_CASES)
def test_canonical_package_has_explicit_surface(package_name: str) -> None:
    package = import_module(package_name)

    assert package.__file__ is not None
    assert package.__file__.endswith("__init__.py")
