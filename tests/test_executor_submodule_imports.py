"""Regression tests for executor package submodule imports (coclab-l6be)."""

from __future__ import annotations

import subprocess
import sys

import pytest

# Submodule import order matters here only for test-ID readability.
EXECUTOR_SUBMODULES: list[str] = [
    "hhplab.recipe.executor.core",
    "hhplab.recipe.executor",
    "hhplab.recipe.executor.containment",
    "hhplab.recipe.executor.transforms",
    "hhplab.recipe.executor.manifest",
    "hhplab.recipe.executor.inputs",
    "hhplab.recipe.executor.resample",
    "hhplab.recipe.executor.msa_coc_panel",
    "hhplab.recipe.executor.panel",
    "hhplab.recipe.executor.panel_policies",
    "hhplab.recipe.executor.persistence",
]


@pytest.mark.parametrize("module", EXECUTOR_SUBMODULES)
def test_executor_submodule_imports_directly(module: str) -> None:
    """Each executor submodule must load in a fresh interpreter.

    Runs ``python -c "import <module>"`` in a subprocess so the test reflects
    the real "first import" scenario.
    """
    result = subprocess.run(
        [sys.executable, "-c", f"import {module}"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"Direct import of {module} failed — probable executor import "
        f"cycle regression (coclab-l6be).\n"
        f"stdout: {result.stdout}\n"
        f"stderr: {result.stderr}"
    )
