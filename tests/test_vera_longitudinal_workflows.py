"""Tests for Vera longitudinal workflow log guards."""

from __future__ import annotations

import importlib
import warnings

import numpy as np
import pandas as pd
import pytest


@pytest.mark.parametrize(
    "module_name",
    [
        "build_vera_hic_pit_longitudinal",
        "build_vera_hic_pit_longitudinal_pooled",
    ],
)
def test_vera_longitudinal_safe_log_avoids_runtime_warning(module_name: str) -> None:
    module = importlib.import_module(f"hhplab.results.workflows.{module_name}")
    series = pd.Series([2.0, 0.0, -3.0, np.nan])

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("error", RuntimeWarning)
        result = module._safe_log(series)

    assert result.iloc[0] == pytest.approx(np.log(2.0))
    assert pd.isna(result.iloc[1])
    assert pd.isna(result.iloc[2])
    assert pd.isna(result.iloc[3])
    assert caught == []
