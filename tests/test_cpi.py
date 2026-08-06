"""Tests for BLS CPI-U ingest and recipe inflation adjustment.

Truth table for adjustment factors:

| value year | CPI-U | base year | base CPI-U | factor | 1000 adjusted |
|------------|-------|-----------|------------|--------|---------------|
| 2020       | 258.8 | 2022      | 292.7      | 1.131 | 1130.99       |
| 2022       | 292.7 | 2022      | 292.7      | 1.000 | 1000.00       |
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

from hhplab.naming import cpi_u_filename, cpi_u_path
from hhplab.recipe.executor.core import ExecutionContext
from hhplab.recipe.executor.panel import _apply_inflation_adjustment
from hhplab.recipe.recipe_schema import InflationAdjustmentPolicy, PanelPolicy
from hhplab.sources.bls.cpi.ingest import (
    CPI_U_ALL_ITEMS_SERIES_ID,
    fetch_cpi_u_annual_index,
    ingest_cpi_u,
)

CPI_FIXTURE = pd.DataFrame(
    {
        "year": [2020, 2021, 2022],
        "cpi_u": [258.8, 271.0, 292.7],
        "series_id": [CPI_U_ALL_ITEMS_SERIES_ID] * 3,
        "period": ["M13"] * 3,
        "period_name": ["Annual"] * 3,
        "data_source": ["bls_cpi_u"] * 3,
        "source_ref": ["https://www.bls.gov/cpi/"] * 3,
        "ingested_at": ["2026-01-01T00:00:00+00:00"] * 3,
    }
)


def _mock_cpi_response():
    return {
        "status": "REQUEST_SUCCEEDED",
        "message": [],
        "Results": {
            "series": [
                {
                    "seriesID": CPI_U_ALL_ITEMS_SERIES_ID,
                    "data": [
                        {
                            "year": "2022",
                            "period": "M13",
                            "periodName": "Annual",
                            "value": "292.7",
                        },
                        {
                            "year": "2021",
                            "period": "M13",
                            "periodName": "Annual",
                            "value": "271.0",
                        },
                        {
                            "year": "2020",
                            "period": "M13",
                            "periodName": "Annual",
                            "value": "258.8",
                        },
                    ],
                }
            ]
        },
    }


def test_fetch_cpi_u_annual_index_extracts_annual_rows() -> None:
    with patch("hhplab.sources.bls.cpi.ingest.httpx.Client") as mock_client:
        mock_response = mock_client.return_value.__enter__.return_value.post.return_value
        mock_response.json.return_value = _mock_cpi_response()

        result = fetch_cpi_u_annual_index(2020, 2022)

    assert list(result["year"]) == [2020, 2021, 2022]
    assert list(result["cpi_u"]) == [258.8, 271.0, 292.7]


def test_ingest_cpi_u_writes_canonical_artifact(tmp_path) -> None:
    with patch(
        "hhplab.sources.bls.cpi.ingest.fetch_cpi_u_annual_index",
        return_value=CPI_FIXTURE[["year", "cpi_u", "series_id", "period", "period_name"]],
    ):
        path = ingest_cpi_u(start_year=2020, end_year=2022, project_root=tmp_path)

    assert path == cpi_u_path(base_dir=tmp_path / "data")
    assert path.name == cpi_u_filename()
    written = pd.read_parquet(path)
    assert list(written.columns) == list(CPI_FIXTURE.columns)
    assert list(written["year"]) == [2020, 2021, 2022]


def test_recipe_inflation_adjustment_adds_base_year_dollar_columns(tmp_path) -> None:
    cpi_path = tmp_path / "data" / "curated" / "cpi" / cpi_u_filename()
    cpi_path.parent.mkdir(parents=True)
    CPI_FIXTURE.to_parquet(cpi_path)
    panel = pd.DataFrame(
        {
            "year": [2020, 2022],
            "median_gross_rent": [1000.0, 1000.0],
            "zori": [1500.0, 1500.0],
        }
    )
    policy = PanelPolicy(
        inflation_adjustment=InflationAdjustmentPolicy(
            base_year=2022,
            columns=["median_gross_rent", "zori"],
            factor_column="cpi_u_adjustment_factor",
        )
    )
    ctx = ExecutionContext(
        project_root=tmp_path,
        recipe=SimpleNamespace(datasets={}),  # type: ignore[arg-type]
    )

    adjusted, summary = _apply_inflation_adjustment(panel, policy=policy, ctx=ctx)

    factor_2020 = 292.7 / 258.8
    assert adjusted.loc[0, "median_gross_rent_2022_dollars"] == pytest.approx(
        1000.0 * factor_2020
    )
    assert adjusted.loc[1, "median_gross_rent_2022_dollars"] == pytest.approx(1000.0)
    assert adjusted.loc[0, "zori_2022_dollars"] == pytest.approx(1500.0 * factor_2020)
    assert adjusted.loc[0, "cpi_u_adjustment_factor"] == pytest.approx(factor_2020)
    assert summary is not None
    assert summary["base_year"] == 2022
    assert summary["output_columns"] == [
        "median_gross_rent_2022_dollars",
        "zori_2022_dollars",
    ]


def test_recipe_inflation_adjustment_reports_missing_cpi_year(tmp_path) -> None:
    cpi_path = tmp_path / "data" / "curated" / "cpi" / cpi_u_filename()
    cpi_path.parent.mkdir(parents=True)
    CPI_FIXTURE[CPI_FIXTURE["year"] != 2020].to_parquet(cpi_path)
    panel = pd.DataFrame({"year": [2020], "rent": [1000.0]})
    policy = PanelPolicy(
        inflation_adjustment=InflationAdjustmentPolicy(base_year=2022, columns=["rent"])
    )
    ctx = ExecutionContext(
        project_root=tmp_path,
        recipe=SimpleNamespace(datasets={}),  # type: ignore[arg-type]
    )

    with pytest.raises(Exception, match="missing panel year"):
        _apply_inflation_adjustment(panel, policy=policy, ctx=ctx)
