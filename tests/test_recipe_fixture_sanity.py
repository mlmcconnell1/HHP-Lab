"""Recipe-backed sanity checks for persisted panel parquet output.

This module owns small contract tests built around fixture recipes under
``tests/fixtures/recipes/``.  The fixture data is intentionally tiny so
expected rows can be inspected as a full truth table instead of inferred
from assertion prose.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pytest

from coclab.panel.finalize import COC_PANEL_COLUMNS
from coclab.recipe.executor import execute_recipe
from coclab.recipe.loader import load_recipe

REPO_ROOT = Path(__file__).resolve().parent.parent
RECIPE_FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "recipes"
COC_PANEL_SANITY_RECIPE = RECIPE_FIXTURE_DIR / "coc-panel-sanity.yaml"
COC_PANEL_OUTPUT = Path("outputs/coc-panel-sanity/panel__Y2020-2021@B2025.parquet")

PIT_ROWS: tuple[dict[str, object], ...] = (
    {
        "coc_id": "COC1",
        "pit_year": 2020,
        "pit_total": 10,
        "pit_sheltered": 7,
        "pit_unsheltered": 3,
    },
    {
        "coc_id": "COC2",
        "pit_year": 2020,
        "pit_total": 20,
        "pit_sheltered": 15,
        "pit_unsheltered": 5,
    },
    {
        "coc_id": "COC1",
        "pit_year": 2021,
        "pit_total": 11,
        "pit_sheltered": 8,
        "pit_unsheltered": 3,
    },
    {
        "coc_id": "COC2",
        "pit_year": 2021,
        "pit_total": 21,
        "pit_sheltered": 16,
        "pit_unsheltered": 5,
    },
)

ACS_ROWS: tuple[dict[str, object], ...] = (
    {
        "tract_geoid": "T1",
        "year": 2020,
        "total_population": 100,
        "median_household_income": 50000,
    },
    {
        "tract_geoid": "T2",
        "year": 2020,
        "total_population": 200,
        "median_household_income": 80000,
    },
    {
        "tract_geoid": "T1",
        "year": 2021,
        "total_population": 120,
        "median_household_income": 51000,
    },
    {
        "tract_geoid": "T2",
        "year": 2021,
        "total_population": 220,
        "median_household_income": 82000,
    },
)

XWALK_ROWS: tuple[dict[str, object], ...] = (
    {
        "coc_id": "COC1",
        "tract_geoid": "T1",
        "area_share": 1.0,
        "pop_share": 0.75,
    },
    {
        "coc_id": "COC1",
        "tract_geoid": "T2",
        "area_share": 0.25,
        "pop_share": 0.25,
    },
    {
        "coc_id": "COC2",
        "tract_geoid": "T2",
        "area_share": 0.75,
        "pop_share": 1.0,
    },
)

EXPECTED_EXTRA_COLUMNS: tuple[str, ...] = ("geo_id", "geo_type")
EXPECTED_NULL_COLUMNS: tuple[str, ...] = (
    "adult_population",
    "population_below_poverty",
    "median_gross_rent",
    "unemployment_rate",
)


@dataclass(frozen=True)
class PanelTruthRow:
    geo_id: str
    year: int
    pit_total: int
    pit_sheltered: int
    pit_unsheltered: int
    total_population: float
    median_household_income: float
    boundary_vintage_used: str
    source: str


def _expected_panel_truth_table() -> tuple[PanelTruthRow, ...]:
    pit_by_key = {
        (str(row["coc_id"]), int(row["pit_year"])): row
        for row in PIT_ROWS
    }
    acs_by_key = {
        (str(row["tract_geoid"]), int(row["year"])): row
        for row in ACS_ROWS
    }
    years = sorted({int(row["pit_year"]) for row in PIT_ROWS})
    coc_ids = sorted({str(row["coc_id"]) for row in PIT_ROWS})

    truth_rows: list[PanelTruthRow] = []
    for year in years:
        for coc_id in coc_ids:
            pit_row = pit_by_key[(coc_id, year)]
            xwalk_rows = [
                row for row in XWALK_ROWS
                if str(row["coc_id"]) == coc_id
            ]
            total_population = 0.0
            income_numerator = 0.0
            income_denominator = 0.0
            for xwalk_row in xwalk_rows:
                acs_row = acs_by_key[(str(xwalk_row["tract_geoid"]), year)]
                total_population += (
                    float(acs_row["total_population"]) * float(xwalk_row["area_share"])
                )
                income_numerator += (
                    float(acs_row["median_household_income"])
                    * float(xwalk_row["pop_share"])
                )
                income_denominator += float(xwalk_row["pop_share"])
            truth_rows.append(PanelTruthRow(
                geo_id=coc_id,
                year=year,
                pit_total=int(pit_row["pit_total"]),
                pit_sheltered=int(pit_row["pit_sheltered"]),
                pit_unsheltered=int(pit_row["pit_unsheltered"]),
                total_population=total_population,
                median_household_income=income_numerator / income_denominator,
                boundary_vintage_used="2025",
                source="coclab_panel",
            ))

    return tuple(truth_rows)


EXPECTED_PANEL_TRUTH_TABLE = _expected_panel_truth_table()


@dataclass(frozen=True)
class ExecutedFixturePanel:
    output_path: Path
    panel: pd.DataFrame


def _write_fixture_assets(project_root: Path) -> None:
    data_dir = project_root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    xwalk_dir = data_dir / "curated" / "xwalks"
    xwalk_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(PIT_ROWS).to_parquet(data_dir / "pit.parquet", index=False)
    pd.DataFrame(ACS_ROWS).to_parquet(data_dir / "acs_tract.parquet", index=False)
    pd.DataFrame(XWALK_ROWS).to_parquet(
        xwalk_dir / "xwalk__B2025xT2020.parquet",
        index=False,
    )


@pytest.fixture
def executed_coc_panel(tmp_path: Path) -> ExecutedFixturePanel:
    _write_fixture_assets(tmp_path)
    recipe = load_recipe(COC_PANEL_SANITY_RECIPE)
    results = execute_recipe(recipe, project_root=tmp_path, quiet=True)

    assert len(results) == 1
    assert results[0].success

    output_path = tmp_path / COC_PANEL_OUTPUT
    panel = pd.read_parquet(output_path).sort_values(["geo_id", "year"]).reset_index(drop=True)
    return ExecutedFixturePanel(output_path=output_path, panel=panel)


def test_coc_panel_sanity_recipe_writes_expected_panel_schema(
    executed_coc_panel: ExecutedFixturePanel,
):
    panel = executed_coc_panel.panel

    assert executed_coc_panel.output_path.exists()
    assert tuple(panel.columns[:len(COC_PANEL_COLUMNS)]) == tuple(COC_PANEL_COLUMNS)
    assert set(panel.columns) == set(COC_PANEL_COLUMNS + list(EXPECTED_EXTRA_COLUMNS))
    assert len(panel) == len(EXPECTED_PANEL_TRUTH_TABLE)
    assert set(panel["geo_type"]) == {"coc"}
    assert panel["boundary_changed"].tolist() == [False, False, False, False]

    for column in EXPECTED_NULL_COLUMNS:
        assert panel[column].isna().all(), f"Expected all-null canonical column: {column}"


@pytest.mark.parametrize(
    "expected",
    EXPECTED_PANEL_TRUTH_TABLE,
    ids=lambda expected: f"{expected.geo_id}-{expected.year}",
)
def test_coc_panel_sanity_recipe_writes_expected_truth_table(
    executed_coc_panel: ExecutedFixturePanel,
    expected: PanelTruthRow,
):
    panel = executed_coc_panel.panel
    row = panel[
        (panel["geo_id"] == expected.geo_id)
        & (panel["year"] == expected.year)
    ].iloc[0]

    assert row["coc_id"] == expected.geo_id
    assert row["geo_id"] == expected.geo_id
    assert row["pit_total"] == expected.pit_total
    assert row["pit_sheltered"] == expected.pit_sheltered
    assert row["pit_unsheltered"] == expected.pit_unsheltered
    assert row["total_population"] == pytest.approx(expected.total_population)
    assert row["median_household_income"] == pytest.approx(
        expected.median_household_income,
    )
    assert row["boundary_vintage_used"] == expected.boundary_vintage_used
    assert row["source"] == expected.source
