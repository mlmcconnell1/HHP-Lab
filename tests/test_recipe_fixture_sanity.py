"""Recipe-backed sanity checks for persisted panel parquet output.

This module owns small contract tests built around fixture recipes under
``tests/fixtures/recipes/``.  The fixture data is intentionally tiny so
expected rows can be inspected as a full truth table instead of inferred
from assertion prose.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

from hhplab.recipe.executor import execute_recipe
from hhplab.recipe.loader import load_recipe

REPO_ROOT = Path(__file__).resolve().parent.parent
RECIPE_FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "recipes"
COC_PANEL_SANITY_RECIPE = RECIPE_FIXTURE_DIR / "coc-panel-sanity.yaml"
COC_MAP_SANITY_RECIPE = RECIPE_FIXTURE_DIR / "coc-map-sanity.yaml"
MSA_PANEL_SANITY_RECIPE = RECIPE_FIXTURE_DIR / "msa-panel-sanity.yaml"
COC_PANEL_OUTPUT = Path("outputs/coc-panel-sanity/panel__Y2020-2021@B2025.parquet")
COC_MAP_OUTPUT = Path("outputs/coc-map-sanity/map__Y2020-2021@B2025.html")
MSA_PANEL_OUTPUT = Path(
    "outputs/msa-panel-sanity/panel__msa__Y2020-2021@Mcensusmsa2023.parquet"
)

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

RECIPE_COC_PANEL_COLUMNS: tuple[str, ...] = (
    "coc_id",
    "coc_name",
    "geo_type",
    "geo_id",
    "year",
    "pit_total",
    "pit_sheltered",
    "pit_unsheltered",
    "boundary_vintage_used",
    "acs5_vintage_used",
    "tract_vintage_used",
    "total_population",
    "population_density_per_sq_km",
    "median_household_income",
    "boundary_changed",
    "source",
)

MSA_PIT_ROWS: tuple[dict[str, object], ...] = (
    {
        "coc_id": "COC1",
        "pit_year": 2020,
        "pit_total": 100,
        "pit_sheltered": 60,
        "pit_unsheltered": 40,
    },
    {
        "coc_id": "COC2",
        "pit_year": 2020,
        "pit_total": 80,
        "pit_sheltered": 50,
        "pit_unsheltered": 30,
    },
    {
        "coc_id": "COC1",
        "pit_year": 2021,
        "pit_total": 120,
        "pit_sheltered": 70,
        "pit_unsheltered": 50,
    },
    {
        "coc_id": "COC2",
        "pit_year": 2021,
        "pit_total": 90,
        "pit_sheltered": 55,
        "pit_unsheltered": 35,
    },
)

MSA_ACS_2010_ROWS: tuple[dict[str, object], ...] = (
    {
        "tract_geoid": "01001000100",
        "year": 2020,
        "total_population": 100,
        "adult_population": 70,
        "population_below_poverty": 20,
        "median_household_income": 50000.0,
        "median_gross_rent": 1000.0,
    },
    {
        "tract_geoid": "01003000100",
        "year": 2020,
        "total_population": 200,
        "adult_population": 150,
        "population_below_poverty": 30,
        "median_household_income": 80000.0,
        "median_gross_rent": 1500.0,
    },
)

MSA_ACS_2020_ROWS: tuple[dict[str, object], ...] = (
    {
        "tract_geoid": "01001000101",
        "year": 2021,
        "total_population": 110,
        "adult_population": 75,
        "population_below_poverty": 21,
        "median_household_income": 52000.0,
        "median_gross_rent": 1050.0,
    },
    {
        "tract_geoid": "01003000101",
        "year": 2021,
        "total_population": 210,
        "adult_population": 155,
        "population_below_poverty": 31,
        "median_household_income": 81000.0,
        "median_gross_rent": 1525.0,
    },
)

MSA_PEP_ROWS: tuple[dict[str, object], ...] = (
    {"county_fips": "01001", "year": 2020, "population": 120},
    {"county_fips": "01003", "year": 2020, "population": 230},
    {"county_fips": "01001", "year": 2021, "population": 125},
    {"county_fips": "01003", "year": 2021, "population": 240},
)

MSA_MEMBERSHIP_ROWS: tuple[dict[str, object], ...] = (
    {
        "msa_id": "11111",
        "cbsa_code": "11111",
        "msa_name": "Alpha Metro",
        "county_fips": "01001",
        "county_name": "Alpha County",
        "state_name": "Test State",
        "central_outlying": "Central",
        "definition_version": "census_msa_2023",
    },
    {
        "msa_id": "22222",
        "cbsa_code": "22222",
        "msa_name": "Beta Metro",
        "county_fips": "01003",
        "county_name": "Beta County",
        "state_name": "Test State",
        "central_outlying": "Central",
        "definition_version": "census_msa_2023",
    },
)

MSA_DEFINITION_ROWS: tuple[dict[str, object], ...] = (
    {
        "msa_id": "11111",
        "cbsa_code": "11111",
        "msa_name": "Alpha Metro",
        "definition_version": "census_msa_2023",
    },
    {
        "msa_id": "22222",
        "cbsa_code": "22222",
        "msa_name": "Beta Metro",
        "definition_version": "census_msa_2023",
    },
)

RECIPE_MSA_PANEL_COLUMNS: tuple[str, ...] = (
    "msa_id",
    "geo_type",
    "geo_id",
    "year",
    "pit_total",
    "pit_sheltered",
    "pit_unsheltered",
    "definition_version_used",
    "acs5_vintage_used",
    "tract_vintage_used",
    "total_population",
    "adult_population",
    "population_below_poverty",
    "median_household_income",
    "median_gross_rent",
    "population",
    "boundary_changed",
    "source",
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
    acs5_vintage_used: str
    tract_vintage_used: str
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
                acs5_vintage_used=str(year),
                tract_vintage_used="2020",
                source="hhplab_panel",
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
    boundaries_dir = data_dir / "curated" / "coc_boundaries"
    boundaries_dir.mkdir(parents=True, exist_ok=True)
    xwalk_dir = data_dir / "curated" / "xwalks"
    xwalk_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(PIT_ROWS).to_parquet(data_dir / "pit.parquet", index=False)
    pd.DataFrame(ACS_ROWS).to_parquet(data_dir / "acs_tract.parquet", index=False)
    pd.DataFrame(XWALK_ROWS).to_parquet(
        xwalk_dir / "xwalk__B2025xT2020.parquet",
        index=False,
    )
    gpd.GeoDataFrame(
        {
            "coc_id": ["COC1", "COC2"],
            "coc_name": ["Fixture CoC 1", "Fixture CoC 2"],
            "boundary_vintage": ["2025", "2025"],
            "source": ["fixture", "fixture"],
        },
        geometry=[box(0, 0, 10, 10), box(10, 0, 20, 10)],
        crs="EPSG:4326",
    ).to_parquet(boundaries_dir / "coc__B2025.parquet")


@dataclass(frozen=True)
class MsaPanelTruthRow:
    msa_id: str
    year: int
    pit_total: float
    pit_sheltered: float
    pit_unsheltered: float
    total_population: float
    adult_population: float
    population_below_poverty: float
    median_household_income: float
    median_gross_rent: float
    population: float
    definition_version_used: str
    acs5_vintage_used: str
    tract_vintage_used: str
    source: str


def _expected_msa_panel_truth_table() -> tuple[MsaPanelTruthRow, ...]:
    pit_by_key = {
        (str(row["coc_id"]), int(row["pit_year"])): row
        for row in MSA_PIT_ROWS
    }
    acs_by_year = {
        2020: {str(row["tract_geoid"]): row for row in MSA_ACS_2010_ROWS},
        2021: {str(row["tract_geoid"]): row for row in MSA_ACS_2020_ROWS},
    }
    pep_by_key = {
        (str(row["county_fips"]), int(row["year"])): row
        for row in MSA_PEP_ROWS
    }

    return (
        MsaPanelTruthRow(
            msa_id="11111",
            year=2020,
            pit_total=50.0,
            pit_sheltered=30.0,
            pit_unsheltered=20.0,
            total_population=float(acs_by_year[2020]["01001000100"]["total_population"]),
            adult_population=float(acs_by_year[2020]["01001000100"]["adult_population"]),
            population_below_poverty=float(
                acs_by_year[2020]["01001000100"]["population_below_poverty"]
            ),
            median_household_income=float(
                acs_by_year[2020]["01001000100"]["median_household_income"]
            ),
            median_gross_rent=float(
                acs_by_year[2020]["01001000100"]["median_gross_rent"]
            ),
            population=float(pep_by_key[("01001", 2020)]["population"]),
            definition_version_used="census_msa_2023",
            acs5_vintage_used="2019",
            tract_vintage_used="2010",
            source="msa_panel",
        ),
        MsaPanelTruthRow(
            msa_id="22222",
            year=2020,
            pit_total=130.0,
            pit_sheltered=80.0,
            pit_unsheltered=50.0,
            total_population=float(acs_by_year[2020]["01003000100"]["total_population"]),
            adult_population=float(acs_by_year[2020]["01003000100"]["adult_population"]),
            population_below_poverty=float(
                acs_by_year[2020]["01003000100"]["population_below_poverty"]
            ),
            median_household_income=float(
                acs_by_year[2020]["01003000100"]["median_household_income"]
            ),
            median_gross_rent=float(
                acs_by_year[2020]["01003000100"]["median_gross_rent"]
            ),
            population=float(pep_by_key[("01003", 2020)]["population"]),
            definition_version_used="census_msa_2023",
            acs5_vintage_used="2019",
            tract_vintage_used="2010",
            source="msa_panel",
        ),
        MsaPanelTruthRow(
            msa_id="11111",
            year=2021,
            pit_total=60.0,
            pit_sheltered=35.0,
            pit_unsheltered=25.0,
            total_population=float(acs_by_year[2021]["01001000101"]["total_population"]),
            adult_population=float(acs_by_year[2021]["01001000101"]["adult_population"]),
            population_below_poverty=float(
                acs_by_year[2021]["01001000101"]["population_below_poverty"]
            ),
            median_household_income=float(
                acs_by_year[2021]["01001000101"]["median_household_income"]
            ),
            median_gross_rent=float(
                acs_by_year[2021]["01001000101"]["median_gross_rent"]
            ),
            population=float(pep_by_key[("01001", 2021)]["population"]),
            definition_version_used="census_msa_2023",
            acs5_vintage_used="2020",
            tract_vintage_used="2020",
            source="msa_panel",
        ),
        MsaPanelTruthRow(
            msa_id="22222",
            year=2021,
            pit_total=150.0,
            pit_sheltered=90.0,
            pit_unsheltered=60.0,
            total_population=float(acs_by_year[2021]["01003000101"]["total_population"]),
            adult_population=float(acs_by_year[2021]["01003000101"]["adult_population"]),
            population_below_poverty=float(
                acs_by_year[2021]["01003000101"]["population_below_poverty"]
            ),
            median_household_income=float(
                acs_by_year[2021]["01003000101"]["median_household_income"]
            ),
            median_gross_rent=float(
                acs_by_year[2021]["01003000101"]["median_gross_rent"]
            ),
            population=float(pep_by_key[("01003", 2021)]["population"]),
            definition_version_used="census_msa_2023",
            acs5_vintage_used="2020",
            tract_vintage_used="2020",
            source="msa_panel",
        ),
    )


EXPECTED_MSA_PANEL_TRUTH_TABLE = _expected_msa_panel_truth_table()


def _write_msa_fixture_assets(project_root: Path) -> None:
    data_dir = project_root / "data" / "curated"
    pit_dir = data_dir / "pit"
    pep_dir = data_dir / "pep"
    acs_dir = data_dir / "acs"
    msa_dir = data_dir / "msa"
    tiger_dir = data_dir / "tiger"
    boundaries_dir = data_dir / "coc_boundaries"

    for directory in (pit_dir, pep_dir, acs_dir, msa_dir, tiger_dir, boundaries_dir):
        directory.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(MSA_PIT_ROWS).to_parquet(
        pit_dir / "pit_vintage__P2024.parquet",
        index=False,
    )
    pd.DataFrame(MSA_PEP_ROWS).to_parquet(
        pep_dir / "pep_county__v2020.parquet",
        index=False,
    )
    pd.DataFrame(MSA_ACS_2010_ROWS).to_parquet(
        acs_dir / "acs5_tracts__A2019xT2010.parquet",
        index=False,
    )
    pd.DataFrame(MSA_ACS_2020_ROWS).to_parquet(
        acs_dir / "acs5_tracts__A2020xT2020.parquet",
        index=False,
    )
    pd.DataFrame(MSA_DEFINITION_ROWS).to_parquet(
        msa_dir / "msa_definitions__census_msa_2023.parquet",
        index=False,
    )
    pd.DataFrame(MSA_MEMBERSHIP_ROWS).to_parquet(
        msa_dir / "msa_county_membership__census_msa_2023.parquet",
        index=False,
    )

    pd.DataFrame({"tract_geoid": ["01001000100", "01003000100"]}).to_parquet(
        tiger_dir / "tracts__T2010.parquet",
        index=False,
    )
    pd.DataFrame({"tract_geoid": ["01001000101", "01003000101"]}).to_parquet(
        tiger_dir / "tracts__T2020.parquet",
        index=False,
    )

    gpd.GeoDataFrame(
        {"GEOID": ["01001", "01003"]},
        geometry=[box(0, 0, 10, 10), box(10, 0, 20, 10)],
        crs="ESRI:102003",
    ).to_parquet(tiger_dir / "counties__C2020.parquet")

    gpd.GeoDataFrame(
        {"coc_id": ["COC1", "COC2"]},
        geometry=[box(0, 0, 20, 10), box(10, 0, 20, 10)],
        crs="ESRI:102003",
    ).to_parquet(boundaries_dir / "coc__B2020.parquet")


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


@pytest.fixture
def executed_msa_panel(tmp_path: Path) -> ExecutedFixturePanel:
    _write_msa_fixture_assets(tmp_path)
    recipe = load_recipe(MSA_PANEL_SANITY_RECIPE)
    results = execute_recipe(recipe, project_root=tmp_path, quiet=True)

    assert len(results) == 1
    assert results[0].success

    output_path = tmp_path / MSA_PANEL_OUTPUT
    panel = pd.read_parquet(output_path).sort_values(["geo_id", "year"]).reset_index(drop=True)
    return ExecutedFixturePanel(output_path=output_path, panel=panel)


def test_coc_map_sanity_recipe_writes_expected_html(tmp_path: Path):
    _write_fixture_assets(tmp_path)
    recipe = load_recipe(COC_MAP_SANITY_RECIPE)
    results = execute_recipe(recipe, project_root=tmp_path, quiet=True)

    assert len(results) == 1
    assert results[0].success

    output_path = tmp_path / COC_MAP_OUTPUT
    assert output_path == tmp_path / "outputs" / "coc-map-sanity" / "map__Y2020-2021@B2025.html"
    assert output_path.exists()
    html = output_path.read_text(encoding="utf-8")
    assert "COC1" in html
    assert "Fixture CoC 1" in html
    assert "Leaflet" in html or "leaflet" in html


def test_coc_panel_sanity_recipe_writes_expected_panel_schema(
    executed_coc_panel: ExecutedFixturePanel,
):
    panel = executed_coc_panel.panel

    assert executed_coc_panel.output_path.exists()
    assert tuple(panel.columns) == RECIPE_COC_PANEL_COLUMNS
    assert len(panel) == len(EXPECTED_PANEL_TRUTH_TABLE)
    assert set(panel["geo_type"]) == {"coc"}
    assert panel["boundary_changed"].tolist() == [False, False, False, False]


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
    assert row["acs5_vintage_used"] == expected.acs5_vintage_used
    assert row["tract_vintage_used"] == expected.tract_vintage_used
    assert row["source"] == expected.source


def test_msa_panel_sanity_recipe_writes_expected_panel_schema(
    executed_msa_panel: ExecutedFixturePanel,
):
    panel = executed_msa_panel.panel

    assert executed_msa_panel.output_path.exists()
    assert tuple(panel.columns) == RECIPE_MSA_PANEL_COLUMNS
    assert len(panel) == len(EXPECTED_MSA_PANEL_TRUTH_TABLE)
    assert set(panel["geo_type"]) == {"msa"}
    assert "coc_id" not in panel.columns
    assert panel["boundary_changed"].tolist() == [False, False, False, False]


@pytest.mark.parametrize(
    "expected",
    EXPECTED_MSA_PANEL_TRUTH_TABLE,
    ids=lambda expected: f"{expected.msa_id}-{expected.year}",
)
def test_msa_panel_sanity_recipe_writes_expected_truth_table(
    executed_msa_panel: ExecutedFixturePanel,
    expected: MsaPanelTruthRow,
):
    panel = executed_msa_panel.panel
    row = panel[
        (panel["geo_id"] == expected.msa_id)
        & (panel["year"] == expected.year)
    ].iloc[0]

    assert row["msa_id"] == expected.msa_id
    assert row["geo_id"] == expected.msa_id
    assert row["pit_total"] == pytest.approx(expected.pit_total)
    assert row["pit_sheltered"] == pytest.approx(expected.pit_sheltered)
    assert row["pit_unsheltered"] == pytest.approx(expected.pit_unsheltered)
    assert row["total_population"] == pytest.approx(expected.total_population)
    assert row["adult_population"] == pytest.approx(expected.adult_population)
    assert row["population_below_poverty"] == pytest.approx(
        expected.population_below_poverty,
    )
    assert row["median_household_income"] == pytest.approx(
        expected.median_household_income,
    )
    assert row["median_gross_rent"] == pytest.approx(expected.median_gross_rent)
    assert row["population"] == pytest.approx(expected.population)
    assert row["definition_version_used"] == expected.definition_version_used
    assert row["acs5_vintage_used"] == expected.acs5_vintage_used
    assert row["tract_vintage_used"] == expected.tract_vintage_used
    assert row["source"] == expected.source


def test_msa_panel_sanity_recipe_materializes_expected_transform_artifacts(
    executed_msa_panel: ExecutedFixturePanel,
):
    project_root = executed_msa_panel.output_path.parents[2]
    transform_dir = project_root / ".recipe_cache" / "transforms"
    expected_files = {
        "coc_to_msa__coc_2020__census_msa_2023.parquet",
        "county_to_msa__county_2020__census_msa_2023.parquet",
        "tract_to_msa_2010__tract_2010__census_msa_2023.parquet",
        "tract_to_msa_2020__tract_2020__census_msa_2023.parquet",
    }

    assert expected_files == {path.name for path in transform_dir.glob("*.parquet")}
