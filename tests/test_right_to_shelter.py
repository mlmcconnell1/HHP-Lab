"""Synthetic contracts for year-coded right-to-shelter source and mapping."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from geopandas import GeoDataFrame
from shapely.geometry import Polygon
from typer.testing import CliRunner

from hhplab.artifacts.curated.policy import CANONICAL_PATTERNS, CURATED_SUBDIRS
from hhplab.cli.main import app
from hhplab.recipe.cache import RecipeCache
from hhplab.recipe.loader import load_recipe
from hhplab.results.runner import run_results
from hhplab.schema.contracts import frame_annotation_contract
from hhplab.sources.coverage import CORE_SOURCE_COVERAGE_SPECS
from hhplab.sources.right_to_shelter import (
    COUNTY_POPULATION_COLUMNS,
    JURISDICTION_SOURCE_COLUMNS,
    POPULATIONS,
    RightToShelterError,
    derive_rts_flags,
    map_coc_years,
    map_jurisdiction_years,
    map_msa_years,
    mapping_output_path,
    validate_jurisdiction_source_table,
    write_mapping_with_provenance,
)
from hhplab.storage.provenance import read_provenance

YEAR_2020 = 2020
YEAR_2021 = 2021
SYNTHETIC_CITATION = "Synthetic test authority; not a real legal citation"


def source_row(
    jurisdiction_id: str,
    year: int,
    *,
    strengths: dict[str, str] | None = None,
    weather: dict[str, bool] | None = None,
) -> dict[str, object]:
    strengths = strengths or {population: "none" for population in POPULATIONS}
    weather = weather or {population: False for population in POPULATIONS}
    row: dict[str, object] = {
        "jurisdiction_id": jurisdiction_id,
        "jurisdiction_type": "state",
        "year": year,
        "citation": SYNTHETIC_CITATION,
        "citation_source_type": "statute",
        "citation_redistribution": "not_applicable",
        "effective_date": f"{year}-01-01",
        "ended_date": "",
    }
    for population in POPULATIONS:
        row[f"{population}_strength"] = strengths[population]
        row[f"{population}_weather_only"] = weather[population]
    return row


def status_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=JURISDICTION_SOURCE_COLUMNS)


def population_frame(rows: list[tuple[str, int, int, int, int]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=COUNTY_POPULATION_COLUMNS)


@pytest.mark.parametrize(
    ("population", "expected"),
    [(population, f"rts_{population}") for population in POPULATIONS],
    ids=list(POPULATIONS),
)
def test_strength_coding_is_population_specific(population: str, expected: str) -> None:
    statuses = {name: "none" for name in POPULATIONS}
    statuses[population] = "enforceable"
    frame = status_frame([source_row("01", YEAR_2020, strengths=statuses)])

    coded = derive_rts_flags(frame, coding_rule="strict")

    assert coded.loc[0, expected] == 1
    assert coded[[column for column in coded if column.startswith("rts_")
                  and column != expected]].iloc[0].eq(0).all()


@pytest.mark.parametrize(
    ("strength", "weather_only", "strict", "broad_without_weather", "broad_with_weather"),
    [
        ("enforceable", False, 1, 1, 1),
        ("conditional", False, 0, 1, 1),
        ("weather_only", False, 0, 0, 1),
        ("none", True, 0, 0, 1),
        ("none", False, 0, 0, 0),
    ],
    ids=["enforceable", "conditional", "weather-level", "weather-flag", "none"],
)
def test_strict_and_broad_coding_truth_table(
    strength: str,
    weather_only: bool,
    strict: int,
    broad_without_weather: int,
    broad_with_weather: int,
) -> None:
    strengths = {population: "none" for population in POPULATIONS}
    weather = {population: False for population in POPULATIONS}
    strengths["families"] = strength
    weather["families"] = weather_only
    frame = status_frame([source_row("01", YEAR_2020, strengths=strengths, weather=weather)])

    assert derive_rts_flags(frame, coding_rule="strict").loc[0, "rts_families"] == strict
    broad = derive_rts_flags(frame, coding_rule="broad").loc[0, "rts_families"]
    assert broad == broad_without_weather
    assert derive_rts_flags(
        frame, coding_rule="broad", include_weather_only=True
    ).loc[0, "rts_families"] == broad_with_weather


@pytest.mark.parametrize(
    ("coding_rule", "include_weather_only"),
    [("strict", True)],
    ids=["strict-weather-only-rejected"],
)
def test_strict_coding_rejects_weather_only_option(
    coding_rule: str, include_weather_only: bool,
) -> None:
    frame = status_frame([source_row("01", YEAR_2020)])

    with pytest.raises(RightToShelterError, match="requires coding_rule='broad'"):
        derive_rts_flags(
            frame, coding_rule=coding_rule, include_weather_only=include_weather_only
        )


@pytest.mark.parametrize(
    ("bad_change", "message"),
    [
        ({"citation": ""}, "citation"),
        ({"single_adults_strength": "strong"}, "single_adults_strength"),
        ({"families_weather_only": "unknown"}, "families_weather_only"),
        ({"citation_source_type": "secondary", "citation_redistribution": "not_applicable"},
         "Secondary citations"),
        ({"effective_date": "2022-01-01", "ended_date": "2021-01-01"}, "end before"),
    ],
    ids=["citation-required", "strength-enum", "weather-boolean", "secondary-terms", "date-order"],
)
def test_source_table_validation_rejects_uncited_or_invalid_rows(
    bad_change: dict[str, object], message: str,
) -> None:
    row = source_row("01", YEAR_2020)
    row.update(bad_change)
    with pytest.raises(RightToShelterError, match=message):
        validate_jurisdiction_source_table(status_frame([row]))


def test_header_only_table_is_valid_schema_but_not_buildable(tmp_path: Path) -> None:
    empty = pd.DataFrame(columns=JURISDICTION_SOURCE_COLUMNS)

    validated = validate_jurisdiction_source_table(empty)

    assert validated.empty
    assert tuple(validated.columns) == JURISDICTION_SOURCE_COLUMNS


def test_multi_state_msa_uses_population_majority_and_emits_shares() -> None:
    statuses = status_frame([
        source_row("01", YEAR_2020, strengths={
            "single_adults": "enforceable", "families": "none", "pregnant_people": "none",
        }),
        source_row("12", YEAR_2020, strengths={
            "single_adults": "none", "families": "enforceable", "pregnant_people": "none",
        }),
    ])
    membership = pd.DataFrame({
        "geo_id": ["multi-state", "multi-state"],
        "county_fips": ["01001", "12001"],
        "allocation_share": [1.0, 1.0],
    })
    population = population_frame([
        ("01001", YEAR_2020, 80, 20, 30),
        ("12001", YEAR_2020, 20, 80, 70),
    ])

    mapped = map_msa_years(
        statuses,
        membership.rename(columns={"geo_id": "msa_id"}),
        population,
    )

    row = mapped.iloc[0]
    assert row["rts_single_adults"] == 1
    assert row["rts_families"] == 1
    assert row["rts_pregnant_people"] == 0
    assert row["rts_single_adults_population_share"] == pytest.approx(0.8)
    assert row["rts_families_population_share"] == pytest.approx(0.8)
    assert row["rts_pregnant_people_population_share"] == pytest.approx(0.0)


def test_nyc_substate_override_differs_from_rest_of_new_york() -> None:
    statuses = status_frame([
        source_row("36", YEAR_2020, strengths={
            "single_adults": "conditional", "families": "none", "pregnant_people": "none",
        }),
        source_row("NYC", YEAR_2020, strengths={
            "single_adults": "enforceable", "families": "none", "pregnant_people": "none",
        }),
    ])
    membership = pd.DataFrame({
        "geo_id": ["ny-metro", "ny-metro"],
        "county_fips": ["36061", "36001"],
    })
    population = population_frame([
        ("36061", YEAR_2020, 60, 50, 10),
        ("36001", YEAR_2020, 40, 50, 10),
    ])
    strict = map_jurisdiction_years(statuses, membership, population, coding_rule="strict")
    broad = map_jurisdiction_years(statuses, membership, population, coding_rule="broad")

    assert strict.loc[0, "rts_single_adults_population_share"] == pytest.approx(0.6)
    assert strict.loc[0, "rts_single_adults"] == 1
    assert broad.loc[0, "rts_single_adults_population_share"] == pytest.approx(1.0)


def test_coc_year_mapping_uses_projected_county_intersections() -> None:
    coc_geometries = GeoDataFrame(
        {"coc_id": ["synthetic-coc"]},
        geometry=[Polygon([(0, 0), (2.5, 0), (2.5, 1), (0, 1)])],
        crs="EPSG:5070",
    )
    county_geometries = GeoDataFrame(
        {"GEOID": ["01001", "12001"]},
        geometry=[
            Polygon([(0, 0), (2, 0), (2, 1), (0, 1)]),
            Polygon([(2, 0), (3, 0), (3, 1), (2, 1)]),
        ],
        crs="EPSG:5070",
    )
    statuses = status_frame([
        source_row("01", YEAR_2020, strengths={
            "single_adults": "enforceable", "families": "none", "pregnant_people": "none",
        }),
        source_row("12", YEAR_2020),
    ])
    population = population_frame([
        ("01001", YEAR_2020, 100, 100, 100),
        ("12001", YEAR_2020, 100, 100, 100),
    ])

    mapped = map_coc_years(statuses, coc_geometries, county_geometries, population)

    assert mapped.loc[0, "rts_single_adults"] == 1
    assert mapped.loc[0, "rts_single_adults_population_share"] == pytest.approx(2 / 3)


@pytest.mark.parametrize(
    ("geography_rule", "expected_flag"),
    [("majority", 0), ("any", 1)],
    ids=["majority", "any-coverage"],
)
def test_geography_flag_rule_is_configurable(
    geography_rule: str, expected_flag: int,
) -> None:
    statuses = status_frame([
        source_row("01", YEAR_2020, strengths={
            "single_adults": "enforceable", "families": "none", "pregnant_people": "none",
        }),
        source_row("12", YEAR_2020),
    ])
    membership = pd.DataFrame({"geo_id": ["unit", "unit"], "county_fips": ["01001", "12001"]})
    population = population_frame([
        ("01001", YEAR_2020, 40, 1, 1),
        ("12001", YEAR_2020, 60, 1, 1),
    ])

    mapped = map_jurisdiction_years(
        statuses, membership, population, geography_rule=geography_rule,
    )

    assert mapped.loc[0, "rts_single_adults"] == expected_flag
    assert mapped.loc[0, "rts_single_adults_population_share"] == pytest.approx(0.4)


def test_mid_panel_change_is_applied_to_each_year() -> None:
    statuses = status_frame([
        source_row("01", YEAR_2020, strengths={
            "single_adults": "enforceable", "families": "none", "pregnant_people": "none",
        }),
        source_row("01", YEAR_2021),
    ])
    membership = pd.DataFrame({"geo_id": ["one"], "county_fips": ["01001"]})
    population = population_frame([
        ("01001", YEAR_2020, 10, 10, 10),
        ("01001", YEAR_2021, 10, 10, 10),
    ])

    mapped = map_jurisdiction_years(statuses, membership, population)

    assert mapped["rts_single_adults"].tolist() == [1, 0]
    assert mapped["year"].tolist() == [YEAR_2020, YEAR_2021]


def test_unknown_jurisdiction_year_fails_instead_of_becoming_zero() -> None:
    membership = pd.DataFrame({"geo_id": ["one"], "county_fips": ["01001"]})
    population = population_frame([("01001", YEAR_2020, 1, 1, 1)])

    with pytest.raises(RightToShelterError, match="unknown status is not coded as zero"):
        map_jurisdiction_years(status_frame([]), membership, population)


def test_partial_coverage_annotation_reaches_results_package(tmp_path: Path) -> None:
    statuses = status_frame([
        source_row("01", YEAR_2020, strengths={
            "single_adults": "enforceable", "families": "none", "pregnant_people": "none",
        }),
        source_row("12", YEAR_2020),
        source_row("01", YEAR_2021, strengths={
            "single_adults": "enforceable", "families": "none", "pregnant_people": "none",
        }),
        source_row("12", YEAR_2021),
    ])
    membership = pd.DataFrame({
        "geo_id": ["multi-state", "multi-state"],
        "county_fips": ["01001", "12001"],
    })
    population = population_frame([
        ("01001", YEAR_2020, 80, 50, 50),
        ("12001", YEAR_2020, 20, 50, 50),
        ("01001", YEAR_2021, 80, 50, 50),
        ("12001", YEAR_2021, 20, 50, 50),
    ])
    mapped = map_jurisdiction_years(statuses, membership, population)
    mapped["rts_single_adults"] = mapped["rts_single_adults"].astype(float)
    mapped.attrs["measures"] = {
        "rts_single_adults": {"units": "indicator", "aggregation_semantics": "intensive"}
    }
    artifact_path = tmp_path / "rts.parquet"
    write_mapping_with_provenance(
        mapped,
        artifact_path,
        geography="msa",
        coding_rule="strict",
        include_weather_only=False,
        geography_rule="majority",
        boundary_vintage="synthetic",
        membership_vintage="synthetic",
    )

    persisted = RecipeCache().read_parquet(artifact_path)
    contract = frame_annotation_contract(persisted)
    assert contract is not None
    assert contract["records"][0]["category"] == "geographic_approximation"
    assert contract["bindings"]["right-to-shelter-geographic-approximation#r1"][
        "columns"]["rts_single_adults"] == [["multi-state", YEAR_2020], ["multi-state", YEAR_2021]]
    assert read_provenance(artifact_path).extra["coding_rule"] == "strict"

    recipe = load_recipe({
        "version": 2,
        "id": "rts-annotation-test",
        "title": "Synthetic RTS annotation propagation",
        "authors": ["test"],
        "universe": {"range": f"{YEAR_2020}-{YEAR_2021}"},
        "targets": [{"id": "target", "geometry": {"type": "coc", "vintage": 2025}}],
        "datasets": {},
        "pipelines": [{"id": "build", "target": "target", "steps": []}],
        "panels": {"panel": {
            "ref": {"pipeline": "build", "output": "panel"},
            "geo_key": "geo_id", "time_key": "year",
        }},
        "analyses": [{
            "id": "summary", "method": "descriptive", "input": "panels.panel",
            "sample": {}, "columns": ["rts_single_adults"], "stats": ["n", "mean"],
        }],
        "results": {
            "bundle_dir": "results/{id}/{run_id}", "include_panels": ["panel"],
            "report": {"title": "RTS annotation propagation"},
        },
    })
    run = run_results(
        recipe,
        original_recipe_bytes=json.dumps(recipe.model_dump(mode="json", by_alias=True)).encode(),
        outputs_root=tmp_path / "results",
        panel_frames={"panel": persisted},
        private=True,
    )

    bundle = Path(run["bundle_path"])
    annotations = json.loads((bundle / "annotations.json").read_bytes())
    assert annotations["applied"] == [{
        "id": "right-to-shelter-geographic-approximation", "revision": 1,
    }]
    assert "partially covered" in (bundle / "report" / "index.html").read_text()


def test_curated_layout_supports_canonical_table_and_output_names() -> None:
    assert "right_to_shelter" in CURATED_SUBDIRS
    assert any(
        pattern.fullmatch("right_to_shelter__jurisdiction_year.csv")
        for pattern in CANONICAL_PATTERNS["right_to_shelter"]
    )
    assert mapping_output_path("msa", YEAR_2020, YEAR_2021, "census_2025").name == (
        "right_to_shelter__msa__Y2020-2021@Mcensus_2025.parquet"
    )


def test_source_coverage_is_registered_with_dynamic_availability_note() -> None:
    spec = CORE_SOURCE_COVERAGE_SPECS["right_to_shelter"]

    assert spec.native_geo == "jurisdiction"
    assert "depends on cited rows" in spec.notes
    assert spec.first_year == 2007


def test_cli_json_error_tells_operator_how_to_add_source_rows(tmp_path: Path) -> None:
    population_path = tmp_path / "population.csv"
    pd.DataFrame(columns=COUNTY_POPULATION_COLUMNS).to_csv(population_path, index=False)
    missing_source = tmp_path / "right-to-shelter.csv"
    runner = CliRunner()

    result = runner.invoke(app, [
        "build", "right-to-shelter",
        "--county-population", str(population_path),
        "--coc-boundary-vintage", "synthetic",
        "--county-vintage", "synthetic",
        "--source-table", str(missing_source),
        "--json",
    ])

    payload = json.loads(result.output)
    assert result.exit_code == 1
    assert payload["status"] == "error"
    assert "Add cited rows" in payload["error"]


@pytest.mark.parametrize(
    ("coding_rule", "weather_option"),
    [("strict", "--include-weather-only")],
    ids=["strict-weather-only-rejected"],
)
def test_cli_json_error_rejects_strict_weather_only(
    tmp_path: Path, coding_rule: str, weather_option: str,
) -> None:
    population_path = tmp_path / "population.csv"
    pd.DataFrame(columns=COUNTY_POPULATION_COLUMNS).to_csv(population_path, index=False)
    runner = CliRunner()

    result = runner.invoke(app, [
        "build", "right-to-shelter",
        "--county-population", str(population_path),
        "--coc-boundary-vintage", "synthetic",
        "--county-vintage", "synthetic",
        "--coding-rule", coding_rule,
        weather_option,
        "--json",
    ])

    payload = json.loads(result.output)
    assert result.exit_code == 1
    assert payload["status"] == "error"
    assert "requires --coding-rule broad" in payload["error"]
