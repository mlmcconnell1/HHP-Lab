"""Offline end-to-end acceptance for the shelter-capacity RecipeV2 example.

The named frame is deliberately synthetic and encodes only software contracts.
It makes PIT totals additive, masks the 2021 unsheltered/total cells, supplies a
2010 baseline for differences, and gives one geography the no-right-to-shelter
flag. It is not an estimate or a proposed research parameter choice.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from shutil import move

import pandas as pd
import pytest
import yaml
from typer.testing import CliRunner

from hhplab.cli.main import app
from hhplab.recipe.loader import load_recipe
from hhplab.recipe.results_plan import plan_results
from hhplab.results.runner import replay_results
from hhplab.storage.run_ledger import RunLedger

RUNNER = CliRunner()
YEARS = tuple(range(2010, 2025))
GEOGRAPHIES = ("g0", "g1", "g2", "g3")
BASELINE_YEAR = 2010
MASKED_YEAR = 2021
RIGHT_TO_SHELTER_GEOGRAPHY = "g3"
SYNTHETIC_PARAMETERS = {
    "delta_theta": 0.49,
    "delta_slope": 0.2,
    "fit_threshold": 0.01,
    "fit_floor": 0.0,
}

# Complete truth tables for each current-year membership decision.  The 2010
# row is excluded from annual changes but supplies their baseline. The mask
# propagates through annual 2021/2022 changes and biennial 2021/2023 changes.
ANNUAL_FD_TRUTH = {
    2010: "year_out_of_range",
    2011: "included",
    2012: "included",
    2013: "included",
    2014: "included",
    2015: "included",
    2016: "included",
    2017: "included",
    2018: "included",
    2019: "included",
    2020: "included",
    2021: "masked:d_unsheltered",
    2022: "masked:d_unsheltered",
    2023: "included",
    2024: "included",
}
BIENNIAL_FD_TRUTH = {
    2010: "year_out_of_range",
    2011: "year_out_of_range",
    2012: "year_out_of_range",
    2013: "included",
    2014: "year_out_of_range",
    2015: "included",
    2016: "year_out_of_range",
    2017: "included",
    2018: "year_out_of_range",
    2019: "included",
    2020: "year_out_of_range",
    2021: "masked:d2_unsheltered",
    2022: "year_out_of_range",
    2023: "masked:d2_unsheltered",
    2024: "year_out_of_range",
}
ODD_YEARS_FE_TRUTH = {
    year: (
        "masked:pit_unsheltered"
        if year == MASKED_YEAR
        else "included"
        if 2011 <= year <= 2023 and year % 2 == 1
        else "year_out_of_range"
    )
    for year in YEARS
}


SYNTHETIC_RECIPE = {
    "version": 2,
    "id": "shelter-capacity-integration-test",
    "title": "Synthetic shelter-capacity acceptance fixture",
    "authors": ["HHP-Lab test fixture"],
    "universe": {"range": "2010-2024"},
    "targets": [
        {"id": "cbsa", "geometry": {"type": "metro", "source": "glynn_fox_v1"}},
        {"id": "coc", "geometry": {"type": "coc", "vintage": 2025}},
    ],
    "datasets": {},
    "pipelines": [
        {"id": "build_cbsa_panel", "target": "cbsa", "steps": []},
        {"id": "build_coc_panel", "target": "coc", "steps": []},
    ],
    "panels": {
        "cbsa": {
            "ref": {"pipeline": "build_cbsa_panel", "output": "panel"},
            "geo_key": "geo_id",
            "time_key": "year",
        },
        "coc": {
            "ref": {"pipeline": "build_coc_panel", "output": "panel"},
            "geo_key": "geo_id",
            "time_key": "year",
        },
    },
    "parameters": SYNTHETIC_PARAMETERS,
    "masks": [
        {
            "id": "covid_unsheltered_2021",
            "columns": ["pit_unsheltered", "pit_total"],
            "years": [MASKED_YEAR],
            "reason": "synthetic 2021 mask acceptance case",
        }
    ],
    "derive": [
        {
            "id": "beds_shelter",
            "op": "sum",
            "of": [
                "hic_es_year_round_beds",
                "hic_th_year_round_beds",
                "hic_sh_year_round_beds",
            ],
            "require_all": True,
        },
        {
            "id": "utilization",
            "op": "ratio",
            "num": "pit_sheltered",
            "den": "beds_shelter",
            "zero_den": "null",
        },
        {"id": "d_beds", "op": "diff", "of": "beds_shelter", "years": 1},
        {"id": "d_sheltered", "op": "diff", "of": "pit_sheltered", "years": 1},
        {"id": "d_unsheltered", "op": "diff", "of": "pit_unsheltered", "years": 1},
        {"id": "d_total", "op": "diff", "of": "pit_total", "years": 1},
        {"id": "d_population", "op": "diff", "of": "population", "years": 1},
        {
            "id": "d_unemployment",
            "op": "diff",
            "of": "unemployment_rate",
            "years": 1,
        },
        {"id": "lag_d_beds", "op": "lag", "of": "d_beds", "years": 1},
        {"id": "d2_beds", "op": "diff", "of": "beds_shelter", "years": 2},
        {"id": "d2_sheltered", "op": "diff", "of": "pit_sheltered", "years": 2},
        {"id": "d2_unsheltered", "op": "diff", "of": "pit_unsheltered", "years": 2},
        {"id": "d2_total", "op": "diff", "of": "pit_total", "years": 2},
        {"id": "d2_population", "op": "diff", "of": "population", "years": 2},
        {
            "id": "d2_unemployment",
            "op": "diff",
            "of": "unemployment_rate",
            "years": 2,
        },
    ],
    "sample_groups": {"fd_main": {"sample": {"years": {"from": 2011, "to": 2024}}}},
    "analyses": [
        {
            "id": "levels_fit",
            "method": "regression",
            "input": "panels.cbsa",
            "sample": {"years": {"from": 2010, "to": 2024}},
            "outcome": "pit_sheltered",
            "predictors": ["beds_shelter"],
            "fixed_effects": ["year"],
            "inference": {"method": "clustered", "cluster_by": "geography"},
        },
        {
            "id": "utilization_dist",
            "method": "descriptive",
            "input": "panels.cbsa",
            "sample": {"years": {"from": 2010, "to": 2024}},
            "columns": ["utilization"],
            "stats": [
                "n",
                "median",
                "p10",
                "p25",
                "p75",
                "p90",
                {"share_gte": {"utilization": 0.9}},
            ],
            "by": ["year"],
        },
        {
            "id": "subst_fe",
            "method": "regression",
            "input": "panels.cbsa",
            "sample": {"years": {"from": 2010, "to": 2024}, "min_obs_per_geography": 2},
            "outcome": "pit_unsheltered",
            "predictors": ["beds_shelter"],
            "controls": ["population", "unemployment_rate", "median_gross_rent"],
            "fixed_effects": ["geography", "year"],
            "inference": {"method": "clustered", "cluster_by": "geography"},
        },
        {
            "id": "subst_fd",
            "method": "regression",
            "input": "panels.cbsa",
            "sample": {"group": "fd_main"},
            "outcome": "d_unsheltered",
            "predictors": ["d_beds"],
            "controls": ["d_population", "d_unemployment"],
            "fixed_effects": ["year"],
            "inference": {"method": "clustered", "cluster_by": "geography"},
        },
        {
            "id": "sheltered_fd",
            "method": "regression",
            "input": "panels.cbsa",
            "sample": {"group": "fd_main"},
            "outcome": "d_sheltered",
            "predictors": ["d_beds"],
            "controls": ["d_population", "d_unemployment"],
            "fixed_effects": ["year"],
            "inference": {"method": "clustered", "cluster_by": "geography"},
        },
        {
            "id": "total_fd",
            "method": "regression",
            "input": "panels.cbsa",
            "sample": {"group": "fd_main"},
            "outcome": "d_total",
            "predictors": ["d_beds"],
            "controls": ["d_population", "d_unemployment"],
            "fixed_effects": ["year"],
            "inference": {"method": "clustered", "cluster_by": "geography"},
        },
    ],
    "variants": [
        {
            "id": "odd_years_fe",
            "applies_to": ["subst_fe"],
            "override": {"sample.years": {"from": 2011, "to": 2023, "step": 2}},
        },
        {
            "id": "odd_years_fd",
            "applies_to": ["subst_fd", "sheltered_fd", "total_fd"],
            "override": {"sample.years": {"from": 2013, "to": 2023, "step": 2}},
            "substitute": {
                "d_beds": "d2_beds",
                "d_sheltered": "d2_sheltered",
                "d_unsheltered": "d2_unsheltered",
                "d_total": "d2_total",
                "d_population": "d2_population",
                "d_unemployment": "d2_unemployment",
            },
        },
        {
            "id": "no_right_to_shelter",
            "applies_to": ["levels_fit", "subst_fe", "subst_fd", "sheltered_fd", "total_fd"],
            "override": {
                "sample.geographies": {"exclude": {"column": "right_to_shelter", "equals": 1}}
            },
        },
        {
            "id": "coc_geography",
            "applies_to": ["levels_fit", "subst_fe", "subst_fd", "sheltered_fd", "total_fd"],
            "override": {"input": "panels.coc"},
        },
        {
            "id": "pop_weighted",
            "applies_to": ["subst_fe", "subst_fd", "sheltered_fd", "total_fd"],
            "override": {"weights": "population"},
        },
        {
            "id": "lagged_beds",
            "applies_to": ["subst_fd", "sheltered_fd", "total_fd"],
            "substitute": {"d_beds": "lag_d_beds"},
        },
        {
            "id": "hc1",
            "applies_to": ["subst_fd"],
            "override": {"inference": {"method": "hc1"}},
        },
    ],
    "diagnostics": [
        {
            "id": "fd_additivity",
            "op": "linear_identity",
            "terms": [
                {"analysis": "sheltered_fd", "term": "d_beds", "sign": 1},
                {"analysis": "subst_fd", "term": "d_beds", "sign": 1},
                {"analysis": "total_fd", "term": "d_beds", "sign": -1},
            ],
            "variants": "all",
            "require_same_design": True,
        },
        {
            "id": "pit_components_cbsa",
            "op": "column_identity",
            "panel": "cbsa",
            "lhs": "pit_total",
            "rhs": ["pit_sheltered", "pit_unsheltered"],
            "rows": "unmasked",
        },
        {
            "id": "fd_components",
            "op": "column_identity",
            "panel": "cbsa",
            "lhs": "d_total",
            "rhs": ["d_sheltered", "d_unsheltered"],
            "rows": {"group": "fd_main"},
        },
    ],
    "findings": [],
    "results": {
        "bundle_dir": "results/{id}/{run_id}",
        "include_panels": ["cbsa", "coc"],
        "report": {"title": "Synthetic shelter-capacity acceptance fixture"},
    },
}


def _panel_frame(*, inconsistent_total: bool = False) -> pd.DataFrame:
    records = []
    for geo_index, geo_id in enumerate(GEOGRAPHIES):
        for year in YEARS:
            elapsed = year - BASELINE_YEAR
            beds = (
                100.0
                + 5 * geo_index
                + (geo_index + 1) * elapsed
                + 0.13 * elapsed**2
                + 0.05 * geo_index**2 * elapsed**2
            )
            sheltered = 1.2 * beds + 3 * geo_index + 2 * elapsed + 0.3 * geo_index * elapsed
            unsheltered = (
                500.0 - 0.7 * beds + 8 * geo_index + 4 * elapsed + 0.2 * geo_index * elapsed
            )
            total = sheltered + unsheltered + (1 if inconsistent_total else 0)
            records.append(
                {
                    "geo_id": geo_id,
                    "year": year,
                    "pit_sheltered": sheltered,
                    "pit_unsheltered": unsheltered,
                    "pit_total": total,
                    "hic_es_year_round_beds": 0.5 * beds,
                    "hic_th_year_round_beds": 0.3 * beds,
                    "hic_sh_year_round_beds": 0.2 * beds,
                    "population": (
                        10000
                        + 400 * geo_index
                        + 60 * elapsed
                        + 25 * (geo_index + 1) * math.sin(0.2 * elapsed)
                        + 3 * geo_index**2 * elapsed
                    ),
                    "unemployment_rate": (
                        4
                        + 0.1 * geo_index
                        + 0.03 * elapsed
                        + 0.2 * (geo_index + 1) * math.sin(0.6 * elapsed)
                        + 0.08 * geo_index**2 * math.cos(0.3 * elapsed)
                    ),
                    "median_gross_rent": (
                        800
                        + 25 * geo_index
                        + 10 * elapsed
                        + 13 * (geo_index + 1) * math.cos(0.3 * elapsed)
                        + 3 * geo_index**2 * math.sin(0.5 * elapsed)
                    ),
                    "right_to_shelter": int(geo_id == RIGHT_TO_SHELTER_GEOGRAPHY),
                }
            )
    frame = pd.DataFrame.from_records(records)
    frame.attrs["measures"] = {
        column: {
            "units": units,
            "aggregation_semantics": semantics,
            "source_lineage": [column],
        }
        for column, units, semantics in (
            ("pit_sheltered", "persons", "extensive"),
            ("pit_unsheltered", "persons", "extensive"),
            ("pit_total", "persons", "extensive"),
            ("hic_es_year_round_beds", "beds", "extensive"),
            ("hic_th_year_round_beds", "beds", "extensive"),
            ("hic_sh_year_round_beds", "beds", "extensive"),
            ("population", "persons", "extensive"),
            ("unemployment_rate", "percent", "intensive"),
        )
    }
    return frame


def test_real_example_has_supported_build_refs_and_keeps_research_inputs_unresolved():
    example_path = Path(__file__).parents[1] / "recipes/examples/shelter-capacity-results.yaml"
    example = load_recipe(example_path)

    unresolved = {path for path, _description in example.unresolved_values()}
    assert unresolved == {
        "recipe.parameters.delta_theta",
        "recipe.parameters.delta_slope",
        "recipe.parameters.fit_threshold",
        "recipe.parameters.fit_floor",
        "recipe.parameters.right_to_shelter_source",
        "recipe.parameters.coc_unemployment_source",
    }
    assert {pipeline.id for pipeline in example.pipelines} == {
        "build_cbsa_panel",
        "build_coc_panel",
    }
    assert example.panels["cbsa"].ref.pipeline == "build_cbsa_panel"
    assert example.panels["coc"].ref.pipeline == "build_coc_panel"
    assert "laus_metro" in example.datasets
    assert example.datasets["laus_metro"].file_set.segments[0].years.range == "2015-2023"
    bed_inputs = next(item.of for item in example.derive if item.id == "beds_shelter")
    assert bed_inputs == [
        "hic_es_year_round_beds",
        "hic_th_year_round_beds",
        "hic_sh_year_round_beds",
    ]
    assert "coc_geography" in {variant.id for variant in example.variants}


@pytest.mark.parametrize("panel_year", [2020, 2021], ids=["acs-2019", "acs-2020"])
def test_real_example_tract_vintage_uses_lagged_acs_year(panel_year):
    example = load_recipe(
        Path(__file__).parents[1] / "recipes/examples/shelter-capacity-results.yaml"
    )
    segment = next(
        segment
        for segment in example.datasets["acs_tract"].file_set.segments
        if int(segment.years.range.split("-")[0])
        <= panel_year
        <= int(segment.years.range.split("-")[1])
    )
    data_year = panel_year - 1
    assert segment.year_offsets["acs_end"] == -1
    assert segment.geometry.vintage == data_year // 10 * 10


@pytest.mark.parametrize("command", ["lock", "run"])
def test_real_example_source_choices_block_lock_and_run_after_numeric_parameters_are_filled(
    tmp_path,
    command,
):
    example_path = Path(__file__).parents[1] / "recipes/examples/shelter-capacity-results.yaml"
    recipe_data = yaml.safe_load(example_path.read_text(encoding="utf-8"))
    recipe_data["parameters"].update(SYNTHETIC_PARAMETERS)
    recipe_path = tmp_path / "shelter-capacity-with-unresolved-sources.yaml"
    recipe_path.write_text(yaml.safe_dump(recipe_data, sort_keys=False), encoding="utf-8")

    expected_source_paths = {
        "recipe.parameters.right_to_shelter_source",
        "recipe.parameters.coc_unemployment_source",
    }
    result = RUNNER.invoke(
        app,
        [
            "results",
            command,
            "--recipe",
            str(recipe_path),
            "--outputs-root",
            str(tmp_path / "outputs"),
            "--json",
            "--non-interactive",
        ],
    )
    assert result.exit_code == 2, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    messages = [payload["error"], *(finding["message"] for finding in payload["findings"])]
    assert all(any(path in message for message in messages) for path in expected_source_paths)


def _recipe_file(tmp_path: Path) -> Path:
    path = tmp_path / "shelter-capacity.yaml"
    path.write_text(yaml.safe_dump(SYNTHETIC_RECIPE, sort_keys=False), encoding="utf-8")
    return path


def _cli_json(args: list[str]) -> dict:
    result = RUNNER.invoke(app, [*args, "--json", "--non-interactive"])
    assert result.exit_code == 0, result.output
    return json.loads(result.output)


def test_synthetic_replay_accounts_for_every_analysis_variant_and_diagnostic_truth_table():
    recipe = load_recipe(SYNTHETIC_RECIPE)
    real_example = load_recipe(
        Path(__file__).parents[1] / "recipes/examples/shelter-capacity-results.yaml"
    )
    assert {variant.id for variant in recipe.variants} == {
        variant.id for variant in real_example.variants
    }
    assert {(variant.id, tuple(variant.applies_to)) for variant in recipe.variants} == {
        (variant.id, tuple(variant.applies_to)) for variant in real_example.variants
    }
    panel = _panel_frame()
    resolved_plan = plan_results(recipe, original_recipe_bytes=b"synthetic test bytes")
    replay = replay_results(
        {
            "recipe": recipe.model_dump(mode="json", by_alias=True, exclude_none=True),
            "plan": resolved_plan.to_dict(),
        },
        {"spec_hash": resolved_plan.plan_sha256},
        {"cbsa": panel, "coc": panel.copy()},
    )

    expected_keys = {
        f"{analysis['id']}@{variant}"
        for analysis in SYNTHETIC_RECIPE["analyses"]
        for variant in (
            "base",
            *(
                variant["id"]
                for variant in SYNTHETIC_RECIPE["variants"]
                if analysis["id"] in variant["applies_to"]
            ),
        )
    }
    assert set(replay["analyses"]) == expected_keys
    assert all(result["status"] == "complete" for result in replay["analyses"].values())

    diagnostics = {
        (record["id"], record["variant"]): record for record in replay["diagnostics"]["diagnostics"]
    }
    expected_additivity = {
        "base": "pass",
        "odd_years_fe": "not_applicable",
        "odd_years_fd": "pass",
        "no_right_to_shelter": "pass",
        "coc_geography": "pass",
        "pop_weighted": "pass",
        "lagged_beds": "pass",
        "hc1": "not_applicable",
    }
    assert {
        variant: diagnostics[("fd_additivity", variant)]["status"]
        for variant in expected_additivity
    } == expected_additivity
    assert diagnostics[("pit_components_cbsa", "base")]["status"] == "pass"
    assert diagnostics[("fd_components", "base")]["status"] == "pass"

    odd_fe_membership = replay["samples"]["subst_fe@odd_years_fe"]
    actual_odd_fe_truth = {
        year: ("included" if included else reason)
        for year, included, reason in (
            odd_fe_membership[["year", "included", "reason"]]
            .drop_duplicates()
            .itertuples(index=False, name=None)
        )
    }
    assert actual_odd_fe_truth == ODD_YEARS_FE_TRUTH

    base_membership = replay["samples"]["subst_fd@base"]
    actual_annual_truth = {
        year: ("included" if included else reason)
        for year, included, reason in (
            base_membership[["year", "included", "reason"]]
            .drop_duplicates()
            .itertuples(index=False, name=None)
        )
    }
    assert actual_annual_truth == ANNUAL_FD_TRUTH
    odd_membership = replay["samples"]["subst_fd@odd_years_fd"]
    actual_biennial_truth = {
        year: ("included" if included else reason)
        for year, included, reason in (
            odd_membership[["year", "included", "reason"]]
            .drop_duplicates()
            .itertuples(index=False, name=None)
        )
    }
    assert actual_biennial_truth == BIENNIAL_FD_TRUTH
    assert "subst_fd@hc1" in replay["analyses"]
    assert replay["analyses"]["subst_fd@hc1"]["sample_source"] == "fd_main@base"


def test_cli_lock_run_and_moved_bundle_verification_use_real_replay(tmp_path, monkeypatch):
    recipe_path = _recipe_file(tmp_path)
    frame = _panel_frame()
    monkeypatch.setattr(
        "hhplab.results.runner._load_run_panels",
        lambda *_args, **_kwargs: ({"cbsa": frame, "coc": frame.copy()}, {}),
    )

    locked = _cli_json(
        ["results", "lock", "--recipe", str(recipe_path), "--outputs-root", str(tmp_path)]
    )
    assert locked["status"] == "ok"
    run = _cli_json(
        [
            "results",
            "run",
            "--recipe",
            str(recipe_path),
            "--outputs-root",
            str(tmp_path),
            "--private",
        ]
    )
    assert run["statuses"]["execution_status"] == "complete"
    bundle = Path(run["bundle_path"])
    assert (bundle / "manifest.json").is_file()
    assert [event["event"] for event in RunLedger(tmp_path).read()["events"]] == [
        "lock_created",
        "run_started",
        "run_finished",
    ]

    moved_bundle = tmp_path / "moved-bundle"
    move(str(bundle), str(moved_bundle))
    verified = _cli_json(["results", "verify", str(moved_bundle)])
    assert verified["integrity"] == "pass"
    assert verified["reproducibility"] == "pass"
    assert verified["exit_code"] == 0


def test_inconsistent_pit_total_is_a_completed_run_with_failed_diagnostic(tmp_path):
    recipe = load_recipe(SYNTHETIC_RECIPE)
    panel = _panel_frame(inconsistent_total=True)
    source = yaml.safe_dump(SYNTHETIC_RECIPE, sort_keys=False).encode()
    from hhplab.results.runner import run_results

    result = run_results(
        recipe,
        original_recipe_bytes=source,
        outputs_root=tmp_path,
        panel_frames={"cbsa": panel, "coc": panel.copy()},
        private=True,
    )
    assert result["statuses"]["execution_status"] == "complete"
    assert result["statuses"]["diagnostics_status"] == "fail"
    diagnostics_path = Path(result["bundle_path"]) / "diagnostics.json"
    records = json.loads(diagnostics_path.read_bytes())["diagnostics"]
    by_id = {record["id"]: record for record in records if record["variant"] == "base"}
    assert by_id["pit_components_cbsa"]["status"] == "fail"
