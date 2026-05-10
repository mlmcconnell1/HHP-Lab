"""Tests for the recipe preflight system (probes, analyzer, CLI, gaps manifest)."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from hhplab.cli.main import app
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.recipe.loader import load_recipe
from hhplab.recipe.preflight import (
    FindingKind,
    PreflightFinding,
    PreflightReport,
    Severity,
    run_preflight,
)
from hhplab.recipe.probes import (
    probe_dataset_schema,
    probe_geo_column,
    probe_measures,
    probe_static_broadcast,
    probe_temporal_filter,
    probe_year_column,
)
from hhplab.recipe.recipe_schema import DatasetSpec, GeometryRef, TemporalFilter

runner = CliRunner()

STALE_TRANSLATED_ACS_PATH = "data/curated/acs/acs5_tracts__A2019xT2020.parquet"
STALE_TRANSLATED_ACS_VINTAGE = "2015-2019"
STALE_TRANSLATED_ACS_REBUILD = (
    "hhplab ingest acs5-tract --acs 2015-2019 --tracts 2020 --force"
)


def _write_stale_translated_acs_cache(path: Path) -> None:
    """Write a pre-fix translated ACS cache lacking translation provenance."""
    write_parquet_with_provenance(
        pd.DataFrame({
            "tract_geoid": ["T1"],
            "year": [2020],
            "acs_vintage": [STALE_TRANSLATED_ACS_VINTAGE],
            "tract_vintage": ["2020"],
            "total_population": [100],
        }),
        path,
        ProvenanceBlock(
            acs_vintage=STALE_TRANSLATED_ACS_VINTAGE,
            tract_vintage="2020",
            extra={"dataset": "acs5_tract_data"},
        ),
    )


def _sae_recipe_dict(year: int = 2023) -> dict:
    return {
        "version": 1,
        "name": "sae-preflight",
        "universe": {"years": [year]},
        "targets": [
            {
                "id": "coc_panel",
                "geometry": {"type": "coc", "vintage": 2025},
            }
        ],
        "datasets": {
            "acs1_county": {
                "provider": "census",
                "product": "acs1",
                "version": 1,
                "native_geometry": {"type": "county", "vintage": 2020, "source": "tiger"},
                "path": f"data/curated/acs/acs1_county_sae__A{year}.parquet",
            },
            "acs5_support": {
                "provider": "census",
                "product": "acs5",
                "version": 1,
                "native_geometry": {"type": "tract", "vintage": 2020, "source": "tiger"},
                "path": "data/curated/acs/acs5_tract_sae_support__A2022xT2020.parquet",
            },
        },
        "pipelines": [
            {
                "id": "main",
                "target": "coc_panel",
                "steps": [
                    {
                        "kind": "small_area_estimate",
                        "output_dataset": "acs_sae_coc",
                        "source_dataset": "acs1_county",
                        "support_dataset": "acs5_support",
                        "source_geometry": {
                            "type": "county",
                            "vintage": 2020,
                            "source": "tiger",
                        },
                        "support_geometry": {
                            "type": "tract",
                            "vintage": 2020,
                            "source": "tiger",
                        },
                        "target_geometry": {"type": "coc", "vintage": 2025},
                        "terminal_acs5_vintage": 2022,
                        "tract_vintage": 2020,
                        "denominators": {"labor_force": "civilian_labor_force"},
                        "measures": {
                            "labor_force": {
                                "outputs": ["sae_unemployment_rate"],
                            },
                        },
                    },
                    {"kind": "join", "datasets": ["acs_sae_coc"]},
                ],
            }
        ],
    }


def _write_sae_preflight_fixtures(root: Path, *, year: int = 2023) -> None:
    acs_dir = root / "data" / "curated" / "acs"
    acs_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "county_fips": ["08031"],
            "acs1_vintage": [year],
            "civilian_labor_force": [1000],
            "unemployed_count": [50],
        }
    ).to_parquet(acs_dir / f"acs1_county_sae__A{year}.parquet")
    pd.DataFrame(
        {
            "tract_geoid": ["08031000100"],
            "county_fips": ["08031"],
            "acs_vintage": ["2022"],
            "tract_vintage": ["2020"],
            "civilian_labor_force": [600],
            "unemployed_count": [30],
        }
    ).to_parquet(acs_dir / "acs5_tract_sae_support__A2022xT2020.parquet")
    xwalk_dir = root / "data" / "curated" / "xwalks"
    xwalk_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "coc_id": ["COC-A"],
            "tract_geoid": ["08031000100"],
            "area_share": [1.0],
        }
    ).to_parquet(xwalk_dir / "xwalk__B2025xT2020.parquet")


def _write_cli_project_markers(root: Path) -> None:
    (root / "pyproject.toml").touch()
    (root / "hhplab").mkdir(exist_ok=True)
    (root / "data").mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# Probe unit tests
# ---------------------------------------------------------------------------

class TestProbeYearColumn:

    def test_declared_present(self):
        r = probe_year_column(["year", "geo_id", "pop"], "year")
        assert r.ok
        assert r.detail["year_column"] == "year"

    def test_declared_missing(self):
        r = probe_year_column(["geo_id", "pop"], "year")
        assert not r.ok
        assert "year" in r.message

    def test_auto_detect_single(self):
        r = probe_year_column(["geo_id", "year", "pop"], None)
        assert r.ok
        assert r.detail["year_column"] == "year"

    def test_auto_detect_none(self):
        r = probe_year_column(["geo_id", "pop"], None)
        assert r.ok
        assert r.detail["year_column"] is None

    def test_ambiguous(self):
        r = probe_year_column(["year", "pit_year", "pop"], None)
        assert not r.ok
        assert "Ambiguous" in r.message


class TestProbeGeoColumn:

    def test_declared_present(self):
        r = probe_geo_column(["coc_id", "year", "pop"], "coc_id")
        assert r.ok
        assert r.detail["geo_column"] == "coc_id"

    def test_declared_missing(self):
        r = probe_geo_column(["year", "pop"], "coc_id")
        assert not r.ok

    def test_auto_detect_single(self):
        r = probe_geo_column(["coc_id", "year", "pop"], None)
        assert r.ok
        assert r.detail["geo_column"] == "coc_id"

    def test_no_candidates(self):
        r = probe_geo_column(["foo", "bar"], None)
        assert not r.ok
        assert "Cannot find" in r.message

    def test_ambiguous(self):
        r = probe_geo_column(["geo_id", "coc_id", "pop"], None)
        assert not r.ok
        assert "Ambiguous" in r.message


class TestProbeMeasures:

    def test_all_present(self):
        r = probe_measures(["geo_id", "pop", "income"], ["pop", "income"], "ds1")
        assert r.ok

    def test_some_missing(self):
        r = probe_measures(["geo_id", "pop"], ["pop", "income"], "ds1")
        assert not r.ok
        assert "income" in r.message
        assert r.detail["missing_measures"] == ["income"]


class TestProbeTemporalFilter:

    def test_column_present(self):
        filt = TemporalFilter(column="date", method="point_in_time", month=1)
        r = probe_temporal_filter(["date", "year", "pop"], filt, "ds1")
        assert r.ok

    def test_column_missing(self):
        filt = TemporalFilter(column="date", method="point_in_time", month=1)
        r = probe_temporal_filter(["year", "pop"], filt, "ds1")
        assert not r.ok
        assert "date" in r.message

    def test_interpolate_requires_year_column(self):
        filt = TemporalFilter(
            column="reference_date",
            method="interpolate_to_month",
            month=1,
        )
        r = probe_temporal_filter(
            ["reference_date", "population"],
            filt,
            "pep",
        )
        assert not r.ok
        assert "requires a year column" in r.message

    def test_interpolate_requires_datetime_column_type(self):
        filt = TemporalFilter(
            column="reference_date",
            method="interpolate_to_month",
            month=1,
        )
        r = probe_temporal_filter(
            ["reference_date", "year", "population"],
            filt,
            "pep",
            year_column="year",
            column_types={"reference_date": "int64"},
        )
        assert not r.ok
        assert "requires a datetime column" in r.message


class TestProbeStaticBroadcast:

    def _make_ds(self, **overrides) -> DatasetSpec:
        defaults = {
            "provider": "test",
            "product": "test",
            "version": 1,
            "native_geometry": GeometryRef(type="coc"),
        }
        defaults.update(overrides)
        return DatasetSpec(**defaults)

    def test_with_year_column(self):
        ds = self._make_ds()
        r = probe_static_broadcast(ds, "ds1", True, 3)
        assert r.ok

    def test_single_year_universe(self):
        ds = self._make_ds()
        r = probe_static_broadcast(ds, "ds1", False, 1)
        assert r.ok

    def test_broadcast_static_opt_in(self):
        ds = self._make_ds(params={"broadcast_static": True})
        r = probe_static_broadcast(ds, "ds1", False, 3)
        assert r.ok

    def test_distinct_paths_safe(self):
        ds = self._make_ds()
        r = probe_static_broadcast(ds, "ds1", False, 3, distinct_paths=3)
        assert r.ok

    def test_implicit_broadcast_blocked(self):
        ds = self._make_ds()
        r = probe_static_broadcast(ds, "ds1", False, 3)
        assert not r.ok
        assert "broadcast" in r.message


class TestProbeDatasetSchema:

    def test_valid_parquet(self, tmp_path: Path):
        path = tmp_path / "test.parquet"
        pd.DataFrame({"geo_id": ["A"], "year": [2020]}).to_parquet(path)
        r = probe_dataset_schema(path)
        assert r.ok
        assert "geo_id" in r.detail["columns"]

    def test_missing_file(self, tmp_path: Path):
        r = probe_dataset_schema(tmp_path / "nope.parquet")
        assert not r.ok
        assert "not found" in r.message


# ---------------------------------------------------------------------------
# Preflight analyzer tests
# ---------------------------------------------------------------------------

def _preflight_recipe(
    *,
    with_path: bool = False,
    with_file_set: bool = False,
    identity_only: bool = False,
) -> dict:
    """Return a recipe dict for preflight testing."""
    base: dict = {
        "version": 1,
        "name": "preflight-test",
        "universe": {"range": "2020-2022"},
        "targets": [
            {"id": "coc_panel", "geometry": {"type": "coc", "vintage": 2025}},
        ],
        "datasets": {
            "pit": {
                "provider": "hud",
                "product": "pit",
                "version": 1,
                "native_geometry": {"type": "coc"},
                "years": "2020-2022",
            },
        },
        "transforms": [],
        "pipelines": [
            {
                "id": "main",
                "target": "coc_panel",
                "steps": [
                    {
                        "resample": {
                            "dataset": "pit",
                            "to_geometry": {"type": "coc", "vintage": 2025},
                            "method": "identity",
                            "measures": ["pit_total"],
                        },
                    },
                    {
                        "join": {
                            "datasets": ["pit"],
                            "join_on": ["geo_id", "year"],
                        },
                    },
                ],
            },
        ],
    }

    if with_path:
        base["datasets"]["pit"]["path"] = "data/pit.parquet"

    if not identity_only:
        base["datasets"]["acs"] = {
            "provider": "census",
            "product": "acs5",
            "version": 1,
            "native_geometry": {"type": "tract", "vintage": 2020},
            "years": "2020-2022",
        }
        base["transforms"] = [
            {
                "id": "tract_to_coc",
                "type": "crosswalk",
                "from": {"type": "tract", "vintage": 2020},
                "to": {"type": "coc", "vintage": 2025},
                "spec": {"weighting": {"scheme": "area"}},
            },
        ]
        base["pipelines"][0]["steps"].insert(0, {
            "materialize": {"transforms": ["tract_to_coc"]},
        })
        base["pipelines"][0]["steps"].insert(2, {
            "resample": {
                "dataset": "acs",
                "to_geometry": {"type": "coc", "vintage": 2025},
                "method": "aggregate",
                "via": "tract_to_coc",
                "measures": ["total_population"],
            },
        })
        base["pipelines"][0]["steps"][-1]["join"]["datasets"] = ["pit", "acs"]

    return base


def _setup_preflight_fixtures(
    tmp_path: Path,
    *,
    include_xwalk: bool = True,
    include_pit: bool = True,
    include_acs: bool = True,
) -> None:
    """Set up dataset and transform files for preflight tests."""
    if include_xwalk:
        xwalk_dir = tmp_path / "data" / "curated" / "xwalks"
        xwalk_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({
            "coc_id": ["COC1", "COC2"],
            "tract_geoid": ["T1", "T2"],
            "area_share": [1.0, 1.0],
        }).to_parquet(xwalk_dir / "xwalk__B2025xT2020.parquet")

    if include_pit:
        pit_path = tmp_path / "data" / "pit.parquet"
        pit_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({
            "coc_id": ["COC1", "COC2", "COC1", "COC2", "COC1", "COC2"],
            "year": [2020, 2020, 2021, 2021, 2022, 2022],
            "pit_total": [10, 20, 11, 21, 12, 22],
        }).to_parquet(pit_path)

    if include_acs:
        acs_path = tmp_path / "data" / "acs.parquet"
        (tmp_path / "data").mkdir(parents=True, exist_ok=True)
        pd.DataFrame({
            "GEOID": ["T1", "T2", "T1", "T2", "T1", "T2"],
            "year": [2020, 2020, 2021, 2021, 2022, 2022],
            "total_population": [100, 200, 110, 210, 120, 220],
        }).to_parquet(acs_path)


def _msa_preflight_recipe() -> dict:
    """Return a recipe dict with a CoC-to-MSA transform."""
    return {
        "version": 1,
        "name": "msa-preflight-test",
        "universe": {"years": [2020]},
        "targets": [
            {"id": "msa_panel", "geometry": {"type": "msa", "source": "census_msa_2023"}},
        ],
        "datasets": {
            "pit": {
                "provider": "hud",
                "product": "pit",
                "version": 1,
                "native_geometry": {"type": "coc"},
                "years": {"years": [2020]},
                "path": "data/pit.parquet",
            },
        },
        "transforms": [
            {
                "id": "coc_to_msa",
                "type": "crosswalk",
                "from": {"type": "coc", "vintage": 2020},
                "to": {"type": "msa", "source": "census_msa_2023"},
                "spec": {"weighting": {"scheme": "area"}},
            },
        ],
        "pipelines": [
            {
                "id": "main",
                "target": "msa_panel",
                "steps": [
                    {"materialize": {"transforms": ["coc_to_msa"]}},
                    {
                        "resample": {
                            "dataset": "pit",
                            "to_geometry": {"type": "msa", "source": "census_msa_2023"},
                            "method": "aggregate",
                            "via": "coc_to_msa",
                            "measures": ["pit_total"],
                        },
                    },
                    {
                        "join": {
                            "datasets": ["pit"],
                            "join_on": ["geo_id", "year"],
                        },
                    },
                ],
            },
        ],
    }


def _map_preflight_recipe(*, geometry: dict[str, object]) -> dict:
    """Return a recipe dict with a recipe-native map target."""
    base = _preflight_recipe(with_path=True, identity_only=True)
    base["targets"][0]["outputs"] = ["map"]
    base["targets"][0]["map_spec"] = {
        "layers": [
            {
                "geometry": geometry,
                "selector_ids": ["COC1" if geometry["type"] == "coc" else "19740"],
                "label": "Primary layer",
            }
        ]
    }
    return base


def _containment_preflight_recipe(pair: tuple[str, str]) -> dict:
    """Return a recipe dict with one recipe-native containment target."""
    container, candidate = pair
    base = _preflight_recipe(with_path=True, identity_only=True)
    target_geometry = (
        {"type": "msa", "source": "census_msa_2023"}
        if container == "msa"
        else {"type": container, "vintage": 2023}
    )
    base["targets"][0] = {
        "id": f"{container}_{candidate}_containment",
        "geometry": target_geometry,
        "outputs": ["containment"],
        "containment_spec": {
            "container": {"type": container, "vintage": 2023},
            "candidate": {"type": candidate, "vintage": 2025 if candidate == "coc" else 2023},
            "selector_ids": ["19740" if container == "msa" else "COC1"],
            "candidate_selector_ids": ["COC1" if candidate == "coc" else "001"],
        },
    }
    if container == "msa":
        base["targets"][0]["containment_spec"]["container"]["source"] = "census_msa_2023"
    base["pipelines"][0]["target"] = base["targets"][0]["id"]
    return base


def _setup_containment_artifacts(
    tmp_path: Path,
    pair: tuple[str, str],
    *,
    include_coc: bool = True,
    include_county: bool = True,
    include_msa: bool = True,
) -> None:
    """Set up minimal containment artifact tables for preflight tests."""
    curated = tmp_path / "data" / "curated"
    if include_coc:
        coc_dir = curated / "coc_boundaries"
        coc_dir.mkdir(parents=True, exist_ok=True)
        coc_vintage = 2025 if pair == ("msa", "coc") else 2023
        pd.DataFrame({"coc_id": ["COC1"]}).to_parquet(coc_dir / f"coc__B{coc_vintage}.parquet")
    if include_county:
        tiger_dir = curated / "tiger"
        tiger_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"GEOID": ["001"]}).to_parquet(tiger_dir / "counties__C2023.parquet")
    if include_msa and pair == ("msa", "coc"):
        msa_dir = curated / "msa"
        msa_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"msa_id": ["19740"], "name": ["Denver-Aurora-Centennial, CO"]}).to_parquet(
            msa_dir / "msa_definitions__census_msa_2023.parquet"
        )
        pd.DataFrame({"msa_id": ["19740"], "county_fips": ["001"]}).to_parquet(
            msa_dir / "msa_county_membership__census_msa_2023.parquet"
        )


class TestRunPreflight:

    def test_clean_preflight(self, tmp_path: Path):
        data = _preflight_recipe(with_path=True, identity_only=True)
        _setup_preflight_fixtures(tmp_path, include_xwalk=False, include_acs=False)
        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)
        assert report.is_ready
        assert report.blocking_count == 0
        assert len(report.pipelines) == 1
        assert report.pipelines[0].task_count > 0

    def test_missing_transform_artifact(self, tmp_path: Path):
        data = _preflight_recipe(with_path=True)
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        _setup_preflight_fixtures(tmp_path, include_xwalk=False)
        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)
        assert not report.is_ready
        transform_findings = [
            f for f in report.findings
            if f.kind == FindingKind.MISSING_TRANSFORM
        ]
        assert len(transform_findings) >= 1
        assert transform_findings[0].transform_id == "tract_to_coc"
        assert transform_findings[0].remediation is not None

    def test_transform_set_missing_artifacts_are_reported_per_segment(self, tmp_path: Path):
        data = _preflight_recipe(with_path=True, identity_only=True)
        data["universe"] = {"range": "2019-2020"}
        data["datasets"]["pit"]["years"] = "2019-2020"
        data["datasets"]["acs"] = {
            "provider": "census",
            "product": "acs5",
            "version": 1,
            "native_geometry": {"type": "tract"},
            "file_set": {
                "path_template": "data/acs_{year}.parquet",
                "segments": [
                    {
                        "years": {"range": "2010-2019"},
                        "geometry": {"type": "tract", "vintage": 2010},
                    },
                    {
                        "years": {"range": "2020-2024"},
                        "geometry": {"type": "tract", "vintage": 2020},
                    },
                ],
            },
        }
        data["transforms"] = [
            {
                "id": "tract2010_to_coc",
                "type": "crosswalk",
                "from": {"type": "tract", "vintage": 2010},
                "to": {"type": "coc", "vintage": 2025},
                "spec": {"weighting": {"scheme": "area"}},
            },
            {
                "id": "tract2020_to_coc",
                "type": "crosswalk",
                "from": {"type": "tract", "vintage": 2020},
                "to": {"type": "coc", "vintage": 2025},
                "spec": {"weighting": {"scheme": "area"}},
            },
        ]
        data["pipelines"][0]["steps"].insert(
            1,
            {
                "resample": {
                    "dataset": "acs",
                    "to_geometry": {"type": "coc", "vintage": 2025},
                    "method": "aggregate",
                    "transform_set": {
                        "segments": [
                            {"years": {"range": "2010-2019"}, "via": "tract2010_to_coc"},
                            {"years": {"range": "2020-2024"}, "via": "tract2020_to_coc"},
                        ]
                    },
                    "measures": ["total_population"],
                },
            },
        )
        data["pipelines"][0]["steps"][-1]["join"]["datasets"] = ["pit", "acs"]
        _setup_preflight_fixtures(tmp_path, include_xwalk=False, include_acs=False)
        for year in (2019, 2020):
            pd.DataFrame(
                {
                    "GEOID": ["T1"],
                    "year": [year],
                    "total_population": [100 + year],
                }
            ).to_parquet(tmp_path / "data" / f"acs_{year}.parquet")

        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)

        transform_findings = [
            finding.transform_id
            for finding in report.findings
            if finding.kind == FindingKind.MISSING_TRANSFORM
        ]
        assert set(transform_findings) == {"tract2010_to_coc", "tract2020_to_coc"}

    def test_pep_decennial_tract_mediated_requires_baseline_year(self, tmp_path: Path):
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True)
        pd.DataFrame(
            {
                "county_fips": ["001"],
                "year": [2024],
                "population": [100],
            }
        ).to_parquet(data_dir / "pep.parquet")

        data = _preflight_recipe(with_path=True, identity_only=True)
        data["universe"] = {"years": [2024]}
        data["datasets"]["pit"]["years"] = "2024-2024"
        data["datasets"]["pep"] = {
            "provider": "census",
            "product": "pep",
            "version": 1,
            "native_geometry": {"type": "county", "vintage": 2020},
            "years": "2020-2024",
            "path": "data/pep.parquet",
            "geo_column": "county_fips",
        }
        data["transforms"] = [
            {
                "id": "county_to_coc_tract_mediated",
                "type": "crosswalk",
                "from": {"type": "county", "vintage": 2020},
                "to": {"type": "coc", "vintage": 2025},
                "spec": {
                    "weighting": {
                        "scheme": "tract_mediated",
                        "variety": "population",
                        "tract_vintage": 2020,
                        "denominator_source": "decennial",
                        "denominator_vintage": 2020,
                    }
                },
            }
        ]
        data["pipelines"][0]["steps"].insert(
            1,
            {
                "resample": {
                    "dataset": "pep",
                    "to_geometry": {"type": "coc", "vintage": 2025},
                    "method": "aggregate",
                    "via": "county_to_coc_tract_mediated",
                    "measures": ["population"],
                }
            },
        )
        data["pipelines"][0]["steps"][-1]["join"]["datasets"] = ["pit", "pep"]
        _setup_preflight_fixtures(tmp_path, include_xwalk=False, include_acs=False)

        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)

        baseline_findings = [
            f for f in report.findings
            if f.kind == FindingKind.MISSING_SUPPORT_DATASET
            and f.transform_id == "county_to_coc_tract_mediated"
        ]
        assert len(baseline_findings) == 1
        assert "baseline-year PEP county estimates for 2020" in baseline_findings[0].message

    def test_msa_transform_missing_membership_is_actionable(self, tmp_path: Path):
        data = _msa_preflight_recipe()
        _setup_preflight_fixtures(tmp_path, include_xwalk=False, include_acs=False)

        curated = tmp_path / "data" / "curated"
        boundaries = curated / "coc_boundaries"
        tiger = curated / "tiger"
        boundaries.mkdir(parents=True, exist_ok=True)
        tiger.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"coc_id": ["COC1"]}).to_parquet(boundaries / "coc__B2020.parquet")
        pd.DataFrame({"GEOID": ["36061"]}).to_parquet(tiger / "counties__C2020.parquet")

        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)
        transform_findings = [
            f for f in report.findings
            if f.kind == FindingKind.MISSING_TRANSFORM and f.transform_id == "coc_to_msa"
        ]
        assert len(transform_findings) == 1
        finding = transform_findings[0]
        assert finding.remediation is not None
        assert "msa county membership artifact" in finding.remediation.hint.lower()
        assert finding.remediation.command == (
            "hhplab generate msa --definition-version census_msa_2023"
        )

    def test_missing_dataset_file(self, tmp_path: Path):
        data = _preflight_recipe(with_path=True, identity_only=True)
        # Don't create the pit file
        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)
        ds_findings = [
            f for f in report.findings
            if f.kind == FindingKind.MISSING_DATASET
        ]
        assert len(ds_findings) >= 1
        assert ds_findings[0].dataset_id == "pit"

    def test_map_target_missing_coc_boundary_is_actionable(self, tmp_path: Path):
        data = _map_preflight_recipe(
            geometry={"type": "coc", "vintage": 2025},
        )
        _setup_preflight_fixtures(tmp_path, include_xwalk=False, include_acs=False)

        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)

        map_findings = [
            f for f in report.findings if f.kind == FindingKind.MISSING_MAP_ARTIFACT
        ]
        assert len(map_findings) == 1
        finding = map_findings[0]
        assert "coc boundary artifact" in finding.message.lower()
        assert finding.remediation is not None
        assert finding.remediation.command == (
            "hhplab ingest boundaries --source hud_exchange --vintage 2025"
        )

    def test_map_target_missing_msa_boundary_is_actionable(self, tmp_path: Path):
        data = _map_preflight_recipe(
            geometry={
                "type": "msa",
                "source": "census_msa_2023",
                "vintage": 2023,
            },
        )
        _setup_preflight_fixtures(tmp_path, include_xwalk=False, include_acs=False)

        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)

        map_findings = [
            f for f in report.findings if f.kind == FindingKind.MISSING_MAP_ARTIFACT
        ]
        assert len(map_findings) == 1
        finding = map_findings[0]
        assert "msa boundary artifact" in finding.message.lower()
        assert finding.remediation is not None
        assert finding.remediation.command == (
            "hhplab ingest msa-boundaries --definition-version census_msa_2023 --year 2023"
        )

    def test_map_target_missing_county_boundary_is_actionable(self, tmp_path: Path):
        data = _map_preflight_recipe(
            geometry={"type": "county", "vintage": 2025},
        )
        _setup_preflight_fixtures(tmp_path, include_xwalk=False, include_acs=False)

        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)

        map_findings = [
            f for f in report.findings if f.kind == FindingKind.MISSING_MAP_ARTIFACT
        ]
        assert len(map_findings) == 1
        finding = map_findings[0]
        assert "county boundary artifact" in finding.message.lower()
        assert finding.remediation is not None
        assert finding.remediation.command == "hhplab ingest tiger --year 2025 --type counties"

    def test_containment_coc_county_ready_when_artifacts_exist(self, tmp_path: Path):
        data = _containment_preflight_recipe(("coc", "county"))
        _setup_preflight_fixtures(tmp_path, include_xwalk=False, include_acs=False)
        _setup_containment_artifacts(tmp_path, ("coc", "county"))

        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)

        assert report.is_ready
        assert [
            f for f in report.findings
            if f.kind
            in {
                FindingKind.MISSING_CONTAINMENT_ARTIFACT,
                FindingKind.CONTAINMENT_SELECTOR,
            }
        ] == []

    def test_containment_msa_coc_ready_when_artifacts_exist(self, tmp_path: Path):
        data = _containment_preflight_recipe(("msa", "coc"))
        _setup_preflight_fixtures(tmp_path, include_xwalk=False, include_acs=False)
        _setup_containment_artifacts(tmp_path, ("msa", "coc"))

        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)

        assert report.is_ready
        assert [
            f for f in report.findings
            if f.kind
            in {
                FindingKind.MISSING_CONTAINMENT_ARTIFACT,
                FindingKind.CONTAINMENT_SELECTOR,
            }
        ] == []

    def test_containment_filter_preflight_validates_artifacts_and_selectors(
        self,
        tmp_path: Path,
    ):
        data = _preflight_recipe(with_path=True, identity_only=True)
        data["targets"][0]["outputs"] = ["panel"]
        data["targets"][0]["containment_filter"] = {
            "container": {"type": "msa", "vintage": 2023, "source": "census_msa_2023"},
            "candidate": {"type": "coc", "vintage": 2025},
            "selector_ids": ["19740"],
            "candidate_selector_ids": ["COC1"],
            "min_share": 0.5,
        }
        _setup_preflight_fixtures(tmp_path, include_xwalk=False, include_acs=False)
        _setup_containment_artifacts(tmp_path, ("msa", "coc"))

        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)

        assert report.is_ready
        assert [
            f for f in report.findings
            if f.kind
            in {
                FindingKind.MISSING_CONTAINMENT_ARTIFACT,
                FindingKind.CONTAINMENT_SELECTOR,
            }
        ] == []

    def test_target_selector_mismatch_is_actionable(self, tmp_path: Path):
        data = _preflight_recipe(with_path=True, identity_only=True)
        data["targets"][0]["selector_ids"] = ["COC2"]
        _setup_preflight_fixtures(tmp_path, include_xwalk=False, include_acs=False)
        _setup_containment_artifacts(tmp_path, ("msa", "coc"))

        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)

        selector_findings = [
            f for f in report.findings
            if f.kind == FindingKind.TARGET_SELECTOR
        ]
        assert len(selector_findings) == 1
        assert "selector_ids did not match available COC IDs: COC2" in (
            selector_findings[0].message
        )
        assert selector_findings[0].remediation is not None
        assert "target.selector_ids" in selector_findings[0].remediation.hint

    def test_containment_coc_county_missing_county_is_actionable(self, tmp_path: Path):
        data = _containment_preflight_recipe(("coc", "county"))
        _setup_preflight_fixtures(tmp_path, include_xwalk=False, include_acs=False)
        _setup_containment_artifacts(
            tmp_path,
            ("coc", "county"),
            include_county=False,
        )

        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)

        containment_findings = [
            f for f in report.findings
            if f.kind == FindingKind.MISSING_CONTAINMENT_ARTIFACT
        ]
        assert len(containment_findings) == 1
        finding = containment_findings[0]
        assert "candidate county geometry artifact" in finding.message
        assert finding.remediation is not None
        assert finding.remediation.command == "hhplab ingest tiger --year 2023 --type counties"

    def test_containment_msa_coc_missing_msa_artifacts_are_actionable(
        self,
        tmp_path: Path,
    ):
        data = _containment_preflight_recipe(("msa", "coc"))
        _setup_preflight_fixtures(tmp_path, include_xwalk=False, include_acs=False)
        _setup_containment_artifacts(
            tmp_path,
            ("msa", "coc"),
            include_msa=False,
        )

        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)

        containment_findings = [
            f for f in report.findings
            if f.kind == FindingKind.MISSING_CONTAINMENT_ARTIFACT
        ]
        assert len(containment_findings) == 2
        assert {f.remediation.command for f in containment_findings if f.remediation} == {
            "hhplab generate msa --definition-version census_msa_2023"
        }
        assert any("MSA definitions artifact" in f.message for f in containment_findings)
        assert any("MSA county membership artifact" in f.message for f in containment_findings)

    def test_containment_selector_mismatch_is_clear(self, tmp_path: Path):
        data = _containment_preflight_recipe(("coc", "county"))
        data["targets"][0]["containment_spec"]["candidate_selector_ids"] = ["999"]
        _setup_preflight_fixtures(tmp_path, include_xwalk=False, include_acs=False)
        _setup_containment_artifacts(tmp_path, ("coc", "county"))

        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)

        selector_findings = [
            f for f in report.findings
            if f.kind == FindingKind.CONTAINMENT_SELECTOR
        ]
        assert len(selector_findings) == 1
        assert "candidate_selector_ids did not match available COUNTY IDs: 999" in (
            selector_findings[0].message
        )

    def test_sae_preflight_ready_when_artifacts_and_crosswalk_exist(self, tmp_path: Path):
        _write_sae_preflight_fixtures(tmp_path)
        recipe = load_recipe(_sae_recipe_dict())

        report = run_preflight(recipe, project_root=tmp_path)

        assert report.is_ready
        assert report.pipelines[0].task_count == 2
        assert report.findings == []

    def test_sae_preflight_reports_missing_source_and_support_artifacts(
        self,
        tmp_path: Path,
    ):
        xwalk_dir = tmp_path / "data" / "curated" / "xwalks"
        xwalk_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"coc_id": ["COC-A"], "tract_geoid": ["08031000100"]}).to_parquet(
            xwalk_dir / "xwalk__B2025xT2020.parquet"
        )
        recipe = load_recipe(_sae_recipe_dict())

        report = run_preflight(recipe, project_root=tmp_path)

        missing = [f for f in report.findings if f.kind == FindingKind.MISSING_DATASET]
        assert {f.dataset_id for f in missing} == {"acs1_county", "acs5_support"}
        assert {f.remediation.command for f in missing if f.remediation} == {
            "hhplab ingest acs1-county --vintage <year>",
            "hhplab ingest acs5-tract --acs <acs-years> --tracts 2020",
        }

    def test_sae_preflight_reports_missing_target_crosswalk(self, tmp_path: Path):
        _write_sae_preflight_fixtures(tmp_path)
        (tmp_path / "data" / "curated" / "xwalks" / "xwalk__B2025xT2020.parquet").unlink()
        recipe = load_recipe(_sae_recipe_dict())

        report = run_preflight(recipe, project_root=tmp_path)

        missing = [f for f in report.findings if f.kind == FindingKind.MISSING_TRANSFORM]
        assert len(missing) == 1
        assert "CoC-to-tract crosswalk" in missing[0].message
        assert missing[0].remediation is not None
        assert missing[0].remediation.command == (
            "hhplab generate xwalks --boundary 2025 --type tract --tracts 2020"
        )

    def test_sae_preflight_reports_invalid_output_family(self, tmp_path: Path):
        _write_sae_preflight_fixtures(tmp_path)
        data = _sae_recipe_dict()
        data["pipelines"][0]["steps"][0]["measures"]["labor_force"]["outputs"] = [
            "sae_household_income_median"
        ]
        recipe = load_recipe(data)

        report = run_preflight(recipe, project_root=tmp_path)

        findings = [
            f
            for f in report.findings
            if f.kind == FindingKind.MISSING_MEASURE
            and "cannot produce outputs" in f.message
        ]
        assert len(findings) == 1
        assert "direct medians" in findings[0].message

    def test_sae_preflight_rejects_rent_burden_outputs_under_gross_rent_bins(
        self,
        tmp_path: Path,
    ):
        _write_sae_preflight_fixtures(tmp_path)
        acs_dir = tmp_path / "data" / "curated" / "acs"
        source = pd.read_parquet(acs_dir / "acs1_county_sae__A2023.parquet")
        source["gross_rent_distribution_total"] = [100]
        source["gross_rent_distribution_cash_rent_3500_plus"] = [10]
        source.to_parquet(acs_dir / "acs1_county_sae__A2023.parquet")
        support = pd.read_parquet(acs_dir / "acs5_tract_sae_support__A2022xT2020.parquet")
        support["gross_rent_distribution_total"] = [100]
        support["gross_rent_distribution_cash_rent_3500_plus"] = [10]
        support.to_parquet(acs_dir / "acs5_tract_sae_support__A2022xT2020.parquet")
        data = _sae_recipe_dict()
        step = data["pipelines"][0]["steps"][0]
        step["denominators"] = {"gross_rent_bins": "gross_rent_distribution_total"}
        step["measures"] = {
            "gross_rent_bins": {
                "outputs": ["sae_gross_rent_pct_income_30_plus"],
            },
        }
        recipe = load_recipe(data)

        report = run_preflight(recipe, project_root=tmp_path)

        findings = [
            f
            for f in report.findings
            if f.kind == FindingKind.MISSING_MEASURE
            and "cannot produce outputs" in f.message
        ]
        assert len(findings) == 1

    def test_sae_preflight_reports_missing_denominator_column(self, tmp_path: Path):
        _write_sae_preflight_fixtures(tmp_path)
        data = _sae_recipe_dict()
        data["pipelines"][0]["steps"][0]["denominators"] = {
            "labor_force": "missing_denominator"
        }
        recipe = load_recipe(data)

        report = run_preflight(recipe, project_root=tmp_path)

        findings = [f for f in report.findings if f.kind == FindingKind.MISSING_MEASURE]
        assert len(findings) == 1
        assert "missing denominator columns" in findings[0].message
        assert findings[0].dataset_id == "acs5_support"

    def test_sae_preflight_reports_acs1_unavailable_vintage(self, tmp_path: Path):
        _write_sae_preflight_fixtures(tmp_path, year=2020)
        recipe = load_recipe(_sae_recipe_dict(year=2020))

        report = run_preflight(recipe, project_root=tmp_path)

        findings = [
            f for f in report.findings
            if f.kind == FindingKind.MISSING_DATASET and f.dataset_id == "acs1_county"
        ]
        assert len(findings) == 1
        assert findings[0].years == [2020]
        assert findings[0].remediation is not None
        assert findings[0].remediation.command is None

    def test_sae_recipe_plan_cli_json_exposes_resolved_task(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        _write_cli_project_markers(tmp_path)
        recipe_path = tmp_path / "sae_recipe.json"
        recipe_path.write_text(json.dumps(_sae_recipe_dict()), encoding="utf-8")
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(
            app,
            ["build", "recipe-plan", "--recipe", str(recipe_path), "--json"],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["status"] == "ok"
        sae_tasks = payload["pipelines"][0]["small_area_estimate_tasks"]
        assert len(sae_tasks) == 1
        assert sae_tasks[0]["output_dataset"] == "acs_sae_coc"
        assert sae_tasks[0]["source_path"] == "data/curated/acs/acs1_county_sae__A2023.parquet"
        assert sae_tasks[0]["support_path"] == (
            "data/curated/acs/acs5_tract_sae_support__A2022xT2020.parquet"
        )
        assert sae_tasks[0]["derived_outputs"] == {
            "labor_force": ["sae_unemployment_rate"]
        }

    def test_sae_recipe_preflight_cli_json_reports_ready(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        _write_cli_project_markers(tmp_path)
        _write_sae_preflight_fixtures(tmp_path)
        recipe_path = tmp_path / "sae_recipe.json"
        recipe_path.write_text(json.dumps(_sae_recipe_dict()), encoding="utf-8")
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(
            app,
            [
                "build",
                "recipe-preflight",
                "--recipe",
                str(recipe_path),
                "--json",
                "--non-interactive",
            ],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["status"] == "ok"
        assert payload["ready"] is True
        assert payload["blocking_count"] == 0
        assert payload["pipelines"][0]["task_count"] == 2

    def test_sae_recipe_preflight_cli_json_reports_invalid_measure_config(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        _write_cli_project_markers(tmp_path)
        _write_sae_preflight_fixtures(tmp_path)
        data = _sae_recipe_dict()
        data["pipelines"][0]["steps"][0]["measures"]["labor_force"]["outputs"] = [
            "sae_household_income_median"
        ]
        recipe_path = tmp_path / "sae_recipe.json"
        recipe_path.write_text(json.dumps(data), encoding="utf-8")
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(
            app,
            [
                "build",
                "recipe-preflight",
                "--recipe",
                str(recipe_path),
                "--json",
                "--non-interactive",
            ],
        )

        assert result.exit_code == 1
        payload = json.loads(result.output)
        assert payload["status"] == "blocked"
        measure_findings = [
            finding
            for finding in payload["findings"]
            if finding["kind"] == "missing_measure"
        ]
        assert len(measure_findings) == 1
        assert "cannot produce outputs" in measure_findings[0]["message"]

    def test_blocks_stale_translated_acs_cache(self, tmp_path: Path):
        data = _preflight_recipe(with_path=True)
        data["universe"] = {"years": [2020]}
        data["datasets"]["pit"]["years"] = {"years": [2020]}
        data["datasets"]["acs"]["years"] = {"years": [2020]}
        data["datasets"]["acs"]["path"] = STALE_TRANSLATED_ACS_PATH
        _setup_preflight_fixtures(tmp_path, include_acs=False)
        stale_path = tmp_path / STALE_TRANSLATED_ACS_PATH
        stale_path.parent.mkdir(parents=True, exist_ok=True)
        _write_stale_translated_acs_cache(stale_path)

        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)

        provenance_findings = [
            f for f in report.findings
            if f.kind == FindingKind.DATASET_PROVENANCE
        ]
        assert len(provenance_findings) == 1
        finding = provenance_findings[0]
        assert not report.is_ready
        assert finding.dataset_id == "acs"
        assert finding.years == [2020]
        assert STALE_TRANSLATED_ACS_PATH in finding.message
        assert finding.remediation is not None
        assert finding.remediation.command == STALE_TRANSLATED_ACS_REBUILD

    def test_planner_error_captured(self, tmp_path: Path):
        data = _preflight_recipe(with_path=True, identity_only=True)
        # Add step referencing a dataset not in the recipe
        data["pipelines"][0]["steps"].insert(0, {
            "resample": {
                "dataset": "nonexistent",
                "to_geometry": {"type": "coc", "vintage": 2025},
                "method": "identity",
                "measures": ["x"],
            },
        })
        # Pydantic will reject this since 'nonexistent' isn't in datasets —
        # add a dummy dataset to pass schema validation
        data["datasets"]["nonexistent"] = {
            "provider": "test",
            "product": "pit",
            "version": 1,
            "native_geometry": {"type": "coc"},
            # No years declared and no path — planner won't fail here,
            # but the path/schema probes will catch missing files
        }
        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)
        # Report should have findings (missing dataset at minimum)
        assert len(report.findings) > 0

    def test_multi_pipeline_recipe(self, tmp_path: Path):
        data = _preflight_recipe(with_path=True, identity_only=True)
        _setup_preflight_fixtures(tmp_path, include_xwalk=False, include_acs=False)
        # Add a second pipeline
        data["pipelines"].append({
            "id": "secondary",
            "target": "coc_panel",
            "steps": [
                {
                    "resample": {
                        "dataset": "pit",
                        "to_geometry": {"type": "coc", "vintage": 2025},
                        "method": "identity",
                        "measures": ["pit_total"],
                    },
                },
                {
                    "join": {
                        "datasets": ["pit"],
                        "join_on": ["geo_id", "year"],
                    },
                },
            ],
        })
        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)
        assert len(report.pipelines) == 2
        assert report.pipelines[0].pipeline_id == "main"
        assert report.pipelines[1].pipeline_id == "secondary"

    def test_schema_probe_missing_measure(self, tmp_path: Path):
        data = _preflight_recipe(with_path=True, identity_only=True)
        # Create a pit file WITHOUT the pit_total column
        pit_path = tmp_path / "data" / "pit.parquet"
        pit_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({
            "coc_id": ["COC1"],
            "year": [2020],
            "wrong_column": [10],
        }).to_parquet(pit_path)

        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)
        measure_findings = [
            f for f in report.findings
            if f.kind == FindingKind.MISSING_MEASURE
        ]
        assert len(measure_findings) >= 1
        assert "pit_total" in measure_findings[0].message

    def test_preflight_warns_on_nonstandard_acs_lag_offset(self, tmp_path: Path):
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True)
        pd.DataFrame({
            "tract_geoid": ["T1"],
            "year": [2020],
            "total_population": [100],
        }).to_parquet(data_dir / "acs_2020.parquet")

        recipe_data = {
            "version": 1,
            "name": "acs-offset-warning",
            "universe": {"years": [2020]},
            "targets": [
                {"id": "tract_panel", "geometry": {"type": "tract", "vintage": 2020}},
            ],
            "datasets": {
                "acs": {
                    "provider": "census",
                    "product": "acs5",
                    "version": 1,
                    "native_geometry": {"type": "tract", "vintage": 2020},
                    "geo_column": "tract_geoid",
                    "file_set": {
                        "path_template": "data/acs_{acs_end}.parquet",
                        "segments": [
                            {
                                "years": {"years": [2020]},
                                "geometry": {"type": "tract", "vintage": 2020},
                                "year_offsets": {"acs_end": 0},
                            },
                        ],
                    },
                },
            },
            "transforms": [],
            "pipelines": [
                {
                    "id": "main",
                    "target": "tract_panel",
                    "steps": [
                        {
                            "resample": {
                                "dataset": "acs",
                                "to_geometry": {"type": "tract", "vintage": 2020},
                                "method": "identity",
                                "measures": ["total_population"],
                            },
                        },
                        {
                            "join": {
                                "datasets": ["acs"],
                                "join_on": ["geo_id", "year"],
                            },
                        },
                    ],
                },
            ],
        }

        recipe = load_recipe(recipe_data)
        report = run_preflight(recipe, project_root=tmp_path)
        lag_findings = [
            f for f in report.findings
            if f.kind == FindingKind.TEMPORAL_ALIGNMENT
        ]
        assert len(lag_findings) == 1
        assert lag_findings[0].severity == Severity.WARNING
        assert "acs_end offset 0" in lag_findings[0].message

    def test_preflight_warns_on_same_year_acs1_vintage_static_path(self, tmp_path: Path):
        """Warn only when same-year ACS1 is used in a PIT/January-aligned pipeline."""
        data_dir = tmp_path / "data" / "curated" / "acs"
        data_dir.mkdir(parents=True)
        pd.DataFrame({
            "metro_id": ["M1"],
            "acs1_vintage": [2023],
            "unemployment_rate_acs1": [0.05],
        }).to_parquet(data_dir / "acs1_metro__A2023@Dglynnfoxv1.parquet")

        recipe_data = {
            "version": 1,
            "name": "acs1-same-year-warning",
            "universe": {"years": [2023]},
            "targets": [
                {"id": "metro_panel", "geometry": {"type": "metro", "source": "glynn_fox_v1"}},
            ],
            "datasets": {
                "pit": {
                    "provider": "hud",
                    "product": "pit",
                    "version": 1,
                    "native_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                    "years": {"years": [2023]},
                    "path": "data/pit.parquet",
                    "params": {"align": "point_in_time_jan"},
                },
                "acs1_metro": {
                    "provider": "census",
                    "product": "acs1",
                    "version": 1,
                    "native_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                    "years": {"years": [2023]},
                    "year_column": "acs1_vintage",
                    "geo_column": "metro_id",
                    "path": "data/curated/acs/acs1_metro__A2023@Dglynnfoxv1.parquet",
                },
            },
            "transforms": [],
            "pipelines": [
                {
                    "id": "main",
                    "target": "metro_panel",
                    "steps": [
                        {
                            "resample": {
                                "dataset": "pit",
                                "to_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                                "method": "identity",
                                "measures": ["pit_total"],
                            },
                        },
                        {
                            "resample": {
                                "dataset": "acs1_metro",
                                "to_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                                "method": "identity",
                                "measures": ["unemployment_rate_acs1"],
                            },
                        },
                        {
                            "join": {
                                "datasets": ["pit", "acs1_metro"],
                                "join_on": ["geo_id", "year"],
                            },
                        },
                    ],
                },
            ],
        }

        recipe = load_recipe(recipe_data)
        report = run_preflight(recipe, project_root=tmp_path)
        lag_findings = [
            f for f in report.findings
            if f.kind == FindingKind.TEMPORAL_ALIGNMENT
        ]
        assert len(lag_findings) == 1
        assert lag_findings[0].severity == Severity.WARNING
        assert "same-year ACS1 vintage" in lag_findings[0].message
        assert lag_findings[0].pipeline_id == "main"
        assert "2023" in lag_findings[0].message
        assert lag_findings[0].dataset_id == "acs1_metro"

    def test_preflight_skips_same_year_acs1_warning_without_january_alignment(self, tmp_path: Path):
        """Same-year ACS1 outside PIT/January-aligned pipelines should not warn."""
        data_dir = tmp_path / "data" / "curated" / "acs"
        data_dir.mkdir(parents=True)
        pd.DataFrame({
            "metro_id": ["M1"],
            "acs1_vintage": [2023],
            "unemployment_rate_acs1": [0.05],
        }).to_parquet(data_dir / "acs1_metro__A2023@Dglynnfoxv1.parquet")

        recipe_data = {
            "version": 1,
            "name": "acs1-same-year-no-jan-context",
            "universe": {"years": [2023]},
            "targets": [
                {"id": "metro_panel", "geometry": {"type": "metro", "source": "glynn_fox_v1"}},
            ],
            "datasets": {
                "acs1_metro": {
                    "provider": "census",
                    "product": "acs1",
                    "version": 1,
                    "native_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                    "years": {"years": [2023]},
                    "year_column": "acs1_vintage",
                    "geo_column": "metro_id",
                    "path": "data/curated/acs/acs1_metro__A2023@Dglynnfoxv1.parquet",
                },
            },
            "transforms": [],
            "pipelines": [
                {
                    "id": "main",
                    "target": "metro_panel",
                    "steps": [
                        {
                            "resample": {
                                "dataset": "acs1_metro",
                                "to_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                                "method": "identity",
                                "measures": ["unemployment_rate_acs1"],
                            },
                        },
                        {
                            "join": {
                                "datasets": ["acs1_metro"],
                                "join_on": ["geo_id", "year"],
                            },
                        },
                    ],
                },
            ],
        }

        recipe = load_recipe(recipe_data)
        report = run_preflight(recipe, project_root=tmp_path)
        lag_findings = [
            f for f in report.findings
            if f.kind == FindingKind.TEMPORAL_ALIGNMENT
        ]
        assert lag_findings == []

    def test_preflight_no_warning_on_prior_year_acs1_vintage_static_path(self, tmp_path: Path):
        """Static-path ACS1 whose __A{year} vintage is prior year
        -> no TEMPORAL_ALIGNMENT warning."""
        data_dir = tmp_path / "data" / "curated" / "acs"
        data_dir.mkdir(parents=True)
        pd.DataFrame({
            "metro_id": ["M1"],
            "acs1_vintage": [2022],
            "unemployment_rate_acs1": [0.05],
        }).to_parquet(data_dir / "acs1_metro__A2022@Dglynnfoxv1.parquet")

        recipe_data = {
            "version": 1,
            "name": "acs1-prior-year-ok",
            "universe": {"years": [2023]},
            "targets": [
                {"id": "metro_panel", "geometry": {"type": "metro", "source": "glynn_fox_v1"}},
            ],
            "datasets": {
                "acs1_metro": {
                    "provider": "census",
                    "product": "acs1",
                    "version": 1,
                    "native_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                    "years": {"years": [2023]},
                    "year_column": "acs1_vintage",
                    "geo_column": "metro_id",
                    "path": "data/curated/acs/acs1_metro__A2022@Dglynnfoxv1.parquet",
                },
            },
            "transforms": [],
            "pipelines": [
                {
                    "id": "main",
                    "target": "metro_panel",
                    "steps": [
                        {
                            "resample": {
                                "dataset": "acs1_metro",
                                "to_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                                "method": "identity",
                                "measures": ["unemployment_rate_acs1"],
                            },
                        },
                        {
                            "join": {
                                "datasets": ["acs1_metro"],
                                "join_on": ["geo_id", "year"],
                            },
                        },
                    ],
                },
            ],
        }

        recipe = load_recipe(recipe_data)
        report = run_preflight(recipe, project_root=tmp_path)
        lag_findings = [
            f for f in report.findings
            if f.kind == FindingKind.TEMPORAL_ALIGNMENT
        ]
        assert lag_findings == []

    def test_preflight_warns_on_same_year_acs1_file_set(self, tmp_path: Path):
        """File-set ACS1 with explicit acs1_end offset 0 warns in January-aligned pipelines."""
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True)
        pd.DataFrame({
            "metro_id": ["M1"],
            "acs1_vintage": [2023],
            "unemployment_rate_acs1": [0.05],
        }).to_parquet(data_dir / "acs1_2023.parquet")

        recipe_data = {
            "version": 1,
            "name": "acs1-fileset-same-year",
            "universe": {"years": [2023]},
            "targets": [
                {"id": "metro_panel", "geometry": {"type": "metro", "source": "glynn_fox_v1"}},
            ],
            "datasets": {
                "pit": {
                    "provider": "hud",
                    "product": "pit",
                    "version": 1,
                    "native_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                    "years": {"years": [2023]},
                    "path": "data/pit.parquet",
                    "params": {"align": "point_in_time_jan"},
                },
                "acs1_metro": {
                    "provider": "census",
                    "product": "acs1",
                    "version": 1,
                    "native_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                    "geo_column": "metro_id",
                    "file_set": {
                        "path_template": "data/acs1_{acs1_end}.parquet",
                        "segments": [
                            {
                                "years": {"years": [2023]},
                                "geometry": {"type": "metro", "source": "glynn_fox_v1"},
                                "year_offsets": {"acs1_end": 0},
                            },
                        ],
                    },
                },
            },
            "transforms": [],
            "pipelines": [
                {
                    "id": "main",
                    "target": "metro_panel",
                    "steps": [
                        {
                            "resample": {
                                "dataset": "pit",
                                "to_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                                "method": "identity",
                                "measures": ["pit_total"],
                            },
                        },
                        {
                            "resample": {
                                "dataset": "acs1_metro",
                                "to_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                                "method": "identity",
                                "measures": ["unemployment_rate_acs1"],
                            },
                        },
                        {
                            "join": {
                                "datasets": ["pit", "acs1_metro"],
                                "join_on": ["geo_id", "year"],
                            },
                        },
                    ],
                },
            ],
        }

        recipe = load_recipe(recipe_data)
        report = run_preflight(recipe, project_root=tmp_path)
        lag_findings = [
            f for f in report.findings
            if f.kind == FindingKind.TEMPORAL_ALIGNMENT
        ]
        assert len(lag_findings) == 1
        assert lag_findings[0].severity == Severity.WARNING
        assert "same-year ACS1 vintage" in lag_findings[0].message
        assert lag_findings[0].pipeline_id == "main"
        assert lag_findings[0].dataset_id == "acs1_metro"

    def test_preflight_warns_on_same_year_acs1_file_set_year_template(self, tmp_path: Path):
        """Direct {year} file-set templates should also warn for January-aligned recipes."""
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True)
        pd.DataFrame({
            "metro_id": ["M1"],
            "acs1_vintage": [2023],
            "unemployment_rate_acs1": [0.05],
        }).to_parquet(data_dir / "acs1_2023.parquet")

        recipe_data = {
            "version": 1,
            "name": "acs1-fileset-year-template",
            "universe": {"years": [2023]},
            "targets": [
                {"id": "metro_panel", "geometry": {"type": "metro", "source": "glynn_fox_v1"}},
            ],
            "datasets": {
                "pit": {
                    "provider": "hud",
                    "product": "pit",
                    "version": 1,
                    "native_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                    "years": {"years": [2023]},
                    "path": "data/pit.parquet",
                    "params": {"align": "point_in_time_jan"},
                },
                "acs1_metro": {
                    "provider": "census",
                    "product": "acs1",
                    "version": 1,
                    "native_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                    "year_column": "acs1_vintage",
                    "geo_column": "metro_id",
                    "file_set": {
                        "path_template": "data/acs1_{year}.parquet",
                        "segments": [
                            {
                                "years": {"years": [2023]},
                                "geometry": {"type": "metro", "source": "glynn_fox_v1"},
                            },
                        ],
                    },
                },
            },
            "transforms": [],
            "pipelines": [
                {
                    "id": "main",
                    "target": "metro_panel",
                    "steps": [
                        {
                            "resample": {
                                "dataset": "pit",
                                "to_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                                "method": "identity",
                                "measures": ["pit_total"],
                            },
                        },
                        {
                            "resample": {
                                "dataset": "acs1_metro",
                                "to_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                                "method": "identity",
                                "measures": ["unemployment_rate_acs1"],
                            },
                        },
                        {
                            "join": {
                                "datasets": ["pit", "acs1_metro"],
                                "join_on": ["geo_id", "year"],
                            },
                        },
                    ],
                },
            ],
        }

        recipe = load_recipe(recipe_data)
        report = run_preflight(recipe, project_root=tmp_path)
        lag_findings = [
            f for f in report.findings
            if f.kind == FindingKind.TEMPORAL_ALIGNMENT
        ]
        assert len(lag_findings) == 1
        assert lag_findings[0].severity == Severity.WARNING
        assert "same-year ACS1 vintage" in lag_findings[0].message
        assert lag_findings[0].pipeline_id == "main"
        assert lag_findings[0].years == [2023]

    def test_preflight_blocks_bad_interpolate_to_month_source_data(self, tmp_path: Path):
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True)
        pd.DataFrame({
            "county_fips": ["01001", "01001"],
            "year": [2019, 2020],
            "reference_date": [7, 7],
            "population": [1000, 1100],
        }).to_parquet(data_dir / "pep.parquet")

        recipe_data = {
            "version": 1,
            "name": "pep-interpolate-preflight",
            "universe": {"years": [2020]},
            "targets": [
                {"id": "county_panel", "geometry": {"type": "county", "vintage": 2020}},
            ],
            "datasets": {
                "pep": {
                    "provider": "census",
                    "product": "pep",
                    "version": 1,
                    "native_geometry": {"type": "county", "vintage": 2020},
                    "path": "data/pep.parquet",
                    "year_column": "year",
                    "geo_column": "county_fips",
                },
            },
            "filters": {
                "pep": {
                    "type": "temporal",
                    "column": "reference_date",
                    "method": "interpolate_to_month",
                    "month": 1,
                },
            },
            "transforms": [],
            "pipelines": [
                {
                    "id": "main",
                    "target": "county_panel",
                    "steps": [
                        {
                            "resample": {
                                "dataset": "pep",
                                "to_geometry": {"type": "county", "vintage": 2020},
                                "method": "identity",
                                "measures": ["population"],
                            },
                        },
                        {
                            "join": {
                                "datasets": ["pep"],
                                "join_on": ["geo_id", "year"],
                            },
                        },
                    ],
                },
            ],
        }

        recipe = load_recipe(recipe_data)
        report = run_preflight(recipe, project_root=tmp_path)
        temporal_findings = [
            f for f in report.findings
            if f.kind == FindingKind.TEMPORAL_FILTER
        ]
        assert len(temporal_findings) >= 1
        assert "requires a datetime column" in temporal_findings[0].message
        assert not report.is_ready


# ---------------------------------------------------------------------------
# Report model tests
# ---------------------------------------------------------------------------

class TestPreflightReport:

    def test_to_dict(self):
        report = PreflightReport(
            recipe_name="test",
            recipe_version=1,
            universe_years=[2020, 2021],
        )
        d = report.to_dict()
        assert d["ready"] is True
        assert d["blocking_count"] == 0
        assert d["warning_count"] == 0

    def test_gaps_manifest(self):
        report = PreflightReport(
            recipe_name="test",
            recipe_version=1,
            universe_years=[2020, 2021],
            findings=[
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.MISSING_DATASET,
                    message="Dataset 'pit' missing",
                    dataset_id="pit",
                ),
                PreflightFinding(
                    severity=Severity.WARNING,
                    kind=FindingKind.MISSING_DATASET,
                    message="Dataset 'optional' missing",
                    dataset_id="optional",
                ),
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.PLANNER_ERROR,
                    message="planner failed",
                ),
            ],
        )
        manifest = report.gaps_manifest()
        assert manifest["total_gaps"] == 2  # planner_error is not a gap kind
        assert manifest["blocking_gaps"] == 1
        assert "missing_dataset" in manifest["gaps_by_kind"]
        assert len(manifest["gaps_by_kind"]["missing_dataset"]) == 2

    def test_finding_to_dict_with_remediation(self):
        f = PreflightFinding(
            severity=Severity.ERROR,
            kind=FindingKind.MISSING_TRANSFORM,
            message="Transform 'x' missing",
            transform_id="x",
            remediation=_make_remediation(),
        )
        d = f.to_dict()
        assert d["severity"] == "error"
        assert d["kind"] == "missing_transform"
        assert d["transform_id"] == "x"
        assert "remediation" in d
        assert d["remediation"]["command"] == "hhplab generate xwalks"

    def test_blocking_findings(self):
        report = PreflightReport(
            recipe_name="test",
            recipe_version=1,
            universe_years=[2020],
            findings=[
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.MISSING_DATASET,
                    message="err",
                ),
                PreflightFinding(
                    severity=Severity.WARNING,
                    kind=FindingKind.MISSING_DATASET,
                    message="warn",
                ),
            ],
        )
        blockers = report.blocking_findings()
        assert len(blockers) == 1
        assert blockers[0].message == "err"


def _make_remediation():
    from hhplab.recipe.preflight import Remediation
    return Remediation(
        hint="Generate crosswalk artifacts.",
        command="hhplab generate xwalks",
    )


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------

def _make_project_root(tmp_path: Path) -> None:
    """Create marker files so _check_working_directory() doesn't warn."""
    (tmp_path / "pyproject.toml").write_text("[project]\nname='test'\n")
    (tmp_path / "hhplab").mkdir(exist_ok=True)
    (tmp_path / "data").mkdir(exist_ok=True)


def _write_recipe(tmp_path: Path, data: dict) -> Path:
    import yaml

    recipe_file = tmp_path / "recipe.yaml"
    recipe_file.write_text(yaml.dump(data), encoding="utf-8")
    return recipe_file


class TestPreflightCLI:

    def test_human_output_clean(self, tmp_path: Path, monkeypatch):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _preflight_recipe(with_path=True, identity_only=True)
        _setup_preflight_fixtures(
            tmp_path, include_xwalk=False, include_acs=False,
        )
        rf = _write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe-preflight",
            "--recipe", str(rf),
        ])
        assert result.exit_code == 0
        assert "Ready to build" in result.output

    def test_human_output_blockers(self, tmp_path: Path, monkeypatch):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _preflight_recipe(with_path=True, identity_only=True)
        # Don't create fixtures — missing files
        rf = _write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe-preflight",
            "--recipe", str(rf),
        ])
        assert result.exit_code == 1
        assert "Blocker" in result.output or "FAILED" in result.output

    def test_json_output_ready(self, tmp_path: Path, monkeypatch):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _preflight_recipe(with_path=True, identity_only=True)
        _setup_preflight_fixtures(
            tmp_path, include_xwalk=False, include_acs=False,
        )
        rf = _write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe-preflight",
            "--recipe", str(rf),
            "--json",
        ])
        assert result.exit_code == 0
        out = json.loads(result.output)
        assert out["status"] == "ok"
        assert out["ready"] is True

    def test_json_output_ready_accepts_non_interactive_flag(
        self,
        tmp_path: Path,
        monkeypatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _preflight_recipe(with_path=True, identity_only=True)
        _setup_preflight_fixtures(
            tmp_path, include_xwalk=False, include_acs=False,
        )
        rf = _write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe-preflight",
            "--recipe", str(rf),
            "--json",
            "--non-interactive",
        ])
        assert result.exit_code == 0
        out = json.loads(result.output)
        assert out["status"] == "ok"
        assert out["ready"] is True

    def test_json_output_blocked(self, tmp_path: Path, monkeypatch):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _preflight_recipe(with_path=True, identity_only=True)
        rf = _write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe-preflight",
            "--recipe", str(rf),
            "--json",
        ])
        assert result.exit_code == 1
        out = json.loads(result.output)
        assert out["status"] == "blocked"
        assert out["blocking_count"] > 0

    def test_gaps_output(self, tmp_path: Path, monkeypatch):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _preflight_recipe(with_path=True, identity_only=True)
        rf = _write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe-preflight",
            "--recipe", str(rf),
            "--gaps",
        ])
        out = json.loads(result.output)
        assert "total_gaps" in out
        assert "gaps_by_kind" in out

    def test_invalid_recipe(self, tmp_path: Path, monkeypatch):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        rf = tmp_path / "bad.yaml"
        rf.write_text("not: a: recipe", encoding="utf-8")
        result = runner.invoke(app, [
            "build", "recipe-preflight",
            "--recipe", str(rf),
        ])
        assert result.exit_code == 2


class TestBuildRecipeWithPreflight:

    def test_preflight_blocks_execution(self, tmp_path: Path, monkeypatch):
        """When dataset files exist but transform is missing, preflight catches it."""
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _preflight_recipe(with_path=True)
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        _setup_preflight_fixtures(
            tmp_path, include_xwalk=False,
        )
        rf = _write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(rf),
        ])
        assert result.exit_code == 1
        assert "Preflight" in result.output or "blocker" in result.output

    def test_preflight_blocks_json(self, tmp_path: Path, monkeypatch):
        """JSON mode emits blocked status with preflight details."""
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _preflight_recipe(with_path=True)
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        _setup_preflight_fixtures(
            tmp_path, include_xwalk=False,
        )
        rf = _write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(rf),
            "--json",
        ])
        assert result.exit_code == 1
        out = json.loads(result.output)
        assert out["status"] == "blocked"
        assert "preflight" in out

    def test_skip_preflight(self, tmp_path: Path, monkeypatch):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        # Use a recipe without path so old validation passes
        data = _preflight_recipe(with_path=False, identity_only=True)
        rf = _write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(rf),
            "--dry-run", "--skip-preflight",
        ])
        assert result.exit_code == 0

    def test_clean_preflight_allows_dry_run(
        self, tmp_path: Path, monkeypatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _preflight_recipe(with_path=True, identity_only=True)
        _setup_preflight_fixtures(
            tmp_path, include_xwalk=False, include_acs=False,
        )
        rf = _write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(rf),
            "--dry-run",
        ])
        assert result.exit_code == 0
        assert "all clear" in result.output

    def test_missing_dataset_routes_through_preflight_json(
        self, tmp_path: Path, monkeypatch,
    ):
        """Missing dataset paths should produce status=blocked with preflight
        payload, not status=error with validation.errors (coclab-pu6j.3)."""
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _preflight_recipe(with_path=True, identity_only=True)
        # Don't create pit file — missing dataset
        rf = _write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(rf),
            "--json",
        ])
        assert result.exit_code == 1
        out = json.loads(result.output)
        assert out["status"] == "blocked"
        assert "preflight" in out


# ---------------------------------------------------------------------------
# Recipe-scoped path checking tests (coclab-pu6j.1)
# ---------------------------------------------------------------------------


class TestRecipeScopedPathChecks:
    """Verify that preflight only checks dataset-years required by the
    execution plan, not every year in every file_set segment."""

    def test_file_set_years_scoped_to_universe(self, tmp_path: Path):
        """A file_set segment covering 2020-2022 with universe=2020-2020
        should only report missing files for 2020, not 2021/2022."""
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True)
        # Create ONLY the 2020 file
        pd.DataFrame({
            "coc_id": ["COC1"], "year": [2020], "pit_total": [10],
        }).to_parquet(data_dir / "pit_2020.parquet")

        recipe_data = {
            "version": 1,
            "name": "scoped-test",
            "universe": {"range": "2020-2020"},
            "targets": [
                {"id": "coc_panel", "geometry": {"type": "coc", "vintage": 2025}},
            ],
            "datasets": {
                "pit": {
                    "provider": "hud",
                    "product": "pit",
                    "version": 1,
                    "native_geometry": {"type": "coc"},
                    "file_set": {
                        "path_template": "data/pit_{year}.parquet",
                        "segments": [
                            {"years": "2020-2022", "geometry": {"type": "coc"}},
                        ],
                    },
                },
            },
            "transforms": [],
            "pipelines": [
                {
                    "id": "main",
                    "target": "coc_panel",
                    "steps": [
                        {
                            "resample": {
                                "dataset": "pit",
                                "to_geometry": {"type": "coc", "vintage": 2025},
                                "method": "identity",
                                "measures": ["pit_total"],
                            },
                        },
                        {
                            "join": {
                                "datasets": ["pit"],
                                "join_on": ["geo_id", "year"],
                            },
                        },
                    ],
                },
            ],
        }

        recipe = load_recipe(recipe_data)
        report = run_preflight(recipe, project_root=tmp_path)
        # Should be ready — only 2020 is needed and 2020 file exists
        ds_findings = [
            f for f in report.findings
            if f.kind == FindingKind.MISSING_DATASET
        ]
        assert len(ds_findings) == 0, (
            f"Expected no missing-dataset findings but got: "
            f"{[f.message for f in ds_findings]}"
        )
        assert report.is_ready

    def test_unused_dataset_not_checked(self, tmp_path: Path):
        """A dataset declared in the recipe but not used by any pipeline
        should not generate missing-file findings."""
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True)
        pd.DataFrame({
            "coc_id": ["COC1"], "year": [2020], "pit_total": [10],
        }).to_parquet(data_dir / "pit.parquet")

        recipe_data = {
            "version": 1,
            "name": "unused-ds-test",
            "universe": {"range": "2020-2020"},
            "targets": [
                {"id": "coc_panel", "geometry": {"type": "coc", "vintage": 2025}},
            ],
            "datasets": {
                "pit": {
                    "provider": "hud",
                    "product": "pit",
                    "version": 1,
                    "native_geometry": {"type": "coc"},
                    "path": "data/pit.parquet",
                    "years": "2020-2020",
                },
                "unused": {
                    "provider": "hud",
                    "product": "pit",
                    "version": 1,
                    "native_geometry": {"type": "coc"},
                    "path": "data/nonexistent.parquet",
                    "optional": True,
                },
            },
            "transforms": [],
            "pipelines": [
                {
                    "id": "main",
                    "target": "coc_panel",
                    "steps": [
                        {
                            "resample": {
                                "dataset": "pit",
                                "to_geometry": {"type": "coc", "vintage": 2025},
                                "method": "identity",
                                "measures": ["pit_total"],
                            },
                        },
                        {
                            "join": {
                                "datasets": ["pit"],
                                "join_on": ["geo_id", "year"],
                            },
                        },
                    ],
                },
            ],
        }

        recipe = load_recipe(recipe_data)
        report = run_preflight(recipe, project_root=tmp_path)
        ds_findings = [
            f for f in report.findings
            if f.kind == FindingKind.MISSING_DATASET
        ]
        assert len(ds_findings) == 0
        assert report.is_ready

    def test_file_set_distinct_paths_do_not_trigger_static_broadcast(
        self, tmp_path: Path,
    ):
        """A file_set with distinct per-year files is safe without a
        year column in the individual parquet files."""
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True)
        for year in (2020, 2021, 2022):
            pd.DataFrame({
                "coc_id": ["COC1"],
                "pit_total": [10 + (year - 2020)],
            }).to_parquet(data_dir / f"pit_{year}.parquet")

        recipe_data = {
            "version": 1,
            "name": "distinct-fileset-test",
            "universe": {"range": "2020-2022"},
            "targets": [
                {"id": "coc_panel", "geometry": {"type": "coc", "vintage": 2025}},
            ],
            "datasets": {
                "pit": {
                    "provider": "hud",
                    "product": "pit",
                    "version": 1,
                    "native_geometry": {"type": "coc"},
                    "file_set": {
                        "path_template": "data/pit_{year}.parquet",
                        "segments": [
                            {"years": "2020-2022", "geometry": {"type": "coc"}},
                        ],
                    },
                },
            },
            "transforms": [],
            "pipelines": [
                {
                    "id": "main",
                    "target": "coc_panel",
                    "steps": [
                        {
                            "resample": {
                                "dataset": "pit",
                                "to_geometry": {"type": "coc", "vintage": 2025},
                                "method": "identity",
                                "measures": ["pit_total"],
                            },
                        },
                        {
                            "join": {
                                "datasets": ["pit"],
                                "join_on": ["geo_id", "year"],
                            },
                        },
                    ],
                },
            ],
        }

        recipe = load_recipe(recipe_data)
        report = run_preflight(recipe, project_root=tmp_path)
        broadcast_findings = [
            f for f in report.findings
            if f.kind == FindingKind.STATIC_BROADCAST
        ]
        assert len(broadcast_findings) == 0
        assert report.is_ready

    def test_generated_metro_transform_ready_when_inputs_exist(
        self, tmp_path: Path,
    ):
        """Generated metro transforms should not block preflight when
        their source artifacts are present."""
        data_dir = tmp_path / "data"
        (data_dir / "curated" / "metro").mkdir(parents=True)
        pd.DataFrame({
            "county_fips": ["01001"],
            "year": [2020],
            "population": [1000],
        }).to_parquet(data_dir / "pep.parquet")
        pd.DataFrame({"metro_id": ["GF01"], "county_fips": ["01001"]}).to_parquet(
            data_dir
            / "curated"
            / "metro"
            / "metro_county_membership__glynn_fox_v1.parquet"
        )

        recipe_data = {
            "version": 1,
            "name": "metro-transform-test",
            "universe": {"range": "2020-2020"},
            "targets": [
                {"id": "metro_panel", "geometry": {"type": "metro", "source": "glynn_fox_v1"}},
            ],
            "datasets": {
                "pep_county": {
                    "provider": "census",
                    "product": "pep",
                    "version": 1,
                    "native_geometry": {"type": "county", "vintage": 2020},
                    "path": "data/pep.parquet",
                    "years": "2020-2020",
                    "year_column": "year",
                    "geo_column": "county_fips",
                },
            },
            "transforms": [
                {
                    "id": "county_to_metro",
                    "type": "crosswalk",
                    "from": {"type": "county", "vintage": 2020},
                    "to": {"type": "metro", "source": "glynn_fox_v1"},
                    "spec": {"weighting": {"scheme": "area"}},
                },
            ],
            "pipelines": [
                {
                    "id": "main",
                    "target": "metro_panel",
                    "steps": [
                        {"materialize": {"transforms": ["county_to_metro"]}},
                        {
                            "resample": {
                                "dataset": "pep_county",
                                "to_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                                "method": "aggregate",
                                "via": "county_to_metro",
                                "measures": ["population"],
                            },
                        },
                        {
                            "join": {
                                "datasets": ["pep_county"],
                                "join_on": ["geo_id", "year"],
                            },
                        },
                    ],
                },
            ],
        }

        recipe = load_recipe(recipe_data)
        report = run_preflight(recipe, project_root=tmp_path)
        transform_findings = [
            f for f in report.findings
            if f.kind == FindingKind.MISSING_TRANSFORM
        ]
        assert len(transform_findings) == 0
        assert report.is_ready

    def test_multi_pipeline_deduplicates_dataset_year_checks(
        self, tmp_path: Path,
    ):
        """Two pipelines referencing the same dataset-year should not
        produce duplicate missing-file findings."""
        recipe_data = {
            "version": 1,
            "name": "dedup-test",
            "universe": {"range": "2020-2020"},
            "targets": [
                {"id": "t1", "geometry": {"type": "coc", "vintage": 2025}},
            ],
            "datasets": {
                "pit": {
                    "provider": "hud",
                    "product": "pit",
                    "version": 1,
                    "native_geometry": {"type": "coc"},
                    "path": "data/pit.parquet",
                    "years": "2020-2020",
                },
            },
            "transforms": [],
            "pipelines": [
                {
                    "id": "p1",
                    "target": "t1",
                    "steps": [{
                        "resample": {
                            "dataset": "pit",
                            "to_geometry": {"type": "coc", "vintage": 2025},
                            "method": "identity",
                            "measures": ["pit_total"],
                        },
                    }],
                },
                {
                    "id": "p2",
                    "target": "t1",
                    "steps": [{
                        "resample": {
                            "dataset": "pit",
                            "to_geometry": {"type": "coc", "vintage": 2025},
                            "method": "identity",
                            "measures": ["pit_total"],
                        },
                    }],
                },
            ],
        }

        recipe = load_recipe(recipe_data)
        report = run_preflight(recipe, project_root=tmp_path)
        ds_findings = [
            f for f in report.findings
            if f.kind == FindingKind.MISSING_DATASET
        ]
        # Should be exactly 1 finding (deduplicated), not 2
        assert len(ds_findings) == 1

    def test_planner_error_surfaces_as_uncovered_years_gap(
        self, tmp_path: Path,
    ):
        """When a planner error indicates uncovered years, the gaps
        manifest should include an uncovered_years entry (coclab-hh6d)."""
        recipe_data = {
            "version": 1,
            "name": "uncovered-test",
            "universe": {"range": "2020-2021"},
            "targets": [
                {"id": "t1", "geometry": {"type": "coc", "vintage": 2025}},
            ],
            "datasets": {
                "pit": {
                    "provider": "hud",
                    "product": "pit",
                    "version": 1,
                    "native_geometry": {"type": "coc"},
                    "years": "2020-2020",
                },
            },
            "transforms": [],
            "pipelines": [
                {
                    "id": "main",
                    "target": "t1",
                    "steps": [{
                        "resample": {
                            "dataset": "pit",
                            "to_geometry": {"type": "coc", "vintage": 2025},
                            "method": "identity",
                            "measures": ["pit_total"],
                        },
                    }],
                },
            ],
        }

        recipe = load_recipe(recipe_data)
        report = run_preflight(recipe, project_root=tmp_path)
        manifest = report.gaps_manifest()
        assert manifest["blocking_gaps"] > 0
        assert "uncovered_years" in manifest["gaps_by_kind"]
        # Verify gap metadata includes remediation and affected info
        gap = manifest["gaps_by_kind"]["uncovered_years"][0]
        assert gap["severity"] == "error"
        assert "years" in gap
        assert "remediation" in gap


# ---------------------------------------------------------------------------
# Gaps manifest tests (coclab-hh6d)
# ---------------------------------------------------------------------------


class TestGapsManifest:
    """Verify the machine-readable gaps manifest includes per-gap metadata
    for all gap types, with severity classification and remediation hints."""

    def test_gaps_manifest_missing_dataset(self, tmp_path: Path):
        """Missing dataset gaps should include dataset_id and remediation."""
        recipe_data = {
            "version": 1,
            "name": "gaps-ds-test",
            "universe": {"range": "2020-2020"},
            "targets": [
                {"id": "t1", "geometry": {"type": "coc", "vintage": 2025}},
            ],
            "datasets": {
                "pit": {
                    "provider": "hud",
                    "product": "pit",
                    "version": 1,
                    "native_geometry": {"type": "coc"},
                    "path": "data/pit.parquet",
                    "years": "2020-2020",
                },
            },
            "transforms": [],
            "pipelines": [{
                "id": "main",
                "target": "t1",
                "steps": [{
                    "resample": {
                        "dataset": "pit",
                        "to_geometry": {"type": "coc", "vintage": 2025},
                        "method": "identity",
                        "measures": ["pit_total"],
                    },
                }],
            }],
        }

        recipe = load_recipe(recipe_data)
        report = run_preflight(recipe, project_root=tmp_path)
        manifest = report.gaps_manifest()
        assert manifest["total_gaps"] > 0
        assert manifest["blocking_gaps"] > 0
        ds_gaps = manifest["gaps_by_kind"].get("missing_dataset", [])
        assert len(ds_gaps) >= 1
        assert ds_gaps[0]["dataset_id"] == "pit"
        assert "remediation" in ds_gaps[0]
        assert "command" in ds_gaps[0]["remediation"]

    def test_gaps_manifest_missing_transform(self, tmp_path: Path):
        """Missing transform gaps should include transform_id and remediation."""
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True)
        pd.DataFrame({
            "coc_id": ["COC1"], "year": [2020], "pop": [100],
        }).to_parquet(data_dir / "acs.parquet")

        recipe_data = {
            "version": 1,
            "name": "gaps-xform-test",
            "universe": {"range": "2020-2020"},
            "targets": [
                {"id": "t1", "geometry": {"type": "coc", "vintage": 2025}},
            ],
            "datasets": {
                "acs": {
                    "provider": "census",
                    "product": "acs5",
                    "version": 1,
                    "native_geometry": {"type": "tract", "vintage": 2020},
                    "path": "data/acs.parquet",
                    "years": "2020-2020",
                },
            },
            "transforms": [{
                "id": "tract_to_coc",
                "type": "crosswalk",
                "from": {"type": "tract", "vintage": 2020},
                "to": {"type": "coc", "vintage": 2025},
                "spec": {"weighting": {"scheme": "area"}},
            }],
            "pipelines": [{
                "id": "main",
                "target": "t1",
                "steps": [
                    {"materialize": {"transforms": ["tract_to_coc"]}},
                    {
                        "resample": {
                            "dataset": "acs",
                            "to_geometry": {"type": "coc", "vintage": 2025},
                            "method": "aggregate",
                            "via": "tract_to_coc",
                            "measures": ["pop"],
                        },
                    },
                ],
            }],
        }

        recipe = load_recipe(recipe_data)
        report = run_preflight(recipe, project_root=tmp_path)
        manifest = report.gaps_manifest()
        xform_gaps = manifest["gaps_by_kind"].get("missing_transform", [])
        assert len(xform_gaps) >= 1
        assert xform_gaps[0]["transform_id"] == "tract_to_coc"
        assert "remediation" in xform_gaps[0]

    def test_gaps_manifest_missing_map_artifact(self, tmp_path: Path):
        """Missing map artifacts should appear in the gaps manifest."""
        data = _map_preflight_recipe(
            geometry={"type": "coc", "vintage": 2025},
        )
        _setup_preflight_fixtures(tmp_path, include_xwalk=False, include_acs=False)

        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)
        manifest = report.gaps_manifest()
        map_gaps = manifest["gaps_by_kind"].get("missing_map_artifact", [])

        assert len(map_gaps) == 1
        assert map_gaps[0]["geometry"] == "coc"
        assert map_gaps[0]["remediation"]["command"] == (
            "hhplab ingest boundaries --source hud_exchange --vintage 2025"
        )

    def test_gaps_manifest_missing_column(self, tmp_path: Path):
        """Missing measure column gaps should appear in the manifest."""
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True)
        pd.DataFrame({
            "coc_id": ["COC1"], "year": [2020], "wrong_col": [10],
        }).to_parquet(data_dir / "pit.parquet")

        recipe_data = {
            "version": 1,
            "name": "gaps-col-test",
            "universe": {"range": "2020-2020"},
            "targets": [
                {"id": "t1", "geometry": {"type": "coc", "vintage": 2025}},
            ],
            "datasets": {
                "pit": {
                    "provider": "hud",
                    "product": "pit",
                    "version": 1,
                    "native_geometry": {"type": "coc"},
                    "path": "data/pit.parquet",
                    "years": "2020-2020",
                },
            },
            "transforms": [],
            "pipelines": [{
                "id": "main",
                "target": "t1",
                "steps": [{
                    "resample": {
                        "dataset": "pit",
                        "to_geometry": {"type": "coc", "vintage": 2025},
                        "method": "identity",
                        "measures": ["pit_total"],
                    },
                }],
            }],
        }

        recipe = load_recipe(recipe_data)
        report = run_preflight(recipe, project_root=tmp_path)
        manifest = report.gaps_manifest()
        measure_gaps = manifest["gaps_by_kind"].get("missing_measure", [])
        assert len(measure_gaps) >= 1
        assert "pit_total" in measure_gaps[0]["message"]

    def test_gaps_manifest_uncovered_years_with_remediation(
        self, tmp_path: Path,
    ):
        """Uncovered-year gaps from planner errors should include
        actionable remediation metadata (coclab-hh6d regression)."""
        recipe_data = {
            "version": 1,
            "name": "gaps-uncovered-test",
            "universe": {"range": "2020-2021"},
            "targets": [
                {"id": "t1", "geometry": {"type": "coc", "vintage": 2025}},
            ],
            "datasets": {
                "pit": {
                    "provider": "hud",
                    "product": "pit",
                    "version": 1,
                    "native_geometry": {"type": "coc"},
                    "years": "2020-2020",
                },
            },
            "transforms": [],
            "pipelines": [{
                "id": "main",
                "target": "t1",
                "steps": [{
                    "resample": {
                        "dataset": "pit",
                        "to_geometry": {"type": "coc", "vintage": 2025},
                        "method": "identity",
                        "measures": ["pit_total"],
                    },
                }],
            }],
        }

        recipe = load_recipe(recipe_data)
        report = run_preflight(recipe, project_root=tmp_path)

        # Verify --json output: should be blocked, not ready
        report_dict = report.to_dict()
        assert not report_dict["ready"]
        assert report_dict["blocking_count"] > 0

        # Verify --gaps output: must include uncovered_years with metadata
        manifest = report.gaps_manifest()
        assert manifest["total_gaps"] > 0
        assert manifest["blocking_gaps"] > 0
        assert "uncovered_years" in manifest["gaps_by_kind"]
        gap = manifest["gaps_by_kind"]["uncovered_years"][0]
        assert gap["severity"] == "error"
        assert gap["years"] == [2021]
        assert gap["dataset_id"] == "pit"
        assert "remediation" in gap
        assert "hint" in gap["remediation"]
        assert "universe" in gap["remediation"]["hint"].lower()

    def test_gaps_cli_uncovered_years_not_empty(
        self, tmp_path: Path, monkeypatch,
    ):
        """CLI --gaps should report blocking gaps for uncovered-year
        planner errors, not status=ok with total_gaps=0."""
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        recipe_data = {
            "version": 1,
            "name": "gaps-cli-test",
            "universe": {"range": "2020-2021"},
            "targets": [
                {"id": "t1", "geometry": {"type": "coc", "vintage": 2025}},
            ],
            "datasets": {
                "pit": {
                    "provider": "hud",
                    "product": "pit",
                    "version": 1,
                    "native_geometry": {"type": "coc"},
                    "years": "2020-2020",
                },
            },
            "transforms": [],
            "pipelines": [{
                "id": "main",
                "target": "t1",
                "steps": [{
                    "resample": {
                        "dataset": "pit",
                        "to_geometry": {"type": "coc", "vintage": 2025},
                        "method": "identity",
                        "measures": ["pit_total"],
                    },
                }],
            }],
        }
        rf = _write_recipe(tmp_path, recipe_data)
        result = runner.invoke(app, [
            "build", "recipe-preflight",
            "--recipe", str(rf),
            "--gaps",
        ])
        out = json.loads(result.output)
        assert out["total_gaps"] > 0
        assert out["blocking_gaps"] > 0
        assert "uncovered_years" in out["gaps_by_kind"]


# ---------------------------------------------------------------------------
# Support-dataset probe tests (coclab-pu6j.2)
# ---------------------------------------------------------------------------


class TestSupportDatasetProbe:
    """Verify that weighted transforms' support-dataset requirements
    are detected by preflight before execution."""

    def test_missing_population_field_detected(self, tmp_path: Path):
        """A transform with scheme=population should block preflight
        when population_field is missing from the support dataset."""
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True)

        # Create pit dataset
        pd.DataFrame({
            "coc_id": ["COC1"], "year": [2020], "pit_total": [10],
        }).to_parquet(data_dir / "pit.parquet")

        # Create weights dataset WITHOUT the required population field
        pd.DataFrame({
            "GEOID": ["T1", "T2"],
            "year": [2020, 2020],
            "wrong_field": [100, 200],
        }).to_parquet(data_dir / "weights.parquet")

        # Create crosswalk
        xwalk_dir = data_dir / "curated" / "xwalks"
        xwalk_dir.mkdir(parents=True)
        pd.DataFrame({
            "coc_id": ["COC1", "COC2"],
            "tract_geoid": ["T1", "T2"],
            "area_share": [1.0, 1.0],
        }).to_parquet(xwalk_dir / "xwalk__B2025xT2020.parquet")

        recipe_data = {
            "version": 1,
            "name": "pop-weight-test",
            "universe": {"range": "2020-2020"},
            "targets": [
                {"id": "coc_panel", "geometry": {"type": "coc", "vintage": 2025}},
            ],
            "datasets": {
                "pit": {
                    "provider": "hud",
                    "product": "pit",
                    "version": 1,
                    "native_geometry": {"type": "coc"},
                    "path": "data/pit.parquet",
                    "years": "2020-2020",
                },
                "weights": {
                    "provider": "census",
                    "product": "acs5",
                    "version": 1,
                    "native_geometry": {"type": "tract", "vintage": 2020},
                    "path": "data/weights.parquet",
                    "years": "2020-2020",
                },
            },
            "transforms": [
                {
                    "id": "tract_to_coc",
                    "type": "crosswalk",
                    "from": {"type": "tract", "vintage": 2020},
                    "to": {"type": "coc", "vintage": 2025},
                    "spec": {
                        "weighting": {
                            "scheme": "population",
                            "population_source": "weights",
                            "population_field": "total_population",
                        },
                    },
                },
            ],
            "pipelines": [
                {
                    "id": "main",
                    "target": "coc_panel",
                    "steps": [
                        {"materialize": {"transforms": ["tract_to_coc"]}},
                        {
                            "resample": {
                                "dataset": "pit",
                                "to_geometry": {"type": "coc", "vintage": 2025},
                                "method": "identity",
                                "measures": ["pit_total"],
                            },
                        },
                        {
                            "join": {
                                "datasets": ["pit"],
                                "join_on": ["geo_id", "year"],
                            },
                        },
                    ],
                },
            ],
        }

        recipe = load_recipe(recipe_data)
        report = run_preflight(recipe, project_root=tmp_path)
        support_findings = [
            f for f in report.findings
            if f.kind == FindingKind.MISSING_SUPPORT_DATASET
        ]
        assert len(support_findings) >= 1
        assert "total_population" in support_findings[0].message
        assert not report.is_ready

    def test_valid_population_source_passes(self, tmp_path: Path):
        """When the support dataset has the required field, no
        support-dataset findings should be reported."""
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True)

        pd.DataFrame({
            "coc_id": ["COC1"], "year": [2020], "pit_total": [10],
        }).to_parquet(data_dir / "pit.parquet")

        # Create weights dataset WITH the required population field
        pd.DataFrame({
            "GEOID": ["T1", "T2"],
            "year": [2020, 2020],
            "total_population": [100, 200],
        }).to_parquet(data_dir / "weights.parquet")

        xwalk_dir = data_dir / "curated" / "xwalks"
        xwalk_dir.mkdir(parents=True)
        pd.DataFrame({
            "coc_id": ["COC1", "COC2"],
            "tract_geoid": ["T1", "T2"],
            "area_share": [1.0, 1.0],
        }).to_parquet(xwalk_dir / "xwalk__B2025xT2020.parquet")

        recipe_data = {
            "version": 1,
            "name": "pop-weight-ok",
            "universe": {"range": "2020-2020"},
            "targets": [
                {"id": "coc_panel", "geometry": {"type": "coc", "vintage": 2025}},
            ],
            "datasets": {
                "pit": {
                    "provider": "hud",
                    "product": "pit",
                    "version": 1,
                    "native_geometry": {"type": "coc"},
                    "path": "data/pit.parquet",
                    "years": "2020-2020",
                },
                "weights": {
                    "provider": "census",
                    "product": "acs5",
                    "version": 1,
                    "native_geometry": {"type": "tract", "vintage": 2020},
                    "path": "data/weights.parquet",
                    "years": "2020-2020",
                },
            },
            "transforms": [
                {
                    "id": "tract_to_coc",
                    "type": "crosswalk",
                    "from": {"type": "tract", "vintage": 2020},
                    "to": {"type": "coc", "vintage": 2025},
                    "spec": {
                        "weighting": {
                            "scheme": "population",
                            "population_source": "weights",
                            "population_field": "total_population",
                        },
                    },
                },
            ],
            "pipelines": [
                {
                    "id": "main",
                    "target": "coc_panel",
                    "steps": [
                        {"materialize": {"transforms": ["tract_to_coc"]}},
                        {
                            "resample": {
                                "dataset": "pit",
                                "to_geometry": {"type": "coc", "vintage": 2025},
                                "method": "identity",
                                "measures": ["pit_total"],
                            },
                        },
                        {
                            "join": {
                                "datasets": ["pit"],
                                "join_on": ["geo_id", "year"],
                            },
                        },
                    ],
                },
            ],
        }

        recipe = load_recipe(recipe_data)
        report = run_preflight(recipe, project_root=tmp_path)
        support_findings = [
            f for f in report.findings
            if f.kind == FindingKind.MISSING_SUPPORT_DATASET
        ]
        assert len(support_findings) == 0

    def test_area_weighting_skips_support_check(self, tmp_path: Path):
        """Transforms with scheme=area should not trigger support-dataset
        probes."""
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True)

        pd.DataFrame({
            "coc_id": ["COC1"], "year": [2020], "pit_total": [10],
        }).to_parquet(data_dir / "pit.parquet")

        xwalk_dir = data_dir / "curated" / "xwalks"
        xwalk_dir.mkdir(parents=True)
        pd.DataFrame({
            "coc_id": ["COC1", "COC2"],
            "tract_geoid": ["T1", "T2"],
            "area_share": [1.0, 1.0],
        }).to_parquet(xwalk_dir / "xwalk__B2025xT2020.parquet")

        recipe_data = {
            "version": 1,
            "name": "area-weight-test",
            "universe": {"range": "2020-2020"},
            "targets": [
                {"id": "coc_panel", "geometry": {"type": "coc", "vintage": 2025}},
            ],
            "datasets": {
                "pit": {
                    "provider": "hud",
                    "product": "pit",
                    "version": 1,
                    "native_geometry": {"type": "coc"},
                    "path": "data/pit.parquet",
                    "years": "2020-2020",
                },
            },
            "transforms": [
                {
                    "id": "tract_to_coc",
                    "type": "crosswalk",
                    "from": {"type": "tract", "vintage": 2020},
                    "to": {"type": "coc", "vintage": 2025},
                    "spec": {"weighting": {"scheme": "area"}},
                },
            ],
            "pipelines": [
                {
                    "id": "main",
                    "target": "coc_panel",
                    "steps": [
                        {"materialize": {"transforms": ["tract_to_coc"]}},
                        {
                            "resample": {
                                "dataset": "pit",
                                "to_geometry": {"type": "coc", "vintage": 2025},
                                "method": "identity",
                                "measures": ["pit_total"],
                            },
                        },
                        {
                            "join": {
                                "datasets": ["pit"],
                                "join_on": ["geo_id", "year"],
                            },
                        },
                    ],
                },
            ],
        }

        recipe = load_recipe(recipe_data)
        report = run_preflight(recipe, project_root=tmp_path)
        support_findings = [
            f for f in report.findings
            if f.kind == FindingKind.MISSING_SUPPORT_DATASET
        ]
        assert len(support_findings) == 0


# ---------------------------------------------------------------------------
# Probe unit tests for new probes (coclab-pu6j.2)
# ---------------------------------------------------------------------------


class TestGetWeightedTransformRequirements:

    def test_population_weighted(self):
        from hhplab.recipe.probes import get_weighted_transform_requirements
        from hhplab.recipe.recipe_schema import (
            CrosswalkSpec,
            CrosswalkTransform,
            CrosswalkWeighting,
            GeometryRef,
        )

        t = CrosswalkTransform(
            id="t1",
            **{"from": GeometryRef(type="tract", vintage=2020)},
            to=GeometryRef(type="coc", vintage=2025),
            spec=CrosswalkSpec(
                weighting=CrosswalkWeighting(
                    scheme="population",
                    population_source="weights",
                    population_field="total_pop",
                ),
            ),
        )
        result = get_weighted_transform_requirements(t)
        assert result == ("weights", "total_pop")

    def test_area_weighted_returns_none(self):
        from hhplab.recipe.probes import get_weighted_transform_requirements
        from hhplab.recipe.recipe_schema import (
            CrosswalkSpec,
            CrosswalkTransform,
            CrosswalkWeighting,
            GeometryRef,
        )

        t = CrosswalkTransform(
            id="t1",
            **{"from": GeometryRef(type="tract", vintage=2020)},
            to=GeometryRef(type="coc", vintage=2025),
            spec=CrosswalkSpec(
                weighting=CrosswalkWeighting(scheme="area"),
            ),
        )
        result = get_weighted_transform_requirements(t)
        assert result is None

    @pytest.mark.parametrize(
        "weighting",
        [
            {"scheme": "population"},
            {"scheme": "population", "population_source": "weights"},
            {"scheme": "population", "population_field": "total_pop"},
        ],
        ids=["missing-both", "missing-field", "missing-source"],
    )
    def test_population_weighting_requires_explicit_source_and_field(self, weighting):
        from pydantic import ValidationError

        from hhplab.recipe.recipe_schema import CrosswalkWeighting

        with pytest.raises(ValidationError, match="scheme='population' requires"):
            CrosswalkWeighting(**weighting)

    def test_rollup_transform_returns_none(self):
        from hhplab.recipe.probes import get_weighted_transform_requirements
        from hhplab.recipe.recipe_schema import (
            GeometryRef,
            RollupKeys,
            RollupSpec,
            RollupTransform,
        )

        t = RollupTransform(
            id="r1",
            **{"from": GeometryRef(type="county")},
            to=GeometryRef(type="state"),
            spec=RollupSpec(
                keys=RollupKeys(from_key="county_fips", to_key="state_fips"),
            ),
        )
        result = get_weighted_transform_requirements(t)
        assert result is None


# ===========================================================================
# Preflight validation coverage tests (coclab-51le)
# ===========================================================================


class TestPreflightPlannerErrors:
    """Test that preflight surfaces planner errors properly."""

    def test_planner_error_for_missing_segment(self, tmp_path: Path):
        """A recipe with years outside segment coverage raises PLANNER_ERROR."""
        data = _preflight_recipe(with_path=True)
        # Narrow ACS segments so 2022 is uncovered
        data["datasets"]["acs"]["years"] = "2020-2021"
        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)
        planner_findings = [
            f for f in report.findings if f.kind == FindingKind.PLANNER_ERROR
        ]
        assert planner_findings
        assert "2022" in planner_findings[0].message

    def test_join_referencing_unresampled_dataset_still_preflights(self, tmp_path: Path):
        """A join step that references a dataset not resampled in this pipeline
        should not crash preflight — the planner proceeds, and path/schema
        probes catch the issue downstream."""
        data = _preflight_recipe(with_path=True, identity_only=True)
        _setup_preflight_fixtures(tmp_path, include_xwalk=False, include_acs=False)
        # Add second dataset NOT in the pipeline steps
        data["datasets"]["extra"] = {
            "provider": "test",
            "product": "test",
            "version": 1,
            "native_geometry": {"type": "coc"},
            "years": "2020-2022",
        }
        # Reference it in the join
        data["pipelines"][0]["steps"][-1]["join"]["datasets"] = ["pit", "extra"]
        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)
        # Should not crash — preflight produces a report even if the join
        # references something that wasn't resampled
        assert report is not None
