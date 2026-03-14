"""Frozen panel builder for the Glynn-Fox claims audit."""

from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from coclab import naming
from coclab.metro.acs import aggregate_acs_to_metro
from coclab.metro.io import (
    read_metro_coc_membership,
    read_metro_county_membership,
)
from coclab.metro.zori import aggregate_yearly_zori_to_metro
from coclab.provenance import ProvenanceBlock, write_parquet_with_provenance

METRO_DEFINITION_VERSION = "glynn_fox_v1"

RAW_REQUIRED_COLUMNS: list[str] = [
    "geo_id",
    "year",
    "pit_total",
    "pit_sheltered",
    "pit_unsheltered",
    "total_population",
    "median_household_income",
    "zori",
]

MODELING_READY_COLUMNS: list[str] = [
    "geo_id",
    "year",
    "pit_sheltered",
    "pit_unsheltered",
    "C_it",
    "N_it",
    "ZRI_it",
    "d_zori",
    "expected_pi",
    "a_it",
    "b_it",
]

HEADLINE_METRO_IDS: list[str] = ["GF01", "GF02", "GF07", "GF10", "GF11", "GF15"]
AUDIT_PANEL_VERSION = "v1"
EXPECTED_PI = 0.60
BETA_VARIANCE = 0.01
METRO_SOURCE_PATH = "outputs/audit_panels/_sources/glynn_fox_metro_audit_source_2015_2024.parquet"


@dataclass(frozen=True)
class AuditPanelSpec:
    panel_name: str
    workload_id: str
    unit_type: str
    source_panel_path: str
    selection_rule: str
    missing_policy: str
    rent_proxy: str
    notes: str
    selected_geo_ids: tuple[str, ...] | None = None


AUDIT_PANEL_SPECS: tuple[AuditPanelSpec, ...] = (
    AuditPanelSpec(
        panel_name="glynn_fox_broad_metro_v1",
        workload_id="A",
        unit_type="metro",
        source_panel_path="data/curated/panel/panel__metro__Y2015-2024@Dglynnfoxv1.parquet",
        selection_rule=(
            "All Glynn/Fox metros from definition glynn_fox_v1 with complete "
            "2015-2024 coverage and valid audit-required fields."
        ),
        missing_policy="drop",
        rent_proxy="zori_january",
        notes=(
            "Primary metro claims-audit panel built from PIT + ACS + ZORI. "
            "Window starts in 2015 because the current curated ZORI artifact "
            "begins in January 2015."
        ),
    ),
    AuditPanelSpec(
        panel_name="glynn_fox_headline_metro_v1",
        workload_id="B",
        unit_type="metro",
        source_panel_path="data/curated/panel/panel__metro__Y2015-2024@Dglynnfoxv1.parquet",
        selection_rule=(
            "Subset of the broad metro audit panel restricted to the headline "
            "metros emphasized in the paper narrative: New York, Los Angeles, "
            "Washington, DC, Seattle, San Francisco, and Boston."
        ),
        missing_policy="drop",
        rent_proxy="zori_january",
        notes="Headline metro subset aligned to Workload A.",
        selected_geo_ids=tuple(HEADLINE_METRO_IDS),
    ),
    AuditPanelSpec(
        panel_name="glynn_fox_coc_robustness_v1",
        workload_id="C",
        unit_type="coc",
        source_panel_path="data/curated/panel/panel__Y2015-2024@B2025.parquet",
        selection_rule=(
            "All CoCs from the 2015-2024 CoC panel that retain complete "
            "coverage and satisfy audit validity checks after dropping rows "
            "with missing required values and removing units with remaining "
            "panel-invalid rows."
        ),
        missing_policy="drop",
        rent_proxy="zori_january",
        notes="Robustness panel derived from the current CoC-wide panel.",
    ),
    AuditPanelSpec(
        panel_name="glynn_fox_reuse_broad_metro_v1",
        workload_id="D",
        unit_type="metro",
        source_panel_path="data/curated/panel/panel__metro__Y2015-2024@Dglynnfoxv1.parquet",
        selection_rule=(
            "Exact copy of Workload A for cross-method reuse comparisons. "
            "No unit or year differences are allowed."
        ),
        missing_policy="drop",
        rent_proxy="zori_january",
        notes="Cross-method reuse panel; data must match Workload A exactly.",
    ),
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _git_commit(project_root: Path) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=project_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return "unknown"
    return result.stdout.strip()


def _beta_parameters(expected_pi: float, variance: float) -> tuple[float, float]:
    common = expected_pi * (1.0 - expected_pi) / variance - 1.0
    alpha = expected_pi * common
    beta = (1.0 - expected_pi) * common
    return alpha, beta


def _load_source_panel(project_root: Path, spec: AuditPanelSpec) -> tuple[pd.DataFrame, str]:
    path = project_root / spec.source_panel_path
    if not path.exists() and spec.unit_type == "metro":
        path = _ensure_metro_source_panel(project_root)
    df = pd.read_parquet(path)
    if "population" in df.columns and "total_population" not in df.columns:
        df = df.rename(columns={"population": "total_population"})
    return df, str(path.relative_to(project_root))


def _acs_path_for_year(project_root: Path, year: int) -> Path:
    tract_vintage = 2010 if year <= 2019 else 2020
    return (
        project_root
        / "data"
        / "curated"
        / "acs"
        / f"acs5_tracts__A{year}xT{tract_vintage}.parquet"
    )


def _build_metro_source_panel(project_root: Path) -> pd.DataFrame:
    years = list(range(2015, 2025))

    pit = pd.read_parquet(project_root / "data" / "curated" / "pit" / "pit_vintage__P2024.parquet")
    pit = pit[pit["pit_year"].isin(years)].copy()
    coc_membership = read_metro_coc_membership(base_dir=project_root / "data")
    metro_pit = (
        pit.merge(coc_membership[["metro_id", "coc_id"]], on="coc_id", how="inner")
        .groupby(["metro_id", "pit_year"], as_index=False)[
            ["pit_total", "pit_sheltered", "pit_unsheltered"]
        ]
        .sum()
        .rename(columns={"pit_year": "year"})
    )

    zori = pd.read_parquet(project_root / "data" / "curated" / "zori" / "zori__county__Z2026.parquet")
    zori = zori[zori["month"] == 1].copy()
    county_membership = read_metro_county_membership(base_dir=project_root / "data")

    metro_acs_frames: list[pd.DataFrame] = []
    county_pop_frames: list[pd.DataFrame] = []
    for year in years:
        acs = pd.read_parquet(_acs_path_for_year(project_root, year)).copy()
        acs_for_metro = acs.rename(columns={"tract_geoid": "GEOID"})
        metro_acs = aggregate_acs_to_metro(acs_for_metro)
        metro_acs["year"] = year
        metro_acs_frames.append(
            metro_acs[
                [
                    "metro_id",
                    "year",
                    "total_population",
                    "adult_population",
                    "population_below_poverty",
                    "median_household_income",
                    "median_gross_rent",
                ]
            ].copy()
        )

        county_pop = (
            acs.assign(county_fips=acs["tract_geoid"].str[:5])
            .groupby("county_fips", as_index=False)["total_population"]
            .sum()
            .rename(columns={"total_population": "population"})
        )
        county_pop["year"] = year
        county_pop_frames.append(county_pop)

    metro_acs_df = pd.concat(metro_acs_frames, ignore_index=True)

    # Build yearly county ZORI (January) and population tables, then
    # delegate to the reusable population-weighted aggregator.
    zori_yearly = (
        zori[zori["year"].isin(years)][["geo_id", "year", "zori"]]
        .rename(columns={"geo_id": "county_fips"})
    )
    county_pop_df = pd.concat(county_pop_frames, ignore_index=True)
    metro_zori_df = aggregate_yearly_zori_to_metro(
        zori_yearly,
        county_pop_df,
        county_membership_df=county_membership,
    )
    panel = metro_pit.merge(metro_acs_df, on=["metro_id", "year"], how="inner")
    panel = panel.merge(metro_zori_df, on=["metro_id", "year"], how="inner")
    panel["geo_id"] = panel["metro_id"]
    return panel.sort_values(["geo_id", "year"]).reset_index(drop=True)


def _ensure_metro_source_panel(project_root: Path) -> Path:
    path = project_root / METRO_SOURCE_PATH
    if path.exists():
        return path
    df = _build_metro_source_panel(project_root)
    provenance = ProvenanceBlock(
        geo_type="metro",
        definition_version="glynn_fox_v1",
        extra={
            "dataset_type": "glynn_fox_audit_source_panel",
            "years": "2015-2024",
        },
    )
    write_parquet_with_provenance(df, path, provenance)
    return path


def _prepare_raw_panel(
    source_df: pd.DataFrame,
    *,
    spec: AuditPanelSpec,
) -> tuple[pd.DataFrame, dict[str, list[str]], pd.DataFrame]:
    df = source_df.copy()
    if spec.selected_geo_ids is not None:
        df = df[df["geo_id"].isin(spec.selected_geo_ids)].copy()

    pre_filter_df = df.copy()
    drop_reasons: dict[str, list[str]] = {}

    required_non_geo = [c for c in RAW_REQUIRED_COLUMNS if c != "geo_id"]
    missing_mask = df[required_non_geo].isna().any(axis=1)
    if missing_mask.any():
        for geo_id in sorted(df.loc[missing_mask, "geo_id"].unique()):
            drop_reasons.setdefault(geo_id, []).append("missing_required_values")
        df = df.loc[~missing_mask].copy()

    bad_bound_mask = df["pit_total"] > df["total_population"]
    if bad_bound_mask.any():
        for geo_id in sorted(df.loc[bad_bound_mask, "geo_id"].unique()):
            drop_reasons.setdefault(geo_id, []).append("pit_exceeds_population")

    bad_identity_mask = df["pit_total"] != (
        df["pit_sheltered"] + df["pit_unsheltered"]
    )
    if bad_identity_mask.any():
        for geo_id in sorted(df.loc[bad_identity_mask, "geo_id"].unique()):
            drop_reasons.setdefault(geo_id, []).append("pit_identity_violation")

    if drop_reasons:
        df = df.loc[~df["geo_id"].isin(drop_reasons)].copy()

    year_counts = df.groupby("geo_id")["year"].nunique()
    expected_year_count = df["year"].nunique()
    incomplete_geos = sorted(year_counts[year_counts != expected_year_count].index.tolist())
    if incomplete_geos:
        for geo_id in incomplete_geos:
            drop_reasons.setdefault(geo_id, []).append("incomplete_year_coverage")
        df = df.loc[~df["geo_id"].isin(incomplete_geos)].copy()

    df = df[RAW_REQUIRED_COLUMNS].sort_values(["geo_id", "year"]).reset_index(drop=True)
    return df, drop_reasons, pre_filter_df


def _validate_raw_panel(
    raw_df: pd.DataFrame,
    *,
    spec: AuditPanelSpec,
    drop_reasons: dict[str, list[str]],
) -> dict[str, Any]:
    issues: list[dict[str, Any]] = []
    passed_checks: list[str] = []

    missing_cols = sorted(set(RAW_REQUIRED_COLUMNS) - set(raw_df.columns))
    if missing_cols:
        issues.append({"check": "required_columns", "missing_columns": missing_cols})
    else:
        passed_checks.append("required_columns")

    duplicate_count = int(raw_df.duplicated(["geo_id", "year"]).sum())
    if duplicate_count:
        issues.append({"check": "unique_geo_year", "duplicate_count": duplicate_count})
    else:
        passed_checks.append("unique_geo_year")

    sorted_ok = raw_df.equals(raw_df.sort_values(["geo_id", "year"]).reset_index(drop=True))
    if not sorted_ok:
        issues.append({"check": "sorted", "message": "panel must be sorted by geo_id, year"})
    else:
        passed_checks.append("sorted")

    year_counts = raw_df.groupby("geo_id")["year"].nunique()
    contiguous_ok = not year_counts.empty and year_counts.nunique() == 1
    if not contiguous_ok:
        issues.append({"check": "contiguity", "message": "units have uneven year counts"})
    else:
        passed_checks.append("contiguity")

    numeric_cols = [
        "pit_total",
        "pit_sheltered",
        "pit_unsheltered",
        "total_population",
        "zori",
    ]
    nonnegative_counts = {
        col: int((raw_df[col] < 0).sum())
        for col in numeric_cols
    }
    if any(nonnegative_counts.values()):
        issues.append({"check": "nonnegative", "counts": nonnegative_counts})
    else:
        passed_checks.append("nonnegative")

    identity_bad = int(
        (raw_df["pit_total"] != (raw_df["pit_sheltered"] + raw_df["pit_unsheltered"])).sum()
    )
    if identity_bad:
        issues.append({"check": "pit_identity", "bad_row_count": identity_bad})
    else:
        passed_checks.append("pit_identity")

    bound_bad = int((raw_df["pit_total"] > raw_df["total_population"]).sum())
    if bound_bad:
        issues.append({"check": "population_bound", "bad_row_count": bound_bad})
    else:
        passed_checks.append("population_bound")

    missing_after_policy = {
        col: int(raw_df[col].isna().sum())
        for col in RAW_REQUIRED_COLUMNS
    }
    if any(missing_after_policy.values()):
        issues.append(
            {
                "check": "missing_policy",
                "message": "drop policy left missing values in required fields",
                "missing_counts": missing_after_policy,
            }
        )
    else:
        passed_checks.append("missing_policy")

    return {
        "panel_name": spec.panel_name,
        "workload_id": spec.workload_id,
        "structurally_valid": not issues,
        "required_checks_passed": not issues,
        "checks_passed": passed_checks,
        "issues": issues,
        "dropped_units": sorted(drop_reasons),
        "drop_reasons": drop_reasons,
    }


def _derive_modeling_ready(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()
    df["C_it"] = df["pit_total"]
    df["N_it"] = df["total_population"]
    df["ZRI_it"] = df["zori"]
    df["d_zori"] = df.groupby("geo_id")["zori"].pct_change()
    df["d_zori"] = df["d_zori"].fillna(0.0)
    df["expected_pi"] = EXPECTED_PI
    alpha, beta = _beta_parameters(EXPECTED_PI, BETA_VARIANCE)
    df["a_it"] = alpha
    df["b_it"] = beta
    return df[MODELING_READY_COLUMNS].copy()


def _validate_modeling_ready(
    modeling_df: pd.DataFrame,
    *,
    raw_validation: dict[str, Any],
) -> dict[str, Any]:
    issues = list(raw_validation["issues"])
    passed_checks = list(raw_validation["checks_passed"])

    missing_cols = sorted(set(MODELING_READY_COLUMNS) - set(modeling_df.columns))
    if missing_cols:
        issues.append({"check": "modeling_columns", "missing_columns": missing_cols})
    else:
        passed_checks.append("modeling_columns")

    required_no_missing = ["C_it", "N_it", "ZRI_it", "a_it", "b_it"]
    missing_counts = {col: int(modeling_df[col].isna().sum()) for col in required_no_missing}
    if any(missing_counts.values()):
        issues.append({"check": "modeling_missing", "missing_counts": missing_counts})
    else:
        passed_checks.append("modeling_missing")

    year_counts = modeling_df.groupby("geo_id")["year"].nunique()
    balanced_ok = not year_counts.empty and year_counts.nunique() == 1
    if not balanced_ok:
        issues.append({"check": "balanced_modeling_panel", "message": "modeling table is not balanced"})
    else:
        passed_checks.append("balanced_modeling_panel")

    first_year_zero_ok = bool((modeling_df.groupby("geo_id")["d_zori"].first() == 0.0).all())
    if not first_year_zero_ok:
        issues.append({"check": "d_zori_first_year", "message": "first modeled year must use 0.0"})
    else:
        passed_checks.append("d_zori_first_year")

    return {
        "structurally_valid": not issues,
        "checks_passed": passed_checks,
        "issues": issues,
        "n_rows_model_ready": int(len(modeling_df)),
        "n_units_model_ready": int(modeling_df["geo_id"].nunique()),
    }


def _copy_metro_reference_artifacts(
    *,
    project_root: Path,
    output_dir: Path,
    definition_version: str = METRO_DEFINITION_VERSION,
) -> list[str]:
    """Copy metro definition artifacts into the audit output bundle.

    Returns the list of filenames copied (empty if source files are missing).
    """
    data_root = project_root / "data"
    source_paths = [
        naming.metro_definitions_path(definition_version, data_root),
        naming.metro_coc_membership_path(definition_version, data_root),
        naming.metro_county_membership_path(definition_version, data_root),
    ]
    output_dir.mkdir(parents=True, exist_ok=True)
    copied: list[str] = []
    for src in source_paths:
        if src.exists():
            shutil.copy2(src, output_dir / src.name)
            copied.append(src.name)
    return copied


def _write_panel_artifacts(
    *,
    project_root: Path,
    spec: AuditPanelSpec,
    raw_df: pd.DataFrame,
    modeling_df: pd.DataFrame,
    raw_validation: dict[str, Any],
    modeling_validation: dict[str, Any],
    pre_filter_df: pd.DataFrame,
    source_panel_path: str,
) -> dict[str, Any]:
    output_dir = project_root / "outputs" / "audit_panels" / spec.panel_name
    raw_path = output_dir / "raw_panel.parquet"
    modeling_path = output_dir / "modeling_input.parquet"
    validation_path = output_dir / "validation_report.json"
    manifest_path = output_dir / "panel_manifest.json"

    timestamp = datetime.now(UTC).isoformat()
    git_commit = _git_commit(project_root)

    provenance = ProvenanceBlock(
        geo_type=spec.unit_type,
        definition_version="glynn_fox_v1" if spec.unit_type == "metro" else None,
        boundary_vintage="2025" if spec.unit_type == "coc" else None,
        extra={
            "panel_name": spec.panel_name,
            "workload_id": spec.workload_id,
            "panel_version": AUDIT_PANEL_VERSION,
            "source_panel_path": source_panel_path,
        },
    )
    write_parquet_with_provenance(raw_df, raw_path, provenance)
    write_parquet_with_provenance(modeling_df, modeling_path, provenance)

    # Include metro definition reference artifacts for metro-based outputs.
    metro_ref_artifacts: list[str] = []
    if spec.unit_type == "metro":
        metro_ref_artifacts = _copy_metro_reference_artifacts(
            project_root=project_root,
            output_dir=output_dir,
        )

    artifacts_present: dict[str, bool] = {
        "raw_panel": raw_path.exists(),
        "modeling_input": modeling_path.exists(),
        "validation_report": True,
        "panel_manifest": True,
    }
    for artifact_name in metro_ref_artifacts:
        artifacts_present[artifact_name] = True

    validation_report = {
        "panel_name": spec.panel_name,
        "workload_id": spec.workload_id,
        "raw_validation": raw_validation,
        "modeling_validation": modeling_validation,
        "artifacts_present": artifacts_present,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    validation_path.write_text(json.dumps(validation_report, indent=2), encoding="utf-8")

    manifest = {
        "panel_name": spec.panel_name,
        "workload_id": spec.workload_id,
        "unit_type": spec.unit_type,
        "panel_version": AUDIT_PANEL_VERSION,
        "selection_rule": spec.selection_rule,
        "source_panel_path": source_panel_path,
        "derived_panel_path": str(modeling_path.relative_to(project_root)),
        "start_year": int(raw_df["year"].min()),
        "end_year": int(raw_df["year"].max()),
        "missing_policy": spec.missing_policy,
        "balanced_required": True,
        "rent_proxy": spec.rent_proxy,
        "build_timestamp": timestamp,
        "git_commit": git_commit,
        "n_rows_raw": int(len(raw_df)),
        "n_rows_model_ready": int(len(modeling_df)),
        "n_units_raw": int(raw_df["geo_id"].nunique()),
        "n_units_model_ready": int(modeling_df["geo_id"].nunique()),
        "dropped_units": raw_validation["dropped_units"],
        "drop_reasons": raw_validation["drop_reasons"],
        "aggregation_logic_ref": "background/audit_panel_specs.md",
        "validation_report_path": str(validation_path.relative_to(project_root)),
        "notes": spec.notes,
        "count_accuracy": {
            "trajectory": "constant",
            "expected_pi": EXPECTED_PI,
            "beta_variance": BETA_VARIANCE,
        },
        "n_units_pre_filter": int(pre_filter_df["geo_id"].nunique()),
    }
    if metro_ref_artifacts:
        manifest["metro_reference_artifacts"] = metro_ref_artifacts
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def build_audit_panels(project_root: Path | None = None) -> list[dict[str, Any]]:
    if project_root is None:
        project_root = _repo_root()
    project_root = Path(project_root)

    manifests: list[dict[str, Any]] = []
    for spec in AUDIT_PANEL_SPECS:
        source_df, source_panel_path = _load_source_panel(project_root, spec)
        raw_df, drop_reasons, pre_filter_df = _prepare_raw_panel(source_df, spec=spec)
        raw_validation = _validate_raw_panel(raw_df, spec=spec, drop_reasons=drop_reasons)
        modeling_df = _derive_modeling_ready(raw_df)
        modeling_validation = _validate_modeling_ready(
            modeling_df,
            raw_validation=raw_validation,
        )
        manifest = _write_panel_artifacts(
            project_root=project_root,
            spec=spec,
            raw_df=raw_df,
            modeling_df=modeling_df,
            raw_validation=raw_validation,
            modeling_validation=modeling_validation,
            pre_filter_df=pre_filter_df,
            source_panel_path=source_panel_path,
        )
        manifests.append(manifest)
    return manifests


if __name__ == "__main__":
    print(json.dumps(build_audit_panels(), indent=2))
