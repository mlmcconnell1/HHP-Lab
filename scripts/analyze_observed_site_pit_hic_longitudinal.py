"""Observed-count longitudinal PIT/HIC checks for the frozen 60-site study.

Run from the repository root with:
``uv run python scripts/analyze_observed_site_pit_hic_longitudinal.py --json``.
The analysis masks source-declared non-observations, retains reported values in
the saved source panel, and uses exact calendar keys for differences and lags.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from collections.abc import Iterable
from pathlib import Path

import numpy as np
import pandas as pd

from hhplab.recipe.executor.inputs import resolve_site_universe_files
from hhplab.sources.hud.pit.observation import observed_pit_mask
from hhplab.storage.provenance import ProvenanceBlock, write_parquet_with_provenance

ROOT = Path(__file__).resolve().parents[1]
OUT = Path(
    "/home/matt/HHP-Output/site-boundary-history-audit/"
    "exploratory-without-sacramento/observed-counts"
)
SITES = Path("/home/matt/HHP-Output/msa_coc_longitudinal_2010_2024_exclude_sacramento_sites")
OLD_SITES = Path("/home/matt/HHP-Output/msa_coc_longitudinal_2010_2024_sites")
AUDIT = Path("/home/matt/HHP-Output/pit-count-types-audit/20261022")
PIT_PATH = ROOT / "data/curated/pit/pit_vintage__P2024.parquet"
PIT_BACKUP = AUDIT / "before/pit_vintage__P2024.parquet"
PIT_PREVIOUS_SHA256 = "a1c3dc50aa6534fd6e468dcbcc0d56ddde8dbb788dcfc97aa5cd025ba6879f21"
MEMBERSHIP_IDENTITY = "d9cb7f511c9b8d4d212a90bff4b854cf5c021dd0c50e956c6219b1cbe1e660ec"
YEARS = tuple(year for year in range(2010, 2025) if year != 2021)
LAGS = (-2, -1, 0, 1, 2)
BOOTSTRAPS = 1999
SEED = 20261022
PRIOR_DIR = Path("/home/matt/HHP-Output/site-boundary-history-audit/exploratory-without-sacramento")
PRIOR_LINEAGE = PRIOR_DIR / "lineage.csv"
PRIOR_PANEL = PRIOR_DIR / "exploratory_site_year_panel.parquet"
PIT_MEASURES = ("pit_unsheltered", "pit_total", "pit_sheltered")
HIC_MEASURES = ("hic_shelter_year_round_beds", "hic_psh_year_round_beds")
OBSERVED_PANEL_COLUMNS = [
    "msa_id", "msa_name", "year", "pit_source_ids", "hic_source_ids", "reported_cocs",
    "pit_unsheltered", "pit_total", "pit_sheltered",
    "unsheltered_observed", "total_observed", "sheltered_observed",
    "hic_shelter_year_round_beds", "hic_psh_year_round_beds",
]


def observed_column(measure: str) -> str:
    return f"{measure.removeprefix('pit_')}_observed"


def sha256(path: Path | str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def weighted_two_way_residuals(
    values: np.ndarray,
    site: np.ndarray,
    year: np.ndarray,
    weights: np.ndarray,
) -> np.ndarray:
    """Absorb weighted site effects, then solve the small year-effect system.

    This Schur-complement solve is exact for the weighted least-squares dummy
    variable projection on any unbalanced site/year observation graph. It also
    handles bootstrap frequency weights and disconnected graphs. Sites with
    zero total weight have a zero inverse and contribute no rows.
    """
    values = np.asarray(values, dtype=float)
    site = np.asarray(site, dtype=int)
    year = np.asarray(year, dtype=int)
    weights = np.asarray(weights, dtype=float)
    if values.ndim == 1:
        values = values[:, None]
    if not (len(values) == len(site) == len(year) == len(weights)):
        raise ValueError("values, site, year, and weights must have the same row count")
    if np.any(weights < 0) or not np.isfinite(weights).all():
        raise ValueError("weights must be finite and nonnegative")
    if not len(values) or not np.any(weights > 0):
        return np.zeros_like(values)
    n_sites = int(site.max()) + 1
    n_years = int(year.max()) + 1
    cell_weights = np.bincount(
        site * n_years + year, weights=weights, minlength=n_sites * n_years
    ).reshape(n_sites, n_years)
    site_totals = cell_weights.sum(axis=1)
    inverse_site_totals = np.divide(
        1.0, site_totals, out=np.zeros_like(site_totals), where=site_totals > 0
    )
    site_sums = np.column_stack([
        np.bincount(site, weights=weights * values[:, column], minlength=n_sites)
        for column in range(values.shape[1])
    ])
    site_means = inverse_site_totals[:, None] * site_sums
    within_site = values - site_means[site]

    year_totals = cell_weights.sum(axis=0)
    year_system = np.diag(year_totals) - cell_weights.T @ (
        cell_weights * inverse_site_totals[:, None]
    )
    right_hand_side = np.column_stack([
        np.bincount(year, weights=weights * within_site[:, column], minlength=n_years)
        for column in range(values.shape[1])
    ])
    year_effects = np.linalg.lstsq(year_system, right_hand_side, rcond=None)[0]
    site_year_effect_means = cell_weights @ year_effects
    site_year_effect_means *= inverse_site_totals[:, None]
    return within_site - year_effects[year] + site_year_effect_means[site]


def weighted_correlation(
    values: np.ndarray, site: np.ndarray, year: np.ndarray, weights: np.ndarray,
    *, fixed_effects: bool,
) -> float:
    values = np.asarray(values, dtype=float)
    if fixed_effects:
        raw_means = (
            np.average(values, axis=0, weights=weights)
            if np.sum(weights) > 0 else np.zeros(values.shape[1])
        )
        centered = values - raw_means
        raw_scale = np.sum(weights[:, None] * np.square(centered), axis=0)
        values = weighted_two_way_residuals(values, site, year, weights)
        residual_scale = np.sum(weights[:, None] * np.square(values), axis=0)
        # Treat exact additive outcomes as undefined despite least-squares
        # roundoff. The relative threshold follows the solve accuracy scale.
        if np.any(residual_scale <= (10e-11) ** 2 * np.maximum(raw_scale, 1.0)):
            return float("nan")
    else:
        total_weight = float(np.sum(weights))
        if total_weight <= 0:
            return float("nan")
        means = np.sum(weights[:, None] * values, axis=0) / total_weight
        values = values - means
    cross = values.T @ (weights[:, None] * values)
    denom = float(np.sqrt(cross[0, 0] * cross[1, 1]))
    scale = float(np.sum(weights[:, None] * np.square(values)))
    if denom <= 1e-24 * max(scale, 1.0):
        return float("nan")
    return float(cross[0, 1] / denom)


def exact_annual_changes(panel: pd.DataFrame, value_columns: Iterable[str]) -> pd.DataFrame:
    """Create t minus t-1 changes only where both exact annual keys exist."""
    columns = list(value_columns)
    current = panel[["msa_id", "year", *columns]].copy()
    previous = panel[["msa_id", "year", *columns]].copy()
    previous["year"] += 1
    previous = previous.rename(columns={column: f"previous_{column}" for column in columns})
    changes = current.merge(previous, on=["msa_id", "year"], how="inner", validate="one_to_one")
    changes = changes.loc[changes[columns].notna().all(axis=1)].copy()
    changes = changes.loc[
        changes[[f"previous_{column}" for column in columns]].notna().all(axis=1)
    ].copy()
    for column in columns:
        changes[column] = np.log1p(changes[column].astype(float)) - np.log1p(
            changes[f"previous_{column}"].astype(float)
        )
    changes["previous_year"] = changes.year - 1
    return changes


def exact_lag_pairs(
    predictors: pd.DataFrame,
    outcomes: pd.DataFrame,
    *,
    predictor_column: str,
    outcome_column: str,
    lag: int,
    predictor_year: str = "year",
    outcome_year: str = "year",
) -> pd.DataFrame:
    """Pair predictor at t with outcome at t+lag, using exact calendar keys."""
    left = predictors[["msa_id", predictor_year, predictor_column]].rename(
        columns={predictor_year: "beds_year"}
    )
    left["outcome_year"] = left.beds_year + lag
    right = outcomes[["msa_id", outcome_year, outcome_column]].rename(
        columns={outcome_year: "outcome_year"}
    )
    matched = left.merge(right, on=["msa_id", "outcome_year"], how="inner", validate="one_to_one")
    matched["lag"] = lag
    return matched


def estimator(
    matched: pd.DataFrame,
    predictor: str,
    outcome: str,
    *,
    fixed_effects: bool,
    rng: np.random.Generator,
    universe_sites: tuple[str, ...],
    bootstrap_replicates: int = BOOTSTRAPS,
) -> dict[str, object]:
    """Estimate adjusted correlation and whole-site bootstrap interval."""
    values = matched[[predictor, outcome]].to_numpy(dtype=float)
    universe_index = {site_id: index for index, site_id in enumerate(universe_sites)}
    site = matched.msa_id.map(universe_index).to_numpy(dtype=int)
    active_sites = np.unique(site)
    compact_site = pd.factorize(matched.msa_id, sort=True)[0]
    year, years = pd.factorize(matched.outcome_year, sort=True)
    weights = np.ones(len(matched), dtype=float)
    point = weighted_correlation(values, site, year, weights, fixed_effects=fixed_effects)
    if fixed_effects:
        design = np.column_stack([
            np.eye(int(compact_site.max()) + 1)[compact_site],
            np.eye(int(year.max()) + 1)[year],
        ])
        explicit_residual = values - design @ np.linalg.lstsq(design, values, rcond=None)[0]
        projected_residual = weighted_two_way_residuals(values, compact_site, year, weights)
        np.testing.assert_allclose(projected_residual, explicit_residual, atol=1e-8)
    bootstrap = []
    bootstrap_degenerate = 0
    bootstrap_projection_failures = 0
    bootstrap_validation_failures = 0
    for _ in range(bootstrap_replicates):
        frequencies = np.bincount(
            rng.choice(active_sites, size=len(active_sites), replace=True),
            minlength=len(universe_sites),
        )
        try:
            estimate = weighted_correlation(
                values, site, year, frequencies[site], fixed_effects=fixed_effects
            )
        except RuntimeError:
            bootstrap_projection_failures += 1
            continue
        if np.isfinite(estimate):
            bootstrap.append(estimate)
        else:
            bootstrap_degenerate += 1
    validation_draw = rng.choice(active_sites, size=len(active_sites), replace=True)
    validation_frequency = np.bincount(validation_draw, minlength=len(universe_sites))
    weighted_validation = weighted_correlation(
        values, site, year, validation_frequency[site], fixed_effects=fixed_effects
    )
    duplicate_frames = []
    for draw_id, source_site in enumerate(validation_draw):
        copy = matched.loc[site == source_site].copy()
        copy["msa_id"] = f"bootstrap-draw-{draw_id}"
        duplicate_frames.append(copy)
    duplicated = pd.concat(duplicate_frames, ignore_index=True)
    duplicate_values = duplicated[[predictor, outcome]].to_numpy(dtype=float)
    duplicate_site = pd.factorize(duplicated.msa_id, sort=True)[0]
    duplicate_year = pd.factorize(duplicated.outcome_year, sort=True)[0]
    duplicate_validation = weighted_correlation(
        duplicate_values, duplicate_site, duplicate_year,
        np.ones(len(duplicated)), fixed_effects=fixed_effects,
    )
    if np.isfinite(weighted_validation) and np.isfinite(duplicate_validation):
        np.testing.assert_allclose(weighted_validation, duplicate_validation, atol=1e-9)
        frequency_validation_passed = True
    else:
        frequency_validation_passed = (
            not np.isfinite(weighted_validation)
            and not np.isfinite(duplicate_validation)
        )
    if not frequency_validation_passed:
        bootstrap_validation_failures += 1
        raise AssertionError("Frequency-weight bootstrap differs from duplicated-site refit")
    # Site deletion is computed on the same observed and lag-specific rows.
    loo = [
        weighted_correlation(values, site, year, (site != omitted).astype(float),
                             fixed_effects=fixed_effects)
        for omitted in active_sites
    ]
    finite_loo = [value for value in loo if np.isfinite(value)]
    minimum_valid = int(np.ceil(.95 * bootstrap_replicates))
    interval_defined = len(bootstrap) >= minimum_valid
    return {
        "n_pairs": int(len(matched)), "n_sites": int(len(active_sites)),
        "bootstrap_universe_sites": int(len(universe_sites)),
        "bootstrap_draw_sites": int(len(active_sites)),
        "n_outcome_years": int(len(years)),
        "pearson_r": float(point) if np.isfinite(point) else None,
        "point_estimate_defined": bool(np.isfinite(point)),
        "site_bootstrap_low": float(np.quantile(bootstrap, .025)) if interval_defined else None,
        "site_bootstrap_high": float(np.quantile(bootstrap, .975)) if interval_defined else None,
        "bootstrap_interval_defined": bool(interval_defined),
        "bootstrap_minimum_valid_replicates": minimum_valid,
        "bootstrap_replicates_requested": int(bootstrap_replicates),
        "bootstrap_replicates_defined": int(len(bootstrap)),
        "bootstrap_replicates_degenerate": int(bootstrap_degenerate),
        "bootstrap_projection_failures": int(bootstrap_projection_failures),
        "bootstrap_validation_failures": int(bootstrap_validation_failures),
        "bootstrap_frequency_matches_duplicated_sample": frequency_validation_passed,
        "leave_one_site_out_min": float(min(finite_loo)) if finite_loo else None,
        "leave_one_site_out_max": float(max(finite_loo)) if finite_loo else None,
        "leave_one_site_out_defined": int(len(finite_loo)),
    }


def _load_frozen_members():
    membership_path = SITES / "msa_coc_memberships__longitudinal_sites.parquet"
    resolution = resolve_site_universe_files(
        artifact_id="longitudinal_sites", threshold=.99,
        sites_path=SITES / "msa_coc_sites__longitudinal_sites.parquet",
        memberships_path=membership_path,
    )
    members = pd.read_parquet(membership_path)
    members = members.loc[(members.threshold == .99) & members.site_eligible].copy()
    old = pd.read_parquet(OLD_SITES / "msa_coc_memberships__longitudinal_sites.parquet")
    old = old.loc[(old.threshold == .99) & old.site_eligible]
    pairs = set(members[["msa_id", "coc_id"]].itertuples(index=False, name=None))
    old_pairs = set(old[["msa_id", "coc_id"]].itertuples(index=False, name=None))
    assert pairs == {pair for pair in old_pairs if pair[0] != "40900"}
    assert resolution.membership_identity == MEMBERSHIP_IDENTITY
    assert members.msa_id.nunique() == 60 and members.coc_id.nunique() == 77
    assert not members.coc_id.duplicated().any()
    return members, resolution, membership_path


def _check_pit_refresh_bridge(current: pd.DataFrame) -> dict[str, object]:
    old = pd.read_parquet(PIT_BACKUP)
    keys = ["pit_year", "coc_id"]
    common_columns = [
        column for column in old.columns
        if column in current.columns and column != "ingested_at"
    ]
    left = old[common_columns].sort_values(keys).reset_index(drop=True)
    right = current[common_columns].sort_values(keys).reset_index(drop=True)
    pd.testing.assert_frame_equal(left, right, check_dtype=False, check_exact=True)
    assert sha256(PIT_BACKUP) == PIT_PREVIOUS_SHA256
    audit_manifest = json.loads((AUDIT / "manifest.json").read_text())
    raw_path = ROOT / audit_manifest["staged"]["2024"]["raw_path"]
    raw_sha = sha256(raw_path)
    assert raw_sha == audit_manifest["staged"]["2024"]["raw_sha256"]
    return {
        "previous_input_sha256": PIT_PREVIOUS_SHA256,
        "backup_path": str(PIT_BACKUP), "backup_sha256": sha256(PIT_BACKUP),
        "current_input_sha256": sha256(PIT_PATH),
        "numeric_key_identity": True,
        "all_preserved_columns_identity": common_columns,
        "source_workbook_path": str(raw_path), "source_workbook_sha256": raw_sha,
        "rows_compared": int(len(left)),
        "refresh_reason": (
            "Source-declared PIT occurrence fields were added; all keys and reported "
            "count values equal the preserved pre-refresh parquet."
        ),
    }


def build_panel(
    members: pd.DataFrame, *, pit: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    names_path = SITES / "msa_coc_sites__longitudinal_sites.parquet"
    names = pd.read_parquet(names_path).set_index("msa_id").msa_name
    source_ids = members[["msa_id", "coc_id"]].rename(columns={"coc_id": "pit_source_id"})
    assert not pit.duplicated(["pit_year", "coc_id"]).any(), "PIT source must be unique by CoC/year"
    selected = source_ids.merge(
        pit.loc[pit.pit_year.isin(YEARS)], left_on="pit_source_id", right_on="coc_id",
        how="left", validate="many_to_many",
    )
    assert len(selected) == len(members) * len(YEARS)
    assert not selected.duplicated(["pit_source_id", "pit_year"]).any()
    old_lineage = pd.read_csv(PRIOR_LINEAGE, dtype={"msa_id": str})
    expected_pit_sources = old_lineage.groupby(["msa_id", "year"]).pit_source_id.apply(
        lambda values: "|".join(sorted(values.astype(str)))
    )
    actual_pit_sources = selected.groupby(["msa_id", "pit_year"]).pit_source_id.apply(
        lambda values: "|".join(sorted(values.astype(str)))
    )
    actual_pit_sources.index = actual_pit_sources.index.set_names(["msa_id", "year"])
    pd.testing.assert_series_equal(
        actual_pit_sources.sort_index(), expected_pit_sources.sort_index(),
        check_names=False,
    )
    for measure in PIT_MEASURES:
        selected[f"{measure}_eligible"] = observed_pit_mask(selected, measure).astype(bool)
    raw_inputs = {
        "pit_unsheltered": selected.unsheltered_count_observed.astype("string")
        .fillna("unknown").value_counts().to_dict()
    }
    lineage_rows = []
    for (msa_id, year), group in selected.groupby(["msa_id", "pit_year"], sort=True):
        row: dict[str, object] = {
            "msa_id": msa_id, "msa_name": names.loc[msa_id], "year": int(year),
            "pit_source_ids": "|".join(sorted(group.pit_source_id)),
            "reported_cocs": int(len(group)),
        }
        for measure in PIT_MEASURES:
            eligible = bool(group[f"{measure}_eligible"].all())
            row[f"{measure.removeprefix('pit_')}_observed"] = eligible
            # Keep reported aggregates in the panel; the observation flag
            # controls analysis eligibility without erasing source values.
            row[measure] = group[measure].sum(min_count=len(group))
        lineage_rows.append(row)
    pit_site = pd.DataFrame(lineage_rows)
    assert pit_site.reported_cocs.eq(
        pit_site.groupby("msa_id").reported_cocs.transform("first")
    ).all()
    assert len(pit_site) == 840 and pit_site.msa_id.nunique() == 60
    impact = json.loads((AUDIT / "frozen-site-impact.json").read_text())
    assert int(pit_site.unsheltered_observed.sum()) == impact["complete_observed_site_years"] == 711
    eligible_transitions = exact_annual_changes(
        pit_site.loc[pit_site.unsheltered_observed], ["pit_unsheltered"]
    )
    assert len(eligible_transitions) == impact["observed_annual_transitions"] == 508
    
    hic_parts = []
    for year in YEARS:
        hic = pd.read_parquet(ROOT / f"data/curated/hic/hic__H{year}.parquet")
        sources = members[["msa_id", "coc_id"]].copy()
        sources["source_id"] = sources.coc_id
        # The predecessor reconciliation is deliberately limited to HIC inputs.
        sources["source_ids"] = [
            _hic_source_ids(coc, year) for coc in sources.coc_id
        ]
        expanded = sources.assign(source_id=sources.source_ids.str.split("|")).explode("source_id")
        assert not expanded.duplicated(["msa_id", "source_id"]).any()
        consumed = expanded.source_id.tolist()
        assert len(consumed) == len(set(consumed)), f"HIC source consumed more than once in {year}"
        matched = expanded.merge(
            hic, left_on="source_id", right_on="coc_id", how="left",
            validate="many_to_one",
        )
        assert matched[list(HIC_MEASURES)].notna().all().all()
        source_lineage = expanded.groupby("msa_id").source_id.apply(
            lambda values: "|".join(sorted(values.astype(str)))
        ).rename("hic_source_ids")
        agg = matched.groupby("msa_id", as_index=False)[list(HIC_MEASURES)].sum()
        agg = agg.merge(source_lineage, on="msa_id", validate="one_to_one")
        agg["year"] = year
        hic_parts.append(agg)
    hic_site = pd.concat(hic_parts, ignore_index=True)
    expected_hic_sources = old_lineage.groupby(["msa_id", "year"]).hic_source_ids.apply(
        lambda values: "|".join(sorted({
            source_id for value in values for source_id in str(value).split("|")
        }))
    ).rename("expected_hic_source_ids").reset_index()
    observed_hic_sources = hic_site[["msa_id", "year", "hic_source_ids"]].merge(
        expected_hic_sources, on=["msa_id", "year"], validate="one_to_one"
    )
    assert observed_hic_sources.hic_source_ids.eq(
        observed_hic_sources.expected_hic_source_ids
    ).all()
    panel = pit_site.merge(hic_site, on=["msa_id", "year"], validate="one_to_one")
    panel = panel[OBSERVED_PANEL_COLUMNS].sort_values(["msa_id", "year"]).reset_index(drop=True)
    assert len(panel) == 840 and panel.groupby("msa_id").size().eq(14).all()
    assert panel.loc[panel.unsheltered_observed, "pit_total"].eq(
        panel.loc[panel.unsheltered_observed, "pit_sheltered"]
        + panel.loc[panel.unsheltered_observed, "pit_unsheltered"]
    ).all()
    previous_panel = pd.read_parquet(PRIOR_PANEL)
    hic_columns = [measure for measure in HIC_MEASURES if measure in previous_panel.columns]
    comparison = panel.merge(
        previous_panel[["msa_id", "year", *hic_columns]], on=["msa_id", "year"],
        suffixes=("", "_prior"), validate="one_to_one",
    )
    for measure in hic_columns:
        np.testing.assert_array_equal(comparison[measure].to_numpy(),
                                      comparison[f"{measure}_prior"].to_numpy())
    # Existing 720 reported annual changes, of which 508 have both observed PIT endpoints.
    prior_all = exact_annual_changes(panel, ["pit_unsheltered"])
    assert len(prior_all) == 720
    bed_sign_counts: dict[str, dict[str, int]] = {}
    for measure in HIC_MEASURES:
        current = panel[["msa_id", "year", measure]].copy()
        previous = panel[["msa_id", "year", measure]].copy()
        previous.year += 1
        previous = previous.rename(columns={measure: "previous_bed"})
        bed_deltas = current.merge(previous, on=["msa_id", "year"], validate="one_to_one")
        delta = bed_deltas[measure] - bed_deltas.previous_bed
        bed_sign_counts[measure] = {
            "increases": int(delta.gt(0).sum()),
            "declines": int(delta.lt(0).sum()),
            "flat": int(delta.eq(0).sum()),
            "all_exact_consecutive_hic_transitions": int(len(delta)),
        }
    inventory_rows = source_ids.merge(
        pit.loc[pit.pit_year.between(min(YEARS), max(YEARS))],
        left_on="pit_source_id", right_on="coc_id", how="left", validate="many_to_many",
    )
    reason_inventory = inventory_rows.groupby(
        ["pit_year", "count_types"], dropna=False
    ).size().rename("source_rows").reset_index()
    reason_inventory["pit_year"] = reason_inventory.pit_year.astype(int)
    return panel, reason_inventory, {
        "reported_site_years": 840,
        "observed_unsheltered_or_total_site_years": int(panel.unsheltered_observed.sum()),
        "observed_sheltered_site_years": int(panel.sheltered_observed.sum()),
        "observed_annual_changes": int(len(eligible_transitions)),
        "reported_annual_changes_before_mask": int(len(prior_all)),
        "unsheltered_count_type_inventory": raw_inputs,
        "hic_lineage_verified_against_prior": True,
        "hic_bed_delta_sign_counts_before_pit_mask": bed_sign_counts,
    }


def _hic_source_ids(coc: str, year: int) -> str:
    source_ids = [coc]
    if coc == "MA-505" and year < 2024:
        source_ids.append("MA-519")
    if coc == "CA-601" and year == 2010:
        source_ids.append("CA-610")
    if coc == "CA-611" and year <= 2013:
        source_ids.append("CA-605")
    if coc == "FL-503" and year <= 2014:
        source_ids.append("FL-516")
    return "|".join(source_ids)


def _validate_prior_hashes() -> dict[str, str]:
    prior_path = PRIOR_DIR / "summary.json"
    prior = json.loads(prior_path.read_text())
    assert prior["membership_identity"] == MEMBERSHIP_IDENTITY
    for name, expected in prior["inputs"].items():
        path = Path(name)
        current = sha256(path)
        if path.resolve() == PIT_PATH.resolve():
            assert expected == PIT_PREVIOUS_SHA256 and current != expected
        else:
            assert current == expected, f"Pinned prior input changed unexpectedly: {path}"
    return prior["inputs"]


def run_analysis(
    panel: pd.DataFrame, *, rng: np.random.Generator
) -> tuple[pd.DataFrame, pd.DataFrame]:
    all_results: list[dict[str, object]] = []
    pair_rows: list[pd.DataFrame] = []
    for bed in HIC_MEASURES:
        outcomes = PIT_MEASURES if bed == "hic_shelter_year_round_beds" else ("pit_unsheltered",)
        for outcome in outcomes:
            include_pooled = outcome == "pit_unsheltered"
            if include_pooled:
            # Prior pooled workflow: raw levels, adjusted log levels, and annual log changes.
                for transform, fixed_effects, raw_outcome, raw_predictor in (
                    ("raw_count_levels", False, True, True),
                    ("log1p_levels_fe", True, False, False),
                ):
                    predictors = panel[["msa_id", "year", bed]].copy()
                    observed_outcomes = panel.loc[
                        panel[observed_column(outcome)], ["msa_id", "year", outcome]
                    ].copy()
                    if not raw_predictor:
                        predictors[bed] = np.log1p(predictors[bed].astype(float))
                    if not raw_outcome:
                        observed_outcomes[outcome] = np.log1p(
                            observed_outcomes[outcome].astype(float)
                        )
                    for lag in LAGS:
                        matched = exact_lag_pairs(
                            predictors, observed_outcomes, predictor_column=bed,
                            outcome_column=outcome, lag=lag
                        )
                        if matched.empty:
                            continue
                        record = estimator(
                            matched, bed, outcome, fixed_effects=fixed_effects,
                            rng=rng, universe_sites=tuple(sorted(panel.msa_id.unique())),
                        )
                        record.update({
                            "bed_measure": bed, "pit_measure": outcome,
                            "analysis": transform, "lag": lag, "sign": "all",
                            "bed_year_min": int(matched.beds_year.min()),
                            "bed_year_max": int(matched.beds_year.max()),
                            "outcome_year_min": int(matched.outcome_year.min()),
                            "outcome_year_max": int(matched.outcome_year.max()),
                        })
                        pre = matched.loc[
                            matched.beds_year.le(2019) & matched.outcome_year.le(2019)
                        ]
                        if len(pre):
                            pre_site = pd.factorize(pre.msa_id, sort=True)[0]
                            pre_year = pd.factorize(pre.outcome_year, sort=True)[0]
                            pre_values = pre[[bed, outcome]].to_numpy(dtype=float)
                            record["pre2020_r"] = weighted_correlation(
                                pre_values, pre_site, pre_year, np.ones(len(pre)),
                                fixed_effects=fixed_effects,
                            )
                            record["pre2020_n_pairs"] = len(pre)
                        else:
                            record["pre2020_r"] = np.nan
                            record["pre2020_n_pairs"] = 0
                        all_results.append(record)
            # Predictor changes can be observed independently of PIT occurrence;
            # compute from the complete HIC series then join observed PIT endpoints.
            bed_changes = exact_annual_changes(panel, [bed]).rename(columns={bed: "bed_change"})
            # Calculate raw direction using exact adjacent HIC years.
            current = panel[["msa_id", "year", bed]].copy()
            previous = panel[["msa_id", "year", bed]].copy()
            previous.year += 1
            previous = previous.rename(columns={bed: "previous_bed"})
            deltas = current.merge(previous, on=["msa_id", "year"], validate="one_to_one")
            deltas["bed_delta"] = deltas[bed] - deltas.previous_bed
            bed_changes = bed_changes.merge(
                deltas[["msa_id", "year", "bed_delta"]],
                on=["msa_id", "year"], validate="one_to_one",
            )
            outcome_changes = exact_annual_changes(
                panel.loc[panel[observed_column(outcome)], ["msa_id", "year", outcome]], [outcome]
            )
            signs = [("increases", bed_changes.bed_delta.gt(0)),
                     ("declines", bed_changes.bed_delta.lt(0))]
            if outcome == "pit_unsheltered":
                signs.insert(0, ("all", pd.Series(True, index=bed_changes.index)))
            for sign, condition in signs:
                predictors = bed_changes.loc[
                    condition, ["msa_id", "year", "bed_change", "bed_delta"]
                ]
                for lag in LAGS:
                    matched = exact_lag_pairs(
                        predictors, outcome_changes,
                        predictor_column="bed_change", outcome_column=outcome,
                        lag=lag, outcome_year="year",
                    )
                    # exact_lag_pairs emits year columns; sign selection happened before FE.
                    if matched.empty:
                        continue
                    record = estimator(matched, "bed_change", outcome, fixed_effects=True,
                                       rng=rng, universe_sites=tuple(sorted(panel.msa_id.unique())))
                    record.update({"bed_measure": bed, "pit_measure": outcome,
                                   "analysis": "annual_log1p_changes_fe", "lag": lag, "sign": sign,
                                   "bed_year_min": int(matched.beds_year.min()),
                                   "bed_year_max": int(matched.beds_year.max()),
                                   "outcome_year_min": int(matched.outcome_year.min()),
                                   "outcome_year_max": int(matched.outcome_year.max())})
                    pre = matched.loc[matched.beds_year.le(2019) & matched.outcome_year.le(2019)]
                    if len(pre):
                        site_index = {
                            site_id: index
                            for index, site_id in enumerate(sorted(panel.msa_id.unique()))
                        }
                        pre_site = pre.msa_id.map(site_index).to_numpy(dtype=int)
                        pre_year = pd.factorize(pre.outcome_year, sort=True)[0]
                        record["pre2020_r"] = weighted_correlation(
                            pre[["bed_change", outcome]].to_numpy(dtype=float), pre_site,
                            pre_year, np.ones(len(pre)), fixed_effects=True,
                        )
                        record["pre2020_n_pairs"] = int(len(pre))
                    else:
                        record["pre2020_r"] = None
                        record["pre2020_n_pairs"] = 0
                    all_results.append(record)
                    pair_rows.append(matched.assign(bed_measure=bed, pit_measure=outcome,
                                                     sign=sign, lag=lag))
    paired = pd.concat(pair_rows, ignore_index=True) if pair_rows else pd.DataFrame()
    return pd.DataFrame(all_results), paired


def validate_estimator() -> dict[str, object]:
    # Deliberately unbalanced bipartite site/year fixture with a missing cell.
    fixture = pd.DataFrame({
        "site": ["a", "a", "b", "b", "c", "c"],
        "year": [1, 2, 1, 3, 2, 3],
        "x": [1., 2., 1.5, 4., 3., 5.],
        "y": [2., 1., 3., 4., 2., 7.],
    })
    site = pd.factorize(fixture.site)[0]
    year = pd.factorize(fixture.year)[0]
    values = fixture[["x", "y"]].to_numpy()
    residual = weighted_two_way_residuals(values, site, year, np.ones(len(fixture)))
    design = np.column_stack([np.eye(site.max() + 1)[site], np.eye(year.max() + 1)[year]])
    expected = values - design @ np.linalg.lstsq(design, values, rcond=None)[0]
    np.testing.assert_allclose(residual, expected, atol=1e-9)
    # Duplicate site histories and frequency weights must give the same estimate.
    site_frequency = np.array([2., 1., 1.])
    frequency = site_frequency[site]
    weighted = weighted_correlation(values, site, year, frequency, fixed_effects=True)
    duplicated_parts = []
    draw = 0
    for source_site, copies in zip(range(3), [2, 1, 1], strict=True):
        for _ in range(copies):
            rows = fixture.loc[fixture.site.eq(fixture.site.unique()[source_site])].copy()
            rows["site"] = f"draw-{draw}"
            duplicated_parts.append(rows)
            draw += 1
    duplicate = pd.concat(duplicated_parts, ignore_index=True)
    ds = pd.factorize(duplicate.site)[0]
    dy = pd.factorize(duplicate.year)[0]
    duplicated = weighted_correlation(duplicate[["x", "y"]].to_numpy(), ds, dy,
                                      np.ones(len(duplicate)), fixed_effects=True)
    # Deterministic complete sample cross-check uses one explicit duplication pattern.
    np.testing.assert_allclose(weighted, duplicated, atol=1e-9)
    # Calendar mask: no 2020->2022 annual change and no fabricated transition.
    calendar = pd.DataFrame({"msa_id": ["x"] * 3, "year": [2020, 2022, 2023], "v": [3., 8., 9.]})
    changes = exact_annual_changes(calendar, ["v"])
    assert changes.year.tolist() == [2023]
    return {"unbalanced_projection_matches_dummy_ols": True,
            "frequency_weight_matches_duplicate_site_bootstrap": True,
            "exact_calendar_change_fixture_passed": True}


def _json_safe(value: object) -> object:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return float(value) if np.isfinite(value) else None
    if pd.isna(value) if not isinstance(value, (str, bool)) else False:
        return None
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--json", action="store_true", help="print machine-readable completion JSON"
    )
    parser.add_argument(
        "--non-interactive", action="store_true",
        help="reserved for consistent agent invocation",
    )
    parser.add_argument(
        "--output-dir", type=Path,
        help="new output directory; existing nonempty directories are preserved",
    )
    args = parser.parse_args()
    out_dir = args.output_dir.expanduser().resolve() if args.output_dir else OUT.resolve()
    if out_dir.exists() and any(out_dir.iterdir()):
        raise SystemExit(
            "Output directory is not empty; choose a fresh --output-dir to preserve "
            f"existing attempts: {out_dir}"
        )
    prior_inputs = _validate_prior_hashes()
    members, resolution, membership_path = _load_frozen_members()
    pit = pd.read_parquet(PIT_PATH)
    refresh_bridge = _check_pit_refresh_bridge(pit)
    panel, reasons, availability = build_panel(members, pit=pit)
    checks = validate_estimator()
    rng = np.random.default_rng(SEED)
    results, pairs = run_analysis(panel, rng=rng)
    out_dir.mkdir(parents=True, exist_ok=True)
    input_hashes = {
        str(PIT_PATH): sha256(PIT_PATH),
        str(PIT_BACKUP): sha256(PIT_BACKUP),
        str(membership_path): sha256(membership_path),
        str(PRIOR_LINEAGE): sha256(PRIOR_LINEAGE),
        str(PRIOR_PANEL): sha256(PRIOR_PANEL),
    }
    input_hashes.update({str(ROOT / f"data/curated/hic/hic__H{year}.parquet"):
                         sha256(ROOT / f"data/curated/hic/hic__H{year}.parquet") for year in YEARS})
    input_hashes[str(AUDIT / "manifest.json")] = sha256(AUDIT / "manifest.json")
    raw_pit_source = refresh_bridge["source_workbook_path"]
    input_hashes[raw_pit_source] = refresh_bridge["source_workbook_sha256"]
    provenance = ProvenanceBlock(geo_type="msa", boundary_vintage="2024",
        definition_version="census_msa_2023", extra={
            "purpose": (
                "Observed-only PIT/HIC longitudinal diagnostic; provisional "
                "historical site geography"
            ),
            "membership_identity": resolution.membership_identity,
            "years": list(YEARS), "excluded_msa_ids": ["14860", "41420", "49620", "40900"],
            "source_lineage": (
                "P2024 PIT consumed once per frozen source CoC; HIC uses the "
                "predecessor source reconciliation from the prior lineage."
            ),
            "inputs": input_hashes, "script_sha256": sha256(__file__),
        })
    panel_path = out_dir / "observed_site_year_panel.parquet"
    write_parquet_with_provenance(panel, panel_path, provenance)
    results.to_csv(out_dir / "observed_longitudinal_results.csv", index=False)
    pairs.to_csv(out_dir / "observed_annual_change_pairs.csv", index=False)
    reasons.to_csv(out_dir / "pit_source_observation_inventory.csv", index=False)
    site_counts = panel.groupby("msa_id", as_index=False)[
        ["unsheltered_observed", "total_observed", "sheltered_observed"]
    ].sum()
    site_counts.to_csv(out_dir / "site_observed_year_counts.csv", index=False)
    year_counts = panel.groupby("year", as_index=False)[
        ["unsheltered_observed", "total_observed", "sheltered_observed"]
    ].sum()
    year_counts.to_csv(out_dir / "year_observed_site_counts.csv", index=False)
    if len(pairs):
        pair_counts = pairs.groupby(
            ["bed_measure", "pit_measure", "sign", "lag", "outcome_year"],
            as_index=False,
        ).size().rename(columns={"size": "n_pairs"})
    else:
        pair_counts = pd.DataFrame(columns=[
            "bed_measure", "pit_measure", "sign", "lag", "outcome_year", "n_pairs",
        ])
    pair_counts.to_csv(out_dir / "annual_pair_counts.csv", index=False)
    script_snapshot = out_dir / "analysis_script.py"
    shutil.copyfile(__file__, script_snapshot)
    summary = {
        "scope": (
            "Exploratory historical 60-site / 77-CoC analysis excluding "
            "Sacramento, Bridgeport, Salem, and York; 2010-2024 excluding 2021."
        ),
        "membership_identity": resolution.membership_identity,
        "sites": 60, "cocs": 77, "reported_site_years": 840,
        "years": list(YEARS), "bootstrap_replicates": BOOTSTRAPS, "seed": SEED,
        "pit_refresh_bridge": refresh_bridge, "prior_pinned_inputs": prior_inputs,
        "inputs": input_hashes, "availability": availability,
        "script_snapshot": str(script_snapshot),
        "script_snapshot_sha256": sha256(script_snapshot),
        "method": (
            "Pearson correlations with exact weighted least-squares site/year effects "
            "from a Schur-complement year system on each observed unbalanced lag/sign "
            "sample. Sign selection is before FE. Whole-site percentile bootstrap "
            "refits effects at each replicate. Each replicate samples the active sites "
            "in that lag/sign subset, matching the earlier split-check convention; "
            "the 60-site frozen universe and active-site count are reported. Positive "
            "lag means beds precede PIT; exact year keys only. Annual differences "
            "require both t and t-1 PIT endpoints observed; predictor HIC change needs "
            "no PIT observation at its own year."
        ),
        "scope_notes": [
            "No imputation or bridging missing PIT years.",
            "Sheltered PIT allows full, partial-unsheltered, and sheltered-only "
            "declared observations, but excludes explicit No YYYY PIT Count and "
            "unknown declarations.",
            "Total and unsheltered require complete, nonpartial observed unsheltered "
            "declarations; reported values remain in the source panel.",
            "Counts are site aggregates of frozen source CoCs; historical geography "
            "remains provisional.",
            "Intervals are exploratory, unadjusted for multiple comparisons, and do "
            "not support causal claims.",
        ],
        "estimator_validation": checks,
        "results": results.to_dict(orient="records"),
        "artifacts": {
            "panel": str(panel_path),
            "results": str(out_dir / "observed_longitudinal_results.csv"),
            "pairs": str(out_dir / "observed_annual_change_pairs.csv"),
            "source_inventory": str(out_dir / "pit_source_observation_inventory.csv"),
            "site_counts": str(out_dir / "site_observed_year_counts.csv"),
            "year_counts": str(out_dir / "year_observed_site_counts.csv"),
            "pair_counts": str(out_dir / "annual_pair_counts.csv"),
        },
    }
    summary = _json_safe(summary)
    manifest_path = out_dir / "observed_longitudinal_manifest.json"
    manifest_path.write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n")
    if args.json:
        print(json.dumps({"status": "complete", "manifest": str(manifest_path),
                          "results": len(results), "availability": availability}, indent=2))
    else:
        print(f"Wrote {len(results)} observed longitudinal results to {out_dir}")


if __name__ == "__main__":
    main()
