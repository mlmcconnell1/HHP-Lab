"""ACS1 gross rent or ZORI versus observed PIT outcomes in the frozen study.

Run with ``uv run python scripts/analyze_observed_site_rent_longitudinal.py --json``.
Inputs and earlier results are read-only; a populated output directory is refused.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from hhplab.storage.provenance import ProvenanceBlock, write_parquet_with_provenance

ROOT = Path(__file__).resolve().parents[1]
HELPER_PATH = ROOT / "scripts/analyze_observed_site_pit_hic_longitudinal.py"
SPEC = importlib.util.spec_from_file_location("observed_pit_hic_estimator", HELPER_PATH)
HELPER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HELPER)
DEFAULT_PARENT = Path(
    "/home/matt/HHP-Output/site-boundary-history-audit/exploratory-without-sacramento/"
    "observed-counts/final-attempt-02"
)
DEFAULT_OUTPUT = Path("/home/matt/HHP-Output/site-rent-unsheltered-longitudinal/20261024/run-02")
DEFAULT_ZORI_OUTPUT = Path(
    "/home/matt/HHP-Output/site-zori-unsheltered-longitudinal/20261024/run-02"
)
DEFAULT_TOTAL_OUTPUT = Path(
    "/home/matt/HHP-Output/site-rent-total-pit-longitudinal/20261024/run-01"
)
DEFAULT_ZORI_TOTAL_OUTPUT = Path(
    "/home/matt/HHP-Output/site-zori-total-pit-longitudinal/20261024/run-01"
)
ZORI_PATH = ROOT / "data/curated/zori/zori__county__Z2026.parquet"
ZORI_RAW_PATH = ROOT / "data/raw/zori/zori__county__2026-03-04.csv"
POPULATION_PATH = ROOT / "data/curated/pep/pep_county__v2020.parquet"
MSA_MEMBERSHIP_PATH = ROOT / "data/curated/msa/msa_county_membership__census_msa_2023.parquet"
ZORI_MIN_COVERAGE = .90
ZORI_WEIGHT_YEAR = 2020
PARENT_PANEL_SHA256 = "e5cdb30954d4e5f6fe22a9fdf9d88250bdc3171dc9eecea25b4c3be25ccf4cbb"
PARENT_MANIFEST_SHA256 = "6c2a7092d54dd1ffc0880d6c98fba9dcbdcae556a08c6e1326f0e110917ba113"
SEED = 20261024
BOOTSTRAPS = 1999
LAGS = (-2, -1, 0, 1, 2)
RENT = "median_gross_rent"
OUTCOME = "pit_unsheltered"
PANEL_COLUMNS = [
    *HELPER.OBSERVED_PANEL_COLUMNS,
    "acs_year", RENT, "acs_rent_available", "acs_cbsa_code", "acs_metro_name",
    "acs_definition_version", "acs_source_ref", "acs_source_path", "acs_source_sha256",
]
SERIES_COLUMNS = ["msa_id", "year", "value", "previous_year", "current_raw", "previous_raw"]
ZORI_PANEL_COLUMNS = [
    *PANEL_COLUMNS, "zori", "zori_available", "common_rent_available",
    "zori_calendar_year", "zori_coverage_ratio", "zori_county_count",
    "zori_membership_county_count", "zori_missing_counties", "zori_fixed_support_count",
    "zori_composition_complete",
]
NOTES = [
    "Frozen 60-site / 77-CoC universe; Bridgeport, Salem, York, Sacramento remain excluded.",
    "PIT years 2010-2024 excluding 2021; source-declared complete unsheltered observations only.",
    "ACS1 B25064_001E is median gross rent including utilities, in each vintage's nominal dollars.",
    "ACS vintage Y-1 is assigned to aligned year Y and compared with January PIT in year Y.",
    "Primary test: lag 0 annual delta log(rent) versus delta log1p(unsheltered), site/year FE.",
    "At lag 0, ACS2009-to-2010 rent growth pairs with January2010-to-January2011 PIT growth.",
    "Positive lag k pairs aligned rent year t with PIT year t+k; raw ACS endpoint is t-1.",
    "Secondary checks: lags -2,-1,+1,+2, adjusted log levels, and pre-2020 restrictions.",
    "All changes require adjacent endpoints; no interpolation, carry-forward or gap bridging.",
    "Rent predictors do not require a PIT observation at their own aligned year.",
    "No ACS2020 substitute; its aligned year 2021 is already outside this study.",
    "Active sites are resampled whole with replacement; fixed effects are refitted in every draw.",
    "Pre-2020 requires both aligned rent and outcome endpoints before 2020.",
    "Unadjusted exploratory 95% percentile intervals; no causal or multiple-testing claim.",
    "Native CBSA rent is a proxy for the whole-CoC site; no median is summed or apportioned.",
    "A target definition label or code match does not harmonize historical CBSA footprints.",
    "Historical site geography and ACS metro geography remain provisional; see coclab-p37ps.",
    "ACS sampling uncertainty is not separately propagated by the whole-site bootstrap.",
    "Year effects absorb common national nominal growth, not region-specific price shocks.",
    "No PSH adjustment is performed in this rent-versus-unsheltered check.",
]


def prepare_output(path: Path) -> None:
    if path.exists() and any(path.iterdir()):
        raise ValueError(f"Output directory is populated: {path}. Choose a fresh --output-dir.")
    path.mkdir(parents=True, exist_ok=True)


def observed_outcome_rows(panel: pd.DataFrame, measure: str) -> pd.DataFrame:
    """Total PIT needs both components observed, with an exact count identity."""
    flag = HELPER.observed_column(measure)
    eligible = panel[flag].fillna(False).astype(bool)
    if measure == "pit_total":
        eligible &= panel.unsheltered_observed.fillna(False).astype(bool)
        eligible &= panel.sheltered_observed.fillna(False).astype(bool)
        identity = panel.pit_total.eq(panel.pit_unsheltered + panel.pit_sheltered)
        if (eligible & ~identity.fillna(False)).any():
            raise ValueError(
                "Observed total PIT differs from its components; inspect parent counts.")
    return panel.loc[eligible].copy()


def assemble_panel(base: pd.DataFrame, rents: pd.DataFrame) -> pd.DataFrame:
    """Left-join year-lagged rent without dropping frozen site-year observations."""
    if base.duplicated(["msa_id", "year"]).any():
        raise ValueError("Duplicate PIT site/year keys; use the reviewed parent panel.")
    if rents.duplicated(["msa_id", "acs_year"]).any():
        raise ValueError("Duplicate ACS metro/vintage keys; inspect the selected inputs.")
    rent_rows = rents.copy()
    rent_rows["year"] = rent_rows.acs_year + 1
    panel = base.merge(rent_rows, on=["msa_id", "year"], how="left", validate="one_to_one")
    panel["acs_year"] = panel.year - 1
    rent_values = pd.to_numeric(panel[RENT], errors="coerce")
    panel["acs_rent_available"] = (
        rent_values.gt(0) & np.isfinite(rent_values)
    ).fillna(False).astype(bool)
    panel[RENT] = rent_values
    if len(panel) != len(base) or set(panel.msa_id) != set(base.msa_id):
        raise AssertionError("Rent join changed the frozen PIT universe")
    return panel.sort_values(["msa_id", "year"]).reset_index(drop=True)


def analysis_series(
    frame: pd.DataFrame, measure: str, *, log1p: bool, changes: bool,
) -> pd.DataFrame:
    """Transform eligible rows, keeping exact raw endpoints for independent review."""
    current = frame[["msa_id", "year", measure]].rename(columns={measure: "current_raw"})
    current["current_raw"] = current.current_raw.astype(float)
    valid = current.current_raw.notna() & np.isfinite(current.current_raw)
    valid &= current.current_raw.ge(0) if log1p else current.current_raw.gt(0)
    current = current.loc[valid].copy()
    transform = np.log1p if log1p else np.log
    if changes:
        previous = current.rename(columns={"current_raw": "previous_raw"}).copy()
        previous["year"] += 1
        current = current.merge(previous, on=["msa_id", "year"], validate="one_to_one")
        current["previous_year"] = current.year - 1
        current["value"] = transform(current.current_raw) - transform(current.previous_raw)
    else:
        current["previous_raw"] = np.nan
        current["previous_year"] = pd.Series(pd.NA, index=current.index, dtype="Int64")
        current["value"] = transform(current.current_raw)
    return current[SERIES_COLUMNS].astype({"previous_year": "Int64"}).copy()


def paired_series(predictor: pd.DataFrame, outcome: pd.DataFrame, lag: int) -> pd.DataFrame:
    left = predictor.rename(columns={
        "year": "aligned_rent_year", "value": "rent_value",
        "previous_year": "previous_aligned_rent_year",
        "current_raw": "rent_current_raw", "previous_raw": "rent_previous_raw",
    }).copy()
    left["outcome_year"] = left.aligned_rent_year + lag
    right = outcome.rename(columns={
        "year": "outcome_year", "value": "pit_value", "previous_year": "previous_pit_year",
        "current_raw": "pit_current_raw", "previous_raw": "pit_previous_raw",
    })
    matched = left.merge(right, on=["msa_id", "outcome_year"], validate="one_to_one")
    matched["acs_year"] = matched.aligned_rent_year - 1
    matched["previous_acs_year"] = matched.previous_aligned_rent_year - 1
    matched["lag"] = lag
    return matched


def read_inputs(parent_dir: Path, acs_dir: Path):
    parent_panel = parent_dir / "observed_site_year_panel.parquet"
    parent_manifest = parent_dir / "observed_longitudinal_manifest.json"
    for path, expected in [
        (parent_panel, PARENT_PANEL_SHA256), (parent_manifest, PARENT_MANIFEST_SHA256),
    ]:
        if HELPER.sha256(path) != expected:
            raise ValueError(f"Reviewed input changed: {path}. Review it before rerunning.")
    manifest = json.loads(parent_manifest.read_text())
    if manifest["membership_identity"] != HELPER.MEMBERSHIP_IDENTITY:
        raise ValueError("Parent membership identity differs from the frozen 60-site study")
    base = pd.read_parquet(parent_panel)
    if len(base) != 840 or base.msa_id.nunique() != 60 or set(base.year) != set(HELPER.YEARS):
        raise ValueError("Parent panel does not match the reviewed 60-site / 14-year universe")
    inputs = {str(parent_panel): HELPER.sha256(parent_panel),
              str(parent_manifest): HELPER.sha256(parent_manifest)}
    inventory = []
    frames = []
    for year in sorted({year - 1 for year in HELPER.YEARS}):
        path = acs_dir / f"acs1_metro__A{year}@Dcensusmsa2023.parquet"
        if not path.exists():
            raise ValueError(f"Missing {path}; run hhplab ingest acs1-metro --vintage {year}.")
        metadata = pq.read_schema(path).metadata or {}
        provenance = json.loads(metadata.get(b"hhplab_provenance", b"{}"))
        if (provenance.get("extra", {}).get("acs_product") != "acs1"
                or str(provenance.get("acs_vintage")) != str(year)):
            raise ValueError(f"Unverified ACS1 product/vintage in {path}; inspect provenance.")
        frame = pd.read_parquet(path, columns=[
            "metro_id", "cbsa_code", "metro_name", "acs1_vintage", "definition_version",
            RENT, "source_ref",
        ])
        if frame.metro_id.duplicated().any():
            raise ValueError(f"Duplicate metro IDs in {path}")
        if not frame.acs1_vintage.astype(str).eq(str(year)).all():
            raise ValueError(f"Mixed ACS vintages in {path}")
        if not frame.definition_version.eq("census_msa_2023").all():
            raise ValueError(f"Unexpected metro target definition in {path}")
        if not frame.metro_id.astype(str).eq(frame.cbsa_code.astype(str)).all():
            raise ValueError(f"Metro/CBSA IDs differ in {path}; inspect mapping before use.")
        digest = HELPER.sha256(path)
        inputs[str(path)] = digest
        selected = frame.loc[frame.metro_id.isin(base.msa_id)].copy()
        selected = selected.rename(columns={
            "metro_id": "msa_id", "cbsa_code": "acs_cbsa_code", "metro_name": "acs_metro_name",
            "definition_version": "acs_definition_version", "source_ref": "acs_source_ref",
        }).drop(columns="acs1_vintage")
        selected["acs_year"] = year
        selected["acs_source_path"] = str(path)
        selected["acs_source_sha256"] = digest
        frames.append(selected)
        inventory.append({
            "acs_year": year, "aligned_pit_year": year + 1, "path": str(path), "sha256": digest,
            "matched_sites": len(selected),
            "missing_site_ids": sorted(set(base.msa_id) - set(selected.msa_id)),
            "acs_product": "acs1", "native_geo": "CBSA", "table": "B25064",
            "source_provenance": provenance,
        })
    return assemble_panel(base, pd.concat(frames, ignore_index=True)), inputs, inventory


def run_checks(
    panel: pd.DataFrame, *, replicates: int, rent_measure: str = RENT,
    available_column: str = "acs_rent_available", outcome_measure: str = OUTCOME,
):
    rng = np.random.default_rng(SEED)
    results, pair_frames = [], []
    observed = observed_outcome_rows(panel, outcome_measure)
    available = panel.loc[panel[available_column].fillna(False)].copy()
    for analysis, changes in [("annual_changes_fe", True), ("log_levels_fe", False)]:
        predictor = analysis_series(available, rent_measure, log1p=False, changes=changes)
        outcome = analysis_series(observed, outcome_measure, log1p=True, changes=changes)
        for lag in LAGS:
            all_pairs = paired_series(predictor, outcome, lag)
            for period in ("full", "pre2020"):
                pairs = all_pairs.copy()
                if period == "pre2020":
                    pairs = pairs.loc[
                        pairs.aligned_rent_year.le(2019) & pairs.outcome_year.le(2019)
                    ].copy()
                if pairs.empty:
                    raise ValueError(f"No pairs for {analysis}/{period}/lag={lag}")
                record = HELPER.estimator(
                    pairs, "rent_value", "pit_value", fixed_effects=True,
                    rng=rng, universe_sites=tuple(sorted(panel.msa_id.unique())),
                    bootstrap_replicates=replicates,
                )
                record.update({
                    "analysis": analysis, "period": period, "lag": lag,
                    "primary": analysis == "annual_changes_fe" and period == "full" and lag == 0,
                    "acs_year_min": int(pairs.acs_year.min()),
                    "acs_year_max": int(pairs.acs_year.max()),
                    "outcome_year_min": int(pairs.outcome_year.min()),
                    "outcome_year_max": int(pairs.outcome_year.max()),
                })
                # Independent point check: explicit dummy-variable least squares.
                site = pd.get_dummies(pairs.msa_id, dtype=float).to_numpy()
                year = pd.get_dummies(pairs.outcome_year, dtype=float).to_numpy()
                design = np.column_stack([site, year])
                values = pairs[["rent_value", "pit_value"]].to_numpy(dtype=float)
                residual = values - design @ np.linalg.lstsq(design, values, rcond=None)[0]
                raw_scale = np.square(values - values.mean(axis=0)).sum(axis=0)
                absorbed = np.any(
                    np.square(residual).sum(axis=0) <= 1e-20 * np.maximum(raw_scale, 1.)
                )
                reference = None if absorbed else float(np.corrcoef(residual.T)[0, 1])
                if reference is None:
                    if record["point_estimate_defined"]:
                        raise AssertionError("Estimator defined a fully absorbed correlation")
                    record["undefined_reason"] = "No residual variation after site/year effects"
                else:
                    np.testing.assert_allclose(record["pearson_r"], reference, atol=1e-10)
                    record["undefined_reason"] = None
                record["dummy_ols_reference_r"] = reference
                results.append(record)
                pair_frames.append(pairs.assign(analysis=analysis, period=period))
                print(f"Checked {analysis}/{period}/lag={lag}: {len(pairs)} pairs",
                      file=sys.stderr, flush=True)
    return pd.DataFrame(results), pd.concat(pair_frames, ignore_index=True)


def plot_results(
    results: pd.DataFrame, output: Path, *,
    title: str = "ACS 1-year gross rent (including utilities) versus unsheltered PIT",
    period_label: str = "2010–2024, excluding 2021",
    source_label: str = "ACS vintage", example: str = "ACS2009→2010 versus PIT2010→2011",
    basename: str = "rent_unsheltered_lead_lag",
) -> list[Path]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 2, figsize=(13, 7.5), sharey=True)
    for axis, analysis, panel_title in zip(
        axes, ("annual_changes_fe", "log_levels_fe"),
        ("Annual changes (primary at lag 0)", "Log levels (secondary)"), strict=True,
    ):
        subset = results.loc[results.analysis.eq(analysis)]
        for period, offset, color, label in [
            ("full", -.07, "#176b9e", period_label),
            ("pre2020", .07, "#c06b20", "Pre-2020 sensitivity"),
        ]:
            data = subset.loc[subset.period.eq(period)].sort_values("lag")
            data = data.loc[data.point_estimate_defined & data.bootstrap_interval_defined]
            axis.errorbar(
                data.lag + offset, data.pearson_r,
                yerr=np.vstack([data.pearson_r - data.site_bootstrap_low,
                                data.site_bootstrap_high - data.pearson_r]),
                fmt="o", capsize=4, color=color, label=label,
            )
        counts = subset.loc[subset.period.eq("full")].sort_values("lag")
        axis.set_xticks(counts.lag, [
            f"{row.lag:+d}\n{row.n_pairs} pairs\n{row.n_sites} sites"
            for row in counts.itertuples()
        ])
        axis.axhline(0, color="#777777", linewidth=1)
        axis.set_title(panel_title)
        axis.set_xlabel("Lag in calendar years; full-period sample counts shown")
        axis.grid(axis="y", alpha=.2)
    lower = min(-.1, float(results.site_bootstrap_low.min()) - .07)
    upper = max(.1, float(results.site_bootstrap_high.max()) + .07)
    axes[0].set_ylim(max(-1, lower), min(1, upper))
    axes[0].set_ylabel("Correlation after site and year effects (95% site-bootstrap interval)")
    axes[0].legend(loc="best", fontsize=9)
    fig.suptitle(title, fontsize=15)
    fig.text(.5, .035,
             f"{source_label} Y−1 aligns to January PIT year Y. "
             "Positive lag: rent leads beyond this alignment.\n"
             f"Lag 0: {example}. Observed PIT endpoints only.\n"
             "Frozen 60-site / 77-CoC universe; available metro matches. "
             "Provisional geography; exploratory intervals.",
             ha="center", va="bottom", fontsize=9)
    fig.tight_layout(rect=(0, .14, 1, .94))
    paths = []
    for extension in ("png", "svg"):
        path = output / f"{basename}.{extension}"
        fig.savefig(path, dpi=180)
        paths.append(path)
    plt.close(fig)
    return paths


def build_zori_annual(
    monthly: pd.DataFrame, population: pd.DataFrame, membership: pd.DataFrame,
    years: list[int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Use full calendar years; null incomplete region-years to keep counties/weights fixed."""
    from hhplab.sources.zori.zori_metro import aggregate_yearly_zori_to_msa

    members = membership[["msa_id", "county_fips"]].copy()
    if members.duplicated().any():
        raise ValueError("Duplicate MSA/county membership; inspect the input crosswalk.")
    counties = set(members.county_fips)
    data = monthly.loc[monthly.geo_id.isin(counties), ["geo_id", "date", "zori"]].copy()
    data["date"] = pd.to_datetime(data.date)
    data["year"] = data.date.dt.year
    data["month"] = data.date.dt.month
    data = data.loc[data.year.isin(years)].copy()
    if data.duplicated(["geo_id", "year", "month"]).any():
        raise ValueError("Duplicate county/month ZORI observations; inspect the source.")
    valid = data.zori.notna() & np.isfinite(data.zori) & data.zori.gt(0)
    county_year = data.loc[valid].groupby(["geo_id", "year"], as_index=False).agg(
        zori=("zori", "mean"), month_count=("month", "nunique"),
    ).rename(columns={"geo_id": "county_fips"})
    grid = pd.MultiIndex.from_product(
        [sorted(counties), years], names=["county_fips", "year"],
    ).to_frame(index=False)
    county_year = grid.merge(county_year, how="left", validate="one_to_one")
    county_year["month_count"] = county_year.month_count.fillna(0).astype(int)
    county_year.loc[county_year.month_count.ne(12), "zori"] = np.nan
    county_year["complete_calendar_year"] = county_year.month_count.eq(12)
    complete = county_year.groupby("county_fips").complete_calendar_year.any()
    county_year["fixed_support_member"] = county_year.county_fips.map(complete).astype(bool)

    weights = population.loc[
        population.year.eq(ZORI_WEIGHT_YEAR) & population.county_fips.isin(counties),
        ["county_fips", "population"],
    ].copy()
    if weights.county_fips.duplicated().any():
        raise ValueError("Duplicate county population weights; inspect the selected vintage.")
    missing = sorted(counties - set(weights.county_fips))
    if missing or not (np.isfinite(weights.population) & weights.population.gt(0)).all():
        raise ValueError(f"Missing or invalid fixed county weights: {missing}; inspect PEP input.")
    fixed_weights = pd.concat([weights.assign(year=year) for year in years], ignore_index=True)
    rolled = aggregate_yearly_zori_to_msa(
        county_year[["county_fips", "year", "zori"]], fixed_weights,
        county_membership_df=members, years=years,
        min_coverage=ZORI_MIN_COVERAGE, balanced_composition=False,
    )
    # The core rollup allows counties to vary by year. Require ALL counties in
    # the fixed support at every usable regional endpoint, so entry/exit cannot
    # masquerade as a price change. Missing region-years remain missing.
    supported_members = members.loc[members.county_fips.isin(complete[complete].index)]
    support_counts = supported_members.groupby("msa_id").county_fips.nunique()
    rolled["fixed_support_count"] = rolled.msa_id.map(support_counts).fillna(0).astype(int)
    rolled["composition_complete"] = (
        rolled.county_count.eq(rolled.fixed_support_count) & rolled.fixed_support_count.gt(0)
    )
    rolled.loc[~rolled.composition_complete, "zori_coc"] = pd.NA
    return rolled, county_year.merge(weights, on="county_fips", validate="many_to_one")


def run_zori(args: argparse.Namespace) -> None:
    """Compare ZORI on available support and both rent measures on identical pairs."""
    prepare_output(args.output_dir)
    acs_panel, inputs, inventory = read_inputs(args.parent_dir, args.acs_dir)
    outcome_measure = f"pit_{args.pit_outcome}"
    outcome_label = args.pit_outcome
    observation_flag = HELPER.observed_column(outcome_measure)
    monthly = pd.read_parquet(ZORI_PATH)
    metadata = pq.read_schema(ZORI_PATH).metadata or {}
    zori_provenance = json.loads(metadata.get(
        b"hhplab_provenance", metadata.get(b"coclab_provenance", b"{}"),
    ))
    raw_sha = HELPER.sha256(ZORI_RAW_PATH)
    if (monthly.raw_sha256.drop_duplicates().tolist() != [raw_sha]
            or zori_provenance.get("extra", {}).get("raw_sha256") != raw_sha
            or not monthly.geo_type.eq("county").all()
            or not monthly.metric.eq("ZORI").all()):
        raise ValueError("Cached ZORI raw hash/product does not match provenance; review inputs.")
    membership = pd.read_parquet(MSA_MEMBERSHIP_PATH)
    membership = membership.loc[membership.msa_id.isin(acs_panel.msa_id)].copy()
    if set(membership.msa_id) != set(acs_panel.msa_id):
        raise ValueError("County membership is missing frozen sites; inspect the 2023 definition.")
    population = pd.read_parquet(POPULATION_PATH)
    start_year = int(pd.to_datetime(monthly.date).dt.year.min())
    end_year = min(int(pd.to_datetime(monthly.date).dt.year.max()), int(acs_panel.year.max()) - 1)
    calendar_years = list(range(start_year, end_year + 1))
    for path in (ZORI_PATH, ZORI_RAW_PATH, POPULATION_PATH, MSA_MEMBERSHIP_PATH):
        inputs[str(path)] = HELPER.sha256(path)
    rollup_code = ROOT / "hhplab/sources/zori/zori_metro.py"
    inputs[str(rollup_code)] = HELPER.sha256(rollup_code)
    notes = [
        "ZORI: Zillow Economic Research, county All Homes smoothed monthly asking-rent series.",
        "Cached March 4, 2026 release; raw CSV hash verified against curated rows and provenance.",
        "Each county/year needs 12 distinct valid months; incomplete annual means are ineligible.",
        "Fixed support: member counties with at least one complete year in the requested window.",
        "A region-year is usable only when every fixed-support county has all 12 valid months.",
        "Incomplete region-years stay missing; no county enters or leaves a usable regional value.",
        "Fixed 2020 PEP population weights, 2023 MSA county membership, minimum coverage 90%.",
        "The population-weighted proxy differs from Zillow's independently estimated metro ZORI.",
        "Calendar-year average Y-1 aligns to January PIT Y; positive lag adds calendar years.",
        f"Primary: delta log(ZORI) versus delta log1p({outcome_label}), lag 0, site/year FE.",
        "Total PIT requires both components observed and total = sheltered + unsheltered.",
        "Levels, additional lags and pre-2020 checks are secondary; no causal claim.",
        "Fully absorbed short-window comparisons are undefined, not zero, with no usable interval.",
        "The frozen 60-site / 77-CoC membership and four existing exclusions remain unchanged.",
        "Study ends with PIT2024; 2021 is excluded and no annual changes bridge that gap.",
        "PIT differences require complete observed counts at both endpoints; real zeroes remain.",
        "Predictor rent changes do not require PIT observations in their own aligned year.",
        "Available ZORI sample plus ZORI and ACS1 gross rent on identical lag-specific pairs.",
        "Common samples require both rents at every predictor endpoint, before FE refitting.",
        f"{args.bootstrap_replicates} active-site bootstrap draws; effects refitted in every draw.",
        "Exploratory percentile intervals are unadjusted across 60 comparisons.",
        "Site and metro rent footprints differ; historical geography remains provisional.",
        "ZORI and ACS cover different rental populations; ACS survey error is not propagated.",
        f"No PSH adjustment in this rent-versus-{outcome_label} diagnostic.",
    ]
    plan = {
        "membership_identity": HELPER.MEMBERSHIP_IDENTITY, "rent_source": "zori",
        "pit_measure": outcome_measure, "pit_observation_flag": observation_flag,
        "notes": notes, "primary_test": "zori_available/annual_changes_fe/full/lag=0",
        "calendar_years": calendar_years, "lags": list(LAGS), "periods": ["full", "pre2020"],
        "samples": ["zori_available", "zori_common", "acs1_common"], "seed": SEED,
        "bootstrap_replicates": args.bootstrap_replicates, "inputs": inputs,
        "script_sha256": HELPER.sha256(__file__), "estimator_sha256": HELPER.sha256(HELPER_PATH),
        "zori_source_provenance": zori_provenance,
    }
    plan_path = args.output_dir / "analysis_plan.json"
    plan_path.write_text(json.dumps(plan, indent=2, allow_nan=False) + "\n")
    shutil.copyfile(__file__, args.output_dir / "analysis_script.py")
    shutil.copyfile(HELPER_PATH, args.output_dir / "estimator_script.py")
    shutil.copyfile(rollup_code, args.output_dir / "zori_rollup_script.py")
    rolled, county_year = build_zori_annual(monthly, population, membership, calendar_years)
    zori_rows = rolled.rename(columns={
        "year": "zori_calendar_year", "zori_coc": "zori",
        "coverage_ratio": "zori_coverage_ratio", "county_count": "zori_county_count",
        "membership_county_count": "zori_membership_county_count",
        "missing_counties": "zori_missing_counties",
        "fixed_support_count": "zori_fixed_support_count",
        "composition_complete": "zori_composition_complete",
    })
    zori_rows["year"] = zori_rows.zori_calendar_year + 1
    zori_columns = ["msa_id", "year", "zori", "zori_calendar_year", "zori_coverage_ratio",
                    "zori_county_count", "zori_membership_county_count", "zori_missing_counties",
                    "zori_fixed_support_count", "zori_composition_complete"]
    panel = acs_panel.loc[acs_panel.year.between(start_year + 1, end_year + 1)].merge(
        zori_rows[zori_columns], on=["msa_id", "year"], how="left", validate="one_to_one",
    )
    if panel.msa_id.nunique() != 60:
        raise AssertionError("ZORI join changed frozen membership")
    panel["zori"] = pd.to_numeric(panel.zori, errors="coerce")
    panel["zori_available"] = (
        panel.zori.notna() & np.isfinite(panel.zori.astype(float)) & panel.zori.gt(0)
    ).fillna(False).astype(bool)
    panel["common_rent_available"] = panel.zori_available & panel.acs_rent_available
    provenance = ProvenanceBlock(
        geo_type="msa", definition_version="census_msa_2023", boundary_vintage="2024",
        extra={"purpose": f"Observed {outcome_label} PIT versus ZORI, matched ACS comparison",
               "membership_identity": HELPER.MEMBERSHIP_IDENTITY, "inputs": inputs,
               "analysis_plan_sha256": HELPER.sha256(plan_path), "notes": notes},
    )
    write_parquet_with_provenance(
        panel[ZORI_PANEL_COLUMNS], args.output_dir / "site_year_panel.parquet", provenance,
    )
    write_parquet_with_provenance(rolled, args.output_dir / "zori_msa_calendar_year.parquet",
                                  provenance)
    county_year.to_csv(args.output_dir / "zori_county_year_inventory.csv", index=False)
    panel[["msa_id", "msa_name", "year", "acs_year", "unsheltered_observed",
           "sheltered_observed", "total_observed",
           "acs_rent_available", "zori_available", "common_rent_available",
           "zori_coverage_ratio"]].to_csv(args.output_dir / "site_year_coverage.csv", index=False)
    (args.output_dir / "acs_source_inventory.json").write_text(
        json.dumps(inventory, indent=2, allow_nan=False) + "\n")
    all_results, all_pairs = [], []
    for sample, measure, availability in [
        ("zori_available", "zori", "zori_available"),
        ("zori_common", "zori", "common_rent_available"),
        ("acs1_common", RENT, "common_rent_available"),
    ]:
        print(f"Estimating {sample}", file=sys.stderr, flush=True)
        results, pairs = run_checks(
            panel, replicates=args.bootstrap_replicates, rent_measure=measure,
            available_column=availability, outcome_measure=outcome_measure,
        )
        all_results.append(results.assign(sample=sample, pit_measure=outcome_measure))
        all_pairs.append(pairs.assign(sample=sample, pit_measure=outcome_measure))
    results = pd.concat(all_results, ignore_index=True)
    pairs = pd.concat(all_pairs, ignore_index=True)
    key = ["analysis", "period", "lag", "msa_id", "aligned_rent_year", "outcome_year"]
    left = pairs.loc[pairs["sample"].eq("zori_common"), key].sort_values(key).reset_index(drop=True)
    right = (
        pairs.loc[pairs["sample"].eq("acs1_common"), key].sort_values(key).reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(left, right)
    if (results.point_estimate_defined & ~results.bootstrap_interval_defined).any():
        raise AssertionError("Some intervals have too few defined draws; inspect results.")
    results.to_csv(args.output_dir / "longitudinal_results.csv", index=False)
    pairs.to_csv(args.output_dir / "paired_observations.csv", index=False)
    pairs.groupby(["sample", "analysis", "period", "lag", "outcome_year"], as_index=False).agg(
        n_pairs=("msa_id", "size"), n_sites=("msa_id", "nunique"),
    ).to_csv(args.output_dir / "pair_counts.csv", index=False)
    plot_results(
        results.loc[results["sample"].eq("zori_available")], args.output_dir,
        title=f"ZORI asking rents versus observed {outcome_label} PIT",
        period_label=f"{start_year + 1}–{end_year + 1}, excluding 2021",
        source_label="ZORI calendar mean", example="ZORI2015→2016 versus PIT2016→2017",
        basename=f"zori_{outcome_label}_lead_lag",
    )
    for path, digest in inputs.items():
        if HELPER.sha256(path) != digest:
            raise AssertionError(f"Input changed during analysis: {path}")
    summary = {
        **plan, "frozen_sites": 60, "site_years": len(panel),
        "pit_years": sorted(panel.year.unique().tolist()),
        "available_zori_site_years": int(panel.zori_available.sum()),
        "observed_unsheltered_site_years": int(panel.unsheltered_observed.sum()),
        "observed_outcome_site_years": len(observed_outcome_rows(panel, outcome_measure)),
        "common_pairs_identical": True, "results": results.to_dict("records"),
        "artifact_sha256": {path.name: HELPER.sha256(path)
                            for path in sorted(args.output_dir.iterdir()) if path.is_file()},
    }
    manifest_path = args.output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(HELPER._json_safe(summary), indent=2, allow_nan=False) + "\n")
    primary = results.loc[results.primary & results.period.eq("full")].to_dict("records")
    if args.json:
        print(json.dumps(HELPER._json_safe({"status": "complete", "manifest": str(manifest_path),
                                          "primary": primary, "results": len(results)}), indent=2))
    else:
        print(f"Wrote {len(results)} ZORI and matched ACS checks to {args.output_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parent-dir", type=Path, default=DEFAULT_PARENT)
    parser.add_argument("--acs-dir", type=Path, default=ROOT / "data/curated/acs")
    parser.add_argument("--rent-source", choices=("acs1", "zori"), default="acs1")
    parser.add_argument("--pit-outcome", choices=("unsheltered", "total"), default="unsheltered")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--bootstrap-replicates", type=int, default=BOOTSTRAPS)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--non-interactive", action="store_true")
    args = parser.parse_args()
    if args.bootstrap_replicates < 199:
        parser.error("Use at least 199 bootstrap replicates; the reviewed default is 1999.")
    if args.output_dir is None:
        if args.pit_outcome == "total":
            args.output_dir = (
                DEFAULT_ZORI_TOTAL_OUTPUT if args.rent_source == "zori" else DEFAULT_TOTAL_OUTPUT
            )
        else:
            args.output_dir = DEFAULT_ZORI_OUTPUT if args.rent_source == "zori" else DEFAULT_OUTPUT
    if args.rent_source == "zori":
        run_zori(args)
        return
    prepare_output(args.output_dir)
    panel, inputs, inventory = read_inputs(args.parent_dir, args.acs_dir)
    outcome_measure = f"pit_{args.pit_outcome}"
    observation_flag = HELPER.observed_column(outcome_measure)
    notes = list(NOTES)
    if args.pit_outcome == "total":
        notes = [note.replace("unsheltered", "total") for note in notes]
        notes.append(
            "Total PIT needs both components observed and total = sheltered + unsheltered.")
    plan = {
        "membership_identity": HELPER.MEMBERSHIP_IDENTITY, "notes": notes,
        "pit_measure": outcome_measure, "pit_observation_flag": observation_flag,
        "primary_test": "annual_changes_fe/full/lag=0", "lags": list(LAGS),
        "periods": ["full", "pre2020"], "seed": SEED,
        "bootstrap_replicates": args.bootstrap_replicates, "inputs": inputs,
        "script_sha256": HELPER.sha256(__file__),
        "estimator_sha256": HELPER.sha256(HELPER_PATH),
    }
    plan_path = args.output_dir / "analysis_plan.json"
    plan_path.write_text(json.dumps(plan, indent=2, allow_nan=False) + "\n")
    shutil.copyfile(__file__, args.output_dir / "analysis_script.py")
    shutil.copyfile(HELPER_PATH, args.output_dir / "estimator_script.py")
    provenance = ProvenanceBlock(
        geo_type="msa", definition_version="census_msa_2023", boundary_vintage="2024",
        extra={"purpose": f"Observed {args.pit_outcome} PIT versus native metro ACS1 gross rent",
               "membership_identity": HELPER.MEMBERSHIP_IDENTITY,
               "inputs": inputs, "analysis_plan_sha256": HELPER.sha256(plan_path),
               "notes": notes},
    )
    write_parquet_with_provenance(panel[PANEL_COLUMNS], args.output_dir / "site_year_panel.parquet",
                                  provenance)
    coverage = panel[["msa_id", "msa_name", "year", "acs_year", "unsheltered_observed",
                      "sheltered_observed", "total_observed",
                      "acs_rent_available", "acs_cbsa_code"]].copy()
    coverage["eligible_level_pair"] = (
        coverage[observation_flag] & coverage.acs_rent_available
    )
    coverage.to_csv(args.output_dir / "site_year_coverage.csv", index=False)
    (args.output_dir / "acs_source_inventory.json").write_text(
        json.dumps(inventory, indent=2, allow_nan=False) + "\n")
    results, pairs = run_checks(
        panel, replicates=args.bootstrap_replicates, outcome_measure=outcome_measure,
    )
    results["pit_measure"] = outcome_measure
    pairs["pit_measure"] = outcome_measure
    if not results.bootstrap_interval_defined.all():
        raise AssertionError("Some intervals have too few defined bootstrap draws; inspect output.")
    if not results.bootstrap_frequency_matches_duplicated_sample.all():
        raise AssertionError("Bootstrap frequency weighting differs from duplicated-site histories")
    results.to_csv(args.output_dir / "longitudinal_results.csv", index=False)
    pairs.to_csv(args.output_dir / "paired_observations.csv", index=False)
    counts = pairs.groupby(["analysis", "period", "lag", "outcome_year"], as_index=False).agg(
        n_pairs=("msa_id", "size"), n_sites=("msa_id", "nunique"),
    )
    counts.to_csv(args.output_dir / "pair_counts.csv", index=False)
    plot_results(
        results, args.output_dir,
        title=f"ACS 1-year gross rent (including utilities) versus {args.pit_outcome} PIT",
        basename=f"rent_{args.pit_outcome}_lead_lag",
    )
    for path, digest in inputs.items():
        if HELPER.sha256(path) != digest:
            raise AssertionError(f"Input changed during analysis: {path}")
    artifacts = {
        path.name: HELPER.sha256(path)
        for path in sorted(args.output_dir.iterdir()) if path.is_file()
    }
    summary = {
        **plan, "frozen_sites": panel.msa_id.nunique(), "reported_site_years": len(panel),
        "observed_unsheltered_site_years": int(panel.unsheltered_observed.sum()),
        "observed_outcome_site_years": len(observed_outcome_rows(panel, outcome_measure)),
        "available_rent_site_years": int(panel.acs_rent_available.sum()),
        "eligible_level_site_years": int(coverage.eligible_level_pair.sum()),
        "missing_rent_site_years": coverage.loc[~coverage.acs_rent_available].to_dict("records"),
        "results": results.to_dict("records"), "artifact_sha256": artifacts,
        "independent_dummy_ols_max_difference": float(
            (results.pearson_r - results.dummy_ols_reference_r).abs().max()),
    }
    manifest_path = args.output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(HELPER._json_safe(summary), indent=2, allow_nan=False) + "\n")
    primary = next(record for record in summary["results"] if record["primary"])
    response = {"status": "complete", "manifest": str(manifest_path),
                "primary": primary, "results": len(results)}
    if args.json:
        print(json.dumps(HELPER._json_safe(response), indent=2, allow_nan=False))
    else:
        print(f"Wrote {len(results)} checks and figures to {args.output_dir}")


if __name__ == "__main__":
    main()
