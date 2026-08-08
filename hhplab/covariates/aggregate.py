"""Panel-ready aggregation helpers for expanded covariates."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from hhplab.covariates.catalog import MeasureAggregation, covariate_source_spec
from hhplab.covariates.ingest import (
    STATE_NAME_TO_ABBREV,
    default_covariate_output_path,
    default_covariate_pair_output_path,
)
from hhplab.geographies.coc.ct_planning_regions import (
    CT_LEGACY_COUNTY_VINTAGE,
    CT_PLANNING_REGION_VINTAGE,
    CtPlanningRegionCrosswalk,
    build_ct_county_planning_region_crosswalk,
    is_ct_legacy_county_fips,
    is_ct_planning_region_fips,
)
from hhplab.geographies.msa import DEFINITION_VERSION as DEFAULT_MSA_DEFINITION_VERSION
from hhplab.geographies.msa.msa_io import read_msa_county_membership, read_msa_definitions
from hhplab.naming import covariate_panel_filename
from hhplab.paths import curated_dir
from hhplab.provenance import ProvenanceBlock, read_provenance, write_parquet_with_provenance
from hhplab.sources.bls.qcew.contract import (
    QCEW_DERIVED_MEASURE_COLUMNS,
    QCEW_SOURCE_ID,
)
from hhplab.sources.census.bps.census_bps_contract import (
    CENSUS_BPS_CLASS_UNIT_COLUMNS,
    CENSUS_BPS_CLASS_VALUE_COLUMNS,
    CENSUS_BPS_DERIVED_MEASURE_COLUMNS,
    CENSUS_BPS_MIX_ADJUSTED_VALUE_PER_UNIT_COLUMN,
    CENSUS_BPS_SOURCE_ID,
)
from hhplab.sources.census.pep.pep_aggregate import load_pep_county
from hhplab.sources.irs.irs_soi_contract import (
    IRS_SOI_MSA_MEASURE_COLUMNS,
    IRS_SOI_PAIR_MEASURE_COLUMNS,
    IRS_SOI_SOURCE_ID,
)
from hhplab.sources.mpi.mpi_contract import MPI_ESTIMATE_YEAR, MPI_SOURCE_ID
from hhplab.sources.prism.temperature import (
    EMERGENCY_SHELTER_ACTIVATION_C,
    FREEZING_C,
    derive_prism_temperature_basis,
)
from hhplab.sources.saiz.saiz_contract import SAIZ_ESTIMATE_YEAR, SAIZ_SOURCE_ID

__all__ = [
    "EMERGENCY_SHELTER_ACTIVATION_C",
    "FREEZING_C",
    "derive_prism_temperature_basis",
]

PRISM_TMIN_SOURCE_ID = "prism_tmin_january"
STATIC_COVARIATE_SOURCE_YEARS = {
    MPI_SOURCE_ID: MPI_ESTIMATE_YEAR,
    SAIZ_SOURCE_ID: SAIZ_ESTIMATE_YEAR,
}


def default_covariate_panel_path(
    source_id: str,
    *,
    output_dir: Path | str | None = None,
) -> Path:
    """Return the deterministic panel-ready output path."""
    spec = covariate_source_spec(source_id)
    base = curated_dir("covariates") if output_dir is None else Path(output_dir)
    return base / covariate_panel_filename(
        source_id,
        spec.first_year,
        spec.last_year,
    )


def aggregate_covariate_source(
    source_id: str,
    *,
    curated_path: Path | str | None = None,
    output_dir: Path | str | None = None,
    output_path: Path | str | None = None,
    years: list[int] | None = None,
    target_geo: str = "coc",
    msa_definition_version: str = DEFAULT_MSA_DEFINITION_VERSION,
    county_population_path: Path | str | None = None,
    data_root: Path | str | None = None,
    min_coverage_ratio: float | None = 1.0,
    drop_below_min_coverage: bool = False,
    force: bool = False,
) -> Path:
    """Materialize a panel-ready covariate table from curated source data.

    This step is a pass-through only when the source is already native to the
    requested target geography. Cross-geography covariate aggregation must be
    handled by an explicit recipe crosswalk/resample step.
    """
    spec = covariate_source_spec(source_id)
    if target_geo not in {"county", "coc", "state", "msa"}:
        raise ValueError(
            "Unsupported covariate target geography "
            f"'{target_geo}'. Use one of: county, coc, state, msa."
        )
    if target_geo != "msa" and spec.native_geo != target_geo:
        raise ValueError(
            f"Covariate source '{source_id}' is native to {spec.native_geo} geography "
            f"and cannot be emitted as {target_geo} panel-ready data without an explicit "
            "crosswalk. Use --target-geo "
            f"{spec.native_geo} for native output, or add a recipe resample/crosswalk step "
            "before joining it to the target panel."
        )
    if target_geo == "msa" and spec.native_geo not in {"county", "msa"}:
        raise ValueError(
            f"Covariate source '{source_id}' is native to {spec.native_geo} geography. "
            "Only county-native covariates can be population-weighted to --target-geo msa, "
            "and native-MSA covariates can be emitted directly."
        )
    if min_coverage_ratio is not None and not 0 <= min_coverage_ratio <= 1:
        raise ValueError("--min-coverage-ratio must be between 0 and 1.")
    if drop_below_min_coverage and target_geo != "msa":
        raise ValueError("--drop-below-min-coverage is only supported with --target-geo msa.")
    if drop_below_min_coverage and min_coverage_ratio is None:
        raise ValueError("--drop-below-min-coverage requires --min-coverage-ratio.")
    input_path = (
        Path(curated_path)
        if curated_path is not None
        else _default_covariate_curated_path(source_id, output_dir=output_dir)
    )
    if not input_path.exists():
        raise FileNotFoundError(
            f"Curated covariate file not found: {input_path}. "
            f"Run `hhplab ingest covariate --source {source_id}` first."
        )
    requested_years = list(years) if years is not None else None
    static_source_year = STATIC_COVARIATE_SOURCE_YEARS.get(source_id)
    if output_path is not None:
        destination = Path(output_path)
    elif requested_years and static_source_year is not None:
        base = curated_dir("covariates") if output_dir is None else Path(output_dir)
        destination = base / covariate_panel_filename(
            source_id,
            min(requested_years),
            max(requested_years),
        )
    else:
        destination = _covariate_panel_path_from_input(
            source_id=source_id,
            input_path=input_path,
            output_dir=output_dir,
        )
    if destination.exists() and not force:
        return destination

    df = pd.read_parquet(input_path)
    if source_id == PRISM_TMIN_SOURCE_ID:
        df = derive_prism_temperature_basis(df)
    required = [
        "geo_type",
        "geo_id",
        *(["msa_id"] if spec.native_geo == "msa" else []),
        "year",
        *spec.measure_columns,
    ]
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"Curated covariate file missing required columns: {missing}")
    static_year_policy = None
    if static_source_year is not None and years is not None:
        df = expand_static_covariate_years(
            df,
            years=years,
            source_year=static_source_year,
            source_id=source_id,
        )
        static_year_policy = {
            "policy": "carry_forward_static_estimate_to_requested_years",
            "source_year": static_source_year,
            "target_years": requested_years,
        }

    if years is not None and source_id != IRS_SOI_SOURCE_ID:
        # Subset before aggregation so population weights are only required for
        # the requested years (PEP county coverage starts in 2010).
        df = df[df["year"].isin(years)].copy()

    input_provenance = read_provenance(input_path)
    if target_geo == "msa" and spec.native_geo == "county":
        if source_id == IRS_SOI_SOURCE_ID:
            result = aggregate_irs_soi_migration_to_msa(
                county_marginals=df,
                pair_path=_irs_soi_pair_path(input_path, input_provenance, output_dir),
                msa_definition_version=msa_definition_version,
                data_root=data_root,
            )
        else:
            mpi_multi_county_rows = (
                input_provenance.extra.get("multi_county_rows")
                if source_id == MPI_SOURCE_ID and input_provenance is not None
                else None
            )
            mpi_msa_rows = (
                _mpi_msa_rows_from_provenance(input_provenance)
                if source_id == MPI_SOURCE_ID and input_provenance is not None
                else None
            )
            if mpi_multi_county_rows and years is not None:
                mpi_multi_county_rows = _expand_mpi_multi_county_rows(
                    mpi_multi_county_rows,
                    years=years,
                )
            if mpi_msa_rows and years is not None:
                mpi_msa_rows = _expand_mpi_multi_county_rows(
                    mpi_msa_rows,
                    years=years,
                )
            result = aggregate_county_covariate_to_msa(
                df,
                measure_columns=list(spec.measure_columns),
                measure_aggregations=spec.measure_aggregations,
                msa_definition_version=msa_definition_version,
                county_population_path=county_population_path,
                data_root=data_root,
                mpi_multi_county_rows=mpi_multi_county_rows,
                mpi_msa_rows=mpi_msa_rows,
            )
    else:
        passthrough_columns = [
            column
            for column in ("source_estimate_year", "static_year_policy")
            if column in df.columns
        ]
        result = df[[*required, *passthrough_columns]].copy()
    if years is not None:
        result = result[result["year"].isin(years)].copy()
    result = _apply_source_specific_derived_columns(source_id, result)
    coverage_policy = _coverage_policy(
        result,
        target_geo=target_geo,
        min_coverage_ratio=min_coverage_ratio,
        drop_below_min_coverage=drop_below_min_coverage,
    )
    if drop_below_min_coverage and coverage_policy["below_threshold_count"]:
        result = result[
            pd.to_numeric(result["coverage_ratio"], errors="coerce") >= min_coverage_ratio
        ].copy()
    result = result.sort_values(["geo_id", "year"]).reset_index(drop=True)

    provenance = ProvenanceBlock(
        geo_type=target_geo,
        extra={
            "dataset_type": "expanded_covariate_panel",
            "source_id": source_id,
            "provider": spec.provider,
            "product": spec.product,
            "native_geo": spec.native_geo,
            "target_geo": target_geo,
            "msa_definition_version": msa_definition_version if target_geo == "msa" else None,
            "county_population_path": (
                str(county_population_path) if county_population_path is not None else None
            ),
            "years": years,
            "static_year_policy": static_year_policy,
            "measure_columns": (
                list(IRS_SOI_MSA_MEASURE_COLUMNS)
                if source_id == IRS_SOI_SOURCE_ID and target_geo == "msa"
                else list(spec.measure_columns)
            ),
            "measure_aggregations": dict(spec.measure_aggregations),
            "derived_measure_columns": (
                list(QCEW_DERIVED_MEASURE_COLUMNS)
                if source_id == QCEW_SOURCE_ID
                else list(CENSUS_BPS_DERIVED_MEASURE_COLUMNS)
                if source_id == CENSUS_BPS_SOURCE_ID
                else []
            ),
            "coverage_policy": coverage_policy,
            "input_path": str(input_path),
            "input_provenance": input_provenance.to_dict() if input_provenance else None,
            "coverage_diagnostics": _coverage_diagnostics(result),
        },
    )
    write_parquet_with_provenance(result, destination, provenance)
    return destination


def _apply_source_specific_derived_columns(
    source_id: str,
    df: pd.DataFrame,
) -> pd.DataFrame:
    if source_id == CENSUS_BPS_SOURCE_ID:
        return _derive_census_bps_mix_adjusted_value_per_unit(df)
    if source_id != QCEW_SOURCE_ID:
        return df
    result = df.copy()
    employment = pd.to_numeric(result["annual_avg_emplvl"], errors="coerce")
    wages = pd.to_numeric(result["total_annual_wages"], errors="coerce")
    employment_positive = employment.where(employment > 0)
    result["annual_avg_weekly_wage"] = wages / (employment_positive * 52.0)
    result["avg_annual_pay"] = wages / employment_positive
    return result


def _derive_census_bps_mix_adjusted_value_per_unit(df: pd.DataFrame) -> pd.DataFrame:
    """Derive a fixed-structure-mix BPS permit valuation-per-unit series."""
    required = {*CENSUS_BPS_CLASS_UNIT_COLUMNS, *CENSUS_BPS_CLASS_VALUE_COLUMNS}
    if not required.issubset(df.columns):
        return df
    result = df.copy()
    group_column = (
        "msa_id"
        if "msa_id" in result.columns
        else "geo_id"
        if "geo_id" in result.columns
        else None
    )
    if group_column is None:
        result[CENSUS_BPS_MIX_ADJUSTED_VALUE_PER_UNIT_COLUMN] = pd.NA
        return result

    derived = pd.Series(pd.NA, index=result.index, dtype="Float64")
    sort_columns = [group_column, "year"] if "year" in result.columns else [group_column]
    for _, group in result.sort_values(sort_columns).groupby(group_column, sort=False):
        class_units = group.loc[:, list(CENSUS_BPS_CLASS_UNIT_COLUMNS)].apply(
            pd.to_numeric, errors="coerce"
        )
        class_values = group.loc[:, list(CENSUS_BPS_CLASS_VALUE_COLUMNS)].apply(
            pd.to_numeric, errors="coerce"
        )
        base_units = _first_positive_bps_class_units(class_units)
        base_total = base_units.sum()
        if not base_total or pd.isna(base_total):
            continue
        base_weights = base_units / base_total
        denominator = class_units.where(class_units > 0)
        value_per_unit = pd.DataFrame(
            class_values.to_numpy() / denominator.to_numpy(),
            index=group.index,
            columns=CENSUS_BPS_CLASS_UNIT_COLUMNS,
        )
        required_class_available = value_per_unit.loc[:, base_weights > 0].notna().all(axis=1)
        weighted_value = value_per_unit.multiply(base_weights.to_numpy()).sum(
            axis=1,
            min_count=1,
        )
        derived.loc[group.index] = weighted_value.where(required_class_available)
    result[CENSUS_BPS_MIX_ADJUSTED_VALUE_PER_UNIT_COLUMN] = derived
    return result


def _first_positive_bps_class_units(class_units: pd.DataFrame) -> pd.Series:
    positive_totals = class_units.sum(axis=1, min_count=1)
    candidates = class_units[positive_totals > 0]
    if candidates.empty:
        return pd.Series(0.0, index=class_units.columns)
    return candidates.iloc[0].fillna(0.0)


def expand_static_covariate_years(
    df: pd.DataFrame,
    *,
    years: list[int],
    source_year: int,
    source_id: str,
) -> pd.DataFrame:
    """Carry a static source estimate to explicit requested panel years."""
    if "year" not in df.columns:
        raise ValueError(f"{source_id} static-year expansion requires a 'year' column.")
    source_rows = df[pd.to_numeric(df["year"], errors="coerce") == source_year].copy()
    if source_rows.empty:
        raise ValueError(
            f"{source_id} static-year expansion expected source year {source_year}, "
            "but no matching rows were found."
        )
    expanded = []
    for year in years:
        yearly = source_rows.copy()
        yearly["year"] = int(year)
        yearly["source_estimate_year"] = int(source_year)
        yearly["static_year_policy"] = "carry_forward"
        expanded.append(yearly)
    return pd.concat(expanded, ignore_index=True)


def _expand_mpi_multi_county_rows(
    rows: list[dict[str, object]],
    *,
    years: list[int],
) -> list[dict[str, object]]:
    expanded: list[dict[str, object]] = []
    for row in rows:
        for year in years:
            yearly = dict(row)
            yearly["year"] = int(year)
            yearly["source_estimate_year"] = MPI_ESTIMATE_YEAR
            yearly["static_year_policy"] = "carry_forward"
            expanded.append(yearly)
    return expanded


def _mpi_msa_rows_from_provenance(provenance: ProvenanceBlock) -> list[dict[str, object]]:
    rows = provenance.extra.get("msa_rows")
    if rows is not None:
        return list(rows)
    skipped_reasons = provenance.extra.get("skipped_reasons") or {}
    msa_row_count = int(skipped_reasons.get("msa_row") or 0)
    if msa_row_count:
        raise ValueError(
            "MPI curated covariate provenance is missing full `msa_rows` metadata "
            f"for {msa_row_count} skipped native MSA row(s). Re-ingest the MPI covariate "
            "with `hhplab ingest covariate --source mpi_unauthorized_immigrants --force` "
            "before aggregating to --target-geo msa."
        )
    return []


def _irs_soi_pair_path(
    county_path: Path,
    provenance: ProvenanceBlock | None,
    output_dir: Path | str | None,
) -> Path:
    if provenance is not None:
        pair_output_path = provenance.extra.get("pair_output_path")
        if pair_output_path:
            return Path(str(pair_output_path))
    if output_dir is not None:
        derived = _irs_soi_pair_path_from_county_path(county_path)
        if derived.exists():
            return derived
        return default_covariate_pair_output_path(IRS_SOI_SOURCE_ID, output_dir=output_dir)
    derived = _irs_soi_pair_path_from_county_path(county_path)
    if derived.exists():
        return derived
    return county_path.with_name(
        default_covariate_pair_output_path(IRS_SOI_SOURCE_ID).name
    )


def _default_covariate_curated_path(
    source_id: str,
    *,
    output_dir: Path | str | None,
) -> Path:
    base = curated_dir("covariates") if output_dir is None else Path(output_dir)
    catalog_default = default_covariate_output_path(source_id, output_dir=output_dir)
    candidates = sorted(base.glob(f"covariate__{source_id}__Y*.parquet"))
    filename_pattern = re.compile(
        rf"covariate__{re.escape(source_id)}__Y\d{{4}}-(?:\d{{4}}|ongoing)\.parquet"
    )
    data_driven = [
        path
        for path in candidates
        if path.name != catalog_default.name and filename_pattern.fullmatch(path.name)
    ]
    if len(data_driven) > 1:
        candidate_names = ", ".join(path.name for path in data_driven)
        raise ValueError(
            f"Multiple curated covariate files found for source '{source_id}': "
            f"{candidate_names}. Pass --curated-path to choose the intended input."
        )
    if data_driven:
        return data_driven[0]
    return catalog_default


def _irs_soi_pair_path_from_county_path(county_path: Path) -> Path:
    return county_path.with_name(
        county_path.name.replace(
            "covariate__irs_soi_migration__",
            "covariate_pairs__irs_soi_migration__",
            1,
        )
    )


def _covariate_panel_path_from_input(
    *,
    source_id: str,
    input_path: Path,
    output_dir: Path | str | None,
) -> Path:
    base = curated_dir("covariates") if output_dir is None else Path(output_dir)
    match = re.search(r"__Y(\d{4})-(\d{4}|ongoing)\.parquet$", input_path.name)
    if match is None:
        return default_covariate_panel_path(source_id, output_dir=output_dir)
    first_year = int(match.group(1))
    last_token = match.group(2)
    last_year = None if last_token == "ongoing" else int(last_token)
    return base / covariate_panel_filename(source_id, first_year, last_year)


def aggregate_irs_soi_migration_to_msa(
    *,
    county_marginals: pd.DataFrame,
    pair_path: Path | str,
    msa_definition_version: str = DEFAULT_MSA_DEFINITION_VERSION,
    data_root: Path | str | None = None,
    msa_county_membership: pd.DataFrame | None = None,
    ct_county_crosswalk: CtPlanningRegionCrosswalk | pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Aggregate IRS SOI county-pair migration flows to MSA-year rows."""
    pair_path = Path(pair_path)
    if not pair_path.exists():
        raise FileNotFoundError(
            f"IRS SOI pair-level covariate file not found: {pair_path}. "
            "Re-run `hhplab ingest covariate --source irs_soi_migration --force` "
            "so the companion covariate_pairs artifact is written."
        )
    pairs = pd.read_parquet(pair_path)
    required_pairs = {
        "year",
        "origin_county_fips",
        "destination_county_fips",
        *IRS_SOI_PAIR_MEASURE_COLUMNS,
    }
    missing_pairs = sorted(required_pairs - set(pairs.columns))
    if missing_pairs:
        raise ValueError(f"IRS SOI pair-level covariate file missing columns: {missing_pairs}")
    required_county = {
        "county_fips",
        "year",
        "other_flows_inflow_returns",
        "other_flows_inflow_exemptions",
        "other_flows_inflow_agi_thousands",
        "other_flows_outflow_returns",
        "other_flows_outflow_exemptions",
        "other_flows_outflow_agi_thousands",
    }
    missing_county = sorted(required_county - set(county_marginals.columns))
    if missing_county:
        raise ValueError(f"IRS SOI county covariate file missing columns: {missing_county}")

    membership = (
        msa_county_membership.copy()
        if msa_county_membership is not None
        else read_msa_county_membership(msa_definition_version, data_root)
    )
    missing_membership = sorted({"msa_id", "county_fips"} - set(membership.columns))
    if missing_membership:
        raise ValueError(
            "MSA county membership is missing required columns: "
            f"{missing_membership}. Run `hhplab generate msa --definition-version "
            f"{msa_definition_version}`."
        )
    membership = membership[["msa_id", "county_fips"]].drop_duplicates().copy()
    membership["msa_id"] = membership["msa_id"].astype("string")
    membership["county_fips"] = membership["county_fips"].astype("string").str.zfill(5)
    county_to_msa = membership.set_index("county_fips")["msa_id"].astype(str).to_dict()
    membership_counts = membership.groupby("msa_id")["county_fips"].nunique().to_dict()
    ct_alignment = _irs_soi_ct_alignment_map(
        membership=membership,
        pairs=pairs,
        county_marginals=county_marginals,
        data_root=data_root,
        ct_county_crosswalk=ct_county_crosswalk,
    )

    pairs = pairs.copy()
    pairs["year"] = pd.to_numeric(pairs["year"], errors="raise").astype(int)
    pairs["origin_county_fips"] = pairs["origin_county_fips"].astype("string").str.zfill(5)
    pairs["destination_county_fips"] = (
        pairs["destination_county_fips"].astype("string").str.zfill(5)
    )
    for column in IRS_SOI_PAIR_MEASURE_COLUMNS:
        pairs[column] = pd.to_numeric(pairs[column], errors="coerce").fillna(0)

    rows_by_key: dict[tuple[str, int], dict[str, object]] = {}
    unmatched_source_counties: set[str] = set()
    for pair in pairs.itertuples(index=False):
        year = int(pair.year)
        origin_options = _irs_soi_msa_county_options(
            str(pair.origin_county_fips),
            county_to_msa=county_to_msa,
            ct_alignment=ct_alignment,
            unmatched_source_counties=unmatched_source_counties,
        )
        destination_options = _irs_soi_msa_county_options(
            str(pair.destination_county_fips),
            county_to_msa=county_to_msa,
            ct_alignment=ct_alignment,
            unmatched_source_counties=unmatched_source_counties,
        )
        values = {
            "returns": float(pair.migration_returns),
            "exemptions": float(pair.migration_exemptions),
            "agi_thousands": float(pair.migration_agi_thousands),
        }
        for origin_county, origin_weight in origin_options:
            origin_msa = county_to_msa.get(origin_county)
            for destination_county, destination_weight in destination_options:
                destination_msa = county_to_msa.get(destination_county)
                weighted_values = {
                    name: value * origin_weight * destination_weight
                    for name, value in values.items()
                }
                if destination_msa is not None and origin_msa != destination_msa:
                    row = _irs_soi_msa_row(
                        rows_by_key,
                        destination_msa,
                        year,
                        membership_counts,
                    )
                    _irs_soi_add_values(row, "inflow", weighted_values)
                if origin_msa is not None and destination_msa != origin_msa:
                    row = _irs_soi_msa_row(rows_by_key, origin_msa, year, membership_counts)
                    _irs_soi_add_values(row, "outflow", weighted_values)
                if origin_msa is not None and origin_msa == destination_msa:
                    row = _irs_soi_msa_row(rows_by_key, origin_msa, year, membership_counts)
                    _irs_soi_add_values(row, "intra_msa", weighted_values)

    _irs_soi_add_suppressed_unallocated(
        rows_by_key,
        county_marginals=county_marginals,
        county_to_msa=county_to_msa,
        membership_counts=membership_counts,
        ct_alignment=ct_alignment,
        unmatched_source_counties=unmatched_source_counties,
    )
    rows = list(rows_by_key.values())
    unmatched_source_county_count = len(unmatched_source_counties)
    for row in rows:
        row["net_returns"] = float(row["inflow_returns"]) - float(row["outflow_returns"])
        row["net_exemptions"] = float(row["inflow_exemptions"]) - float(
            row["outflow_exemptions"]
        )
        row["net_agi_thousands"] = float(row["inflow_agi_thousands"]) - float(
            row["outflow_agi_thousands"]
        )
        known_external = float(row["inflow_returns"]) + float(row["outflow_returns"])
        suppressed = float(row["suppressed_unallocated_inflow_returns"]) + float(
            row["suppressed_unallocated_outflow_returns"]
        )
        denominator = known_external + suppressed
        row["coverage_ratio"] = known_external / denominator if denominator else pd.NA
        row["suppressed_unallocated_returns"] = suppressed
        row["definition_version"] = msa_definition_version
        row["unmatched_source_county_count"] = unmatched_source_county_count
    return pd.DataFrame(rows)


def _irs_soi_ct_alignment_map(
    *,
    membership: pd.DataFrame,
    pairs: pd.DataFrame,
    county_marginals: pd.DataFrame,
    data_root: Path | str | None,
    ct_county_crosswalk: CtPlanningRegionCrosswalk | pd.DataFrame | None,
) -> dict[str, list[tuple[str, float]]]:
    """Return legacy CT county -> planning-region allocation alternatives."""
    source_counties = set(
        pairs["origin_county_fips"].dropna().astype("string").str.zfill(5).astype(str)
    )
    source_counties.update(
        pairs["destination_county_fips"].dropna().astype("string").str.zfill(5).astype(str)
    )
    if "county_fips" in county_marginals.columns:
        source_counties.update(
            county_marginals["county_fips"].dropna().astype("string").str.zfill(5).astype(str)
        )
    return _ct_legacy_to_planning_alignment_map(
        membership=membership,
        source_counties=source_counties,
        data_root=data_root,
        ct_county_crosswalk=ct_county_crosswalk,
    )


def _ct_legacy_to_planning_alignment_map(
    *,
    membership: pd.DataFrame,
    source_counties: set[str],
    data_root: Path | str | None,
    ct_county_crosswalk: CtPlanningRegionCrosswalk | pd.DataFrame | None,
) -> dict[str, list[tuple[str, float]]]:
    """Return legacy CT county -> planning-region area-share alternatives."""
    if not membership["county_fips"].dropna().astype(str).map(is_ct_planning_region_fips).any():
        return {}
    legacy_ct_counties = sorted(
        county for county in source_counties if is_ct_legacy_county_fips(county)
    )
    if not legacy_ct_counties:
        return {}

    if ct_county_crosswalk is None:
        ct_county_crosswalk = build_ct_county_planning_region_crosswalk(
            legacy_county_vintage=CT_LEGACY_COUNTY_VINTAGE,
            planning_region_vintage=CT_PLANNING_REGION_VINTAGE,
            base_dir=data_root,
        )
    mapping = (
        ct_county_crosswalk.mapping.copy()
        if isinstance(ct_county_crosswalk, CtPlanningRegionCrosswalk)
        else ct_county_crosswalk.copy()
    )
    required = {"legacy_county_fips", "planning_region_fips", "legacy_share"}
    missing = sorted(required - set(mapping.columns))
    if missing:
        raise ValueError(f"CT county alignment crosswalk missing columns: {missing}")
    mapping["legacy_county_fips"] = mapping["legacy_county_fips"].astype("string").str.zfill(5)
    mapping["planning_region_fips"] = (
        mapping["planning_region_fips"].astype("string").str.zfill(5)
    )
    mapping["legacy_share"] = pd.to_numeric(mapping["legacy_share"], errors="coerce").fillna(0)

    alignment: dict[str, list[tuple[str, float]]] = {}
    for legacy_county, group in mapping.groupby("legacy_county_fips"):
        options = [
            (str(row.planning_region_fips), float(row.legacy_share))
            for row in group.itertuples(index=False)
            if float(row.legacy_share) > 0
        ]
        if options:
            alignment[str(legacy_county)] = options
    return alignment


def _irs_soi_msa_county_options(
    county_fips: str,
    *,
    county_to_msa: dict[str, str],
    ct_alignment: dict[str, list[tuple[str, float]]],
    unmatched_source_counties: set[str],
) -> list[tuple[str, float]]:
    if county_fips in county_to_msa:
        return [(county_fips, 1.0)]
    aligned = ct_alignment.get(county_fips)
    if aligned:
        return aligned
    if is_ct_legacy_county_fips(county_fips):
        unmatched_source_counties.add(county_fips)
    return [(county_fips, 1.0)]


def _expand_ct_legacy_covariates_to_planning(
    county: pd.DataFrame,
    *,
    population: pd.DataFrame,
    measure_columns: list[str],
    aggregations: dict[str, MeasureAggregation],
    ct_alignment: dict[str, list[tuple[str, float]]],
) -> pd.DataFrame:
    """Replace legacy CT county rows with planning-region rows."""
    if not ct_alignment:
        return county
    rows: list[pd.Series] = []
    for _, row in county.iterrows():
        county_fips = str(row["county_fips"])
        options = ct_alignment.get(county_fips)
        if not options:
            preserved = row.copy()
            preserved["_ct_source_county_fips"] = county_fips
            preserved["_ct_allocation_weight"] = 1.0
            rows.append(preserved)
            continue
        for planning_region_fips, weight in options:
            allocated = row.copy()
            allocated["_ct_source_county_fips"] = county_fips
            allocated["_ct_allocation_weight"] = weight
            allocated["county_fips"] = planning_region_fips
            for column in measure_columns:
                if aggregations[column] in {"extensive_sum", "rate"}:
                    value = pd.to_numeric(pd.Series([allocated[column]]), errors="coerce").iloc[0]
                    allocated[column] = value * weight if pd.notna(value) else pd.NA
            rows.append(allocated)
    expanded = pd.DataFrame(rows).reset_index(drop=True)
    population_lookup = population[["county_fips", "year", "population"]].copy()
    population_lookup["county_fips"] = (
        population_lookup["county_fips"].astype("string").str.zfill(5)
    )
    population_lookup["year"] = pd.to_numeric(
        population_lookup["year"], errors="coerce"
    ).astype("Int64")
    population_lookup["population"] = pd.to_numeric(
        population_lookup["population"], errors="coerce"
    )
    expanded = expanded.merge(
        population_lookup,
        left_on=["_ct_source_county_fips", "year"],
        right_on=["county_fips", "year"],
        how="left",
        suffixes=("", "_source"),
    )
    intensive_columns = [
        column
        for column in measure_columns
        if aggregations[column] == "intensive_pop_weighted_mean"
    ]
    ct_source_counties = set(ct_alignment)
    ct_expanded = expanded["_ct_source_county_fips"].astype(str).isin(ct_source_counties)
    missing_ct_population = ct_expanded & expanded["population"].isna()
    if intensive_columns and missing_ct_population.any():
        sample = (
            expanded.loc[
                missing_ct_population,
                ["_ct_source_county_fips", "year"],
            ]
            .rename(columns={"_ct_source_county_fips": "county_fips"})
            .drop_duplicates()
            .head(10)
            .to_dict(orient="records")
        )
        raise ValueError(
            "County population weights are missing for CT legacy county-years needed "
            "to expand intensive covariates to planning regions; "
            f"sample: {sample}. Provide --county-population-path with matching PEP rows."
        )
    expanded["_ct_allocated_population"] = (
        expanded["population"] * expanded["_ct_allocation_weight"]
    )
    group_columns = [
        column
        for column in expanded.columns
        if column
        not in {
            *measure_columns,
            "_ct_source_county_fips",
            "_ct_allocation_weight",
            "county_fips_source",
            "population",
            "_ct_allocated_population",
        }
    ]
    output_rows: list[dict[str, object]] = []
    for group_key, group in expanded.groupby(group_columns, as_index=False, dropna=False):
        if not isinstance(group_key, tuple):
            group_key = (group_key,)
        output_row = dict(zip(group_columns, group_key, strict=True))
        for column in measure_columns:
            values = pd.to_numeric(group[column], errors="coerce")
            valid = values.notna()
            if aggregations[column] in {"extensive_sum", "rate"}:
                output_row[column] = float(values[valid].sum()) if valid.any() else pd.NA
                continue
            weights = pd.to_numeric(
                group.loc[valid, "_ct_allocated_population"], errors="coerce"
            )
            weight_denominator = weights.sum()
            output_row[column] = (
                float((values[valid] * weights).sum() / weight_denominator)
                if weight_denominator > 0
                else pd.NA
            )
        output_rows.append(output_row)
    return pd.DataFrame(output_rows)


def _expand_ct_legacy_population_to_planning(
    population: pd.DataFrame,
    *,
    ct_alignment: dict[str, list[tuple[str, float]]],
) -> pd.DataFrame:
    """Add area-allocated planning-region PEP rows for legacy CT county years."""
    if not ct_alignment:
        return population
    planning_rows: list[pd.Series] = []
    for _, row in population.iterrows():
        county_fips = str(row["county_fips"])
        options = ct_alignment.get(county_fips)
        if not options:
            continue
        for planning_region_fips, weight in options:
            allocated = row.copy()
            allocated["county_fips"] = planning_region_fips
            value = pd.to_numeric(pd.Series([allocated["population"]]), errors="coerce").iloc[0]
            allocated["population"] = value * weight if pd.notna(value) else pd.NA
            planning_rows.append(allocated)
    if not planning_rows:
        return population
    planning = pd.DataFrame(planning_rows)
    planning["population"] = pd.to_numeric(planning["population"], errors="coerce")
    planning = (
        planning.groupby(["county_fips", "year"], as_index=False)["population"]
        .sum(min_count=1)
        .reset_index(drop=True)
    )
    existing_keys = set(
        population[["county_fips", "year"]].drop_duplicates().itertuples(index=False, name=None)
    )
    planning = planning[
        ~planning[["county_fips", "year"]]
        .apply(tuple, axis=1)
        .isin(existing_keys)
    ].copy()
    if planning.empty:
        return population
    return pd.concat([population, planning], ignore_index=True)


def _irs_soi_msa_row(
    rows_by_key: dict[tuple[str, int], dict[str, object]],
    msa_id: str,
    year: int,
    membership_counts: dict[str, int],
) -> dict[str, object]:
    key = (str(msa_id), int(year))
    row = rows_by_key.get(key)
    if row is not None:
        return row
    row = {
        "geo_type": "msa",
        "geo_id": key[0],
        "msa_id": key[0],
        "year": key[1],
        "membership_county_count": int(membership_counts.get(key[0], 0)),
    }
    for column in IRS_SOI_MSA_MEASURE_COLUMNS:
        row[column] = 0.0
    rows_by_key[key] = row
    return row


def _irs_soi_add_values(
    row: dict[str, object],
    prefix: str,
    values: dict[str, float],
) -> None:
    row[f"{prefix}_returns"] = float(row[f"{prefix}_returns"]) + values["returns"]
    row[f"{prefix}_exemptions"] = (
        float(row[f"{prefix}_exemptions"]) + values["exemptions"]
    )
    row[f"{prefix}_agi_thousands"] = (
        float(row[f"{prefix}_agi_thousands"]) + values["agi_thousands"]
    )


def _irs_soi_add_suppressed_unallocated(
    rows_by_key: dict[tuple[str, int], dict[str, object]],
    *,
    county_marginals: pd.DataFrame,
    county_to_msa: dict[str, str],
    membership_counts: dict[str, int],
    ct_alignment: dict[str, list[tuple[str, float]]],
    unmatched_source_counties: set[str],
) -> None:
    county = county_marginals.copy()
    county["county_fips"] = county["county_fips"].astype("string").str.zfill(5)
    county["year"] = pd.to_numeric(county["year"], errors="raise").astype(int)
    suppressed_map = {
        "suppressed_unallocated_inflow_returns": "other_flows_inflow_returns",
        "suppressed_unallocated_inflow_exemptions": "other_flows_inflow_exemptions",
        "suppressed_unallocated_inflow_agi_thousands": "other_flows_inflow_agi_thousands",
        "suppressed_unallocated_outflow_returns": "other_flows_outflow_returns",
        "suppressed_unallocated_outflow_exemptions": "other_flows_outflow_exemptions",
        "suppressed_unallocated_outflow_agi_thousands": "other_flows_outflow_agi_thousands",
    }
    for source_column in suppressed_map.values():
        county[source_column] = pd.to_numeric(county[source_column], errors="coerce").fillna(0)
    for row in county.itertuples(index=False):
        county_options = _irs_soi_msa_county_options(
            str(row.county_fips),
            county_to_msa=county_to_msa,
            ct_alignment=ct_alignment,
            unmatched_source_counties=unmatched_source_counties,
        )
        for county_fips, weight in county_options:
            msa_id = county_to_msa.get(county_fips)
            if msa_id is None:
                continue
            target = _irs_soi_msa_row(rows_by_key, msa_id, int(row.year), membership_counts)
            for target_column, source_column in suppressed_map.items():
                target[target_column] = float(target[target_column]) + (
                    float(getattr(row, source_column)) * weight
                )


def aggregate_county_covariate_to_msa(
    df: pd.DataFrame,
    *,
    measure_columns: list[str],
    measure_aggregations: dict[str, MeasureAggregation] | None = None,
    msa_definition_version: str = DEFAULT_MSA_DEFINITION_VERSION,
    county_population_path: Path | str | None = None,
    data_root: Path | str | None = None,
    msa_county_membership: pd.DataFrame | None = None,
    msa_definitions: pd.DataFrame | None = None,
    county_population: pd.DataFrame | None = None,
    mpi_multi_county_rows: list[dict[str, object]] | None = None,
    mpi_msa_rows: list[dict[str, object]] | None = None,
    ct_county_crosswalk: CtPlanningRegionCrosswalk | pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Aggregate county-native covariates to MSA-year rows."""
    required = {"county_fips", "year", *measure_columns}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(
            f"County covariate data missing required columns for MSA rollup: {missing}"
        )
    aggregations: dict[str, MeasureAggregation] = {
        column: "intensive_pop_weighted_mean" for column in measure_columns
    }
    if measure_aggregations is not None:
        aggregations.update(measure_aggregations)
    unsupported = sorted(
        {
            aggregation
            for aggregation in aggregations.values()
            if aggregation not in {"extensive_sum", "intensive_pop_weighted_mean", "rate"}
        }
    )
    if unsupported:
        raise ValueError(f"Unsupported covariate MSA aggregation method(s): {unsupported}")

    membership = (
        msa_county_membership.copy()
        if msa_county_membership is not None
        else read_msa_county_membership(msa_definition_version, data_root)
    )
    missing_membership = sorted({"msa_id", "county_fips"} - set(membership.columns))
    if missing_membership:
        raise ValueError(
            "MSA county membership is missing required columns: "
            f"{missing_membership}. Run `hhplab generate msa --definition-version "
            f"{msa_definition_version}`."
        )
    membership = membership[["msa_id", "county_fips"]].drop_duplicates().copy()
    membership["county_fips"] = membership["county_fips"].astype("string").str.zfill(5)
    membership["msa_id"] = membership["msa_id"].astype("string")
    membership_counts = membership.groupby("msa_id")["county_fips"].nunique().to_dict()
    membership_counties_by_msa = (
        membership.groupby("msa_id")["county_fips"]
        .agg(lambda values: set(values.dropna().astype(str)))
        .to_dict()
    )
    definitions = (
        msa_definitions.copy()
        if msa_definitions is not None
        else read_msa_definitions(msa_definition_version, data_root)
        if mpi_msa_rows
        else None
    )

    population = (
        county_population.copy()
        if county_population is not None
        else load_pep_county(county_population_path)
    )
    missing_population = sorted({"county_fips", "year", "population"} - set(population.columns))
    if missing_population:
        raise ValueError(
            "County population weights are missing required columns: "
            f"{missing_population}. Provide a PEP county parquet with county_fips, "
            "year, population."
        )
    population = population[["county_fips", "year", "population"]].copy()
    population["county_fips"] = population["county_fips"].astype("string").str.zfill(5)
    population["year"] = pd.to_numeric(population["year"], errors="coerce").astype("Int64")
    population["population"] = pd.to_numeric(population["population"], errors="coerce")

    optional_lineage_columns = [
        column for column in ("source_estimate_year", "static_year_policy") if column in df.columns
    ]
    county = df[["county_fips", "year", *measure_columns, *optional_lineage_columns]].copy()
    county["county_fips"] = county["county_fips"].astype("string").str.zfill(5)
    county["year"] = pd.to_numeric(county["year"], errors="coerce").astype("Int64")
    source_counties = set(county["county_fips"].dropna().astype(str))
    ct_alignment = _ct_legacy_to_planning_alignment_map(
        membership=membership,
        source_counties=source_counties,
        data_root=data_root,
        ct_county_crosswalk=ct_county_crosswalk,
    )
    if ct_alignment:
        county = _expand_ct_legacy_covariates_to_planning(
            county,
            population=population,
            measure_columns=measure_columns,
            aggregations=aggregations,
            ct_alignment=ct_alignment,
        )
        population = _expand_ct_legacy_population_to_planning(
            population,
            ct_alignment=ct_alignment,
        )
        source_counties = set(county["county_fips"].dropna().astype(str))
    member_counties = set(membership["county_fips"].dropna().astype(str))
    unmatched_source_counties = sorted(source_counties - member_counties)
    county = county.merge(membership, on="county_fips", how="inner")
    county = county.merge(population, on=["county_fips", "year"], how="left")
    if county["population"].isna().any():
        all_extensive_sum = all(
            aggregations[column] == "extensive_sum" for column in measure_columns
        )
        if all_extensive_sum:
            county["population"] = county["population"].fillna(0.0)
        else:
            sample = (
                county.loc[county["population"].isna(), ["county_fips", "year"]]
                .drop_duplicates()
                .head(10)
                .to_dict(orient="records")
            )
            raise ValueError(
                "County population weights are missing for covariate county-years; "
                f"sample: {sample}. Provide --county-population-path with matching PEP rows."
            )

    rows: list[dict[str, object]] = []
    row_by_key: dict[tuple[str, int], dict[str, object]] = {}
    grouped = county.groupby(["msa_id", "year"], dropna=False, sort=True)
    for (msa_id, year), group in grouped:
        key = (str(msa_id), int(year))
        row: dict[str, object] = {
            "geo_type": "msa",
            "geo_id": key[0],
            "msa_id": key[0],
            "year": key[1],
            "population_weight_denominator": float(group["population"].sum()),
            "_covered_county_fips": set(group["county_fips"].dropna().astype(str))
            & membership_counties_by_msa.get(str(msa_id), set()),
            "membership_county_count": int(membership_counts.get(str(msa_id), 0)),
            "definition_version": msa_definition_version,
            "unmatched_source_county_count": len(unmatched_source_counties),
            "mpi_multi_county_source_row_count": 0,
            "mpi_msa_source_row_count": 0,
        }
        row["county_count"] = len(row["_covered_county_fips"])
        for column in optional_lineage_columns:
            values = list(group[column].dropna().unique())
            row[column] = (
                values[0] if len(values) == 1 else ",".join(str(value) for value in values)
            )
        row["coverage_ratio"] = (
            row["county_count"] / row["membership_county_count"]
            if row["membership_county_count"]
            else pd.NA
        )
        for column in measure_columns:
            values = pd.to_numeric(group[column], errors="coerce")
            valid = values.notna()
            denominator = group.loc[valid, "population"].sum()
            aggregation = aggregations[column]
            if aggregation == "extensive_sum":
                row[column] = float(values[valid].sum()) if valid.any() else pd.NA
            elif aggregation == "rate":
                row[column] = (
                    float(values[valid].sum() / denominator) if denominator > 0 else pd.NA
                )
            else:
                row[column] = (
                    float((values[valid] * group.loc[valid, "population"]).sum() / denominator)
                    if denominator > 0
                    else pd.NA
                )
        rows.append(row)
        row_by_key[key] = row

    _apply_mpi_multi_county_msa_rows(
        row_by_key,
        rows,
        mpi_multi_county_rows=mpi_multi_county_rows or [],
        measure_columns=measure_columns,
        membership=membership,
        membership_counts=membership_counts,
        membership_counties_by_msa=membership_counties_by_msa,
        population=population,
        source_counties=source_counties,
        msa_definition_version=msa_definition_version,
    )
    _apply_mpi_msa_rows(
        row_by_key,
        rows,
        mpi_msa_rows=mpi_msa_rows or [],
        measure_columns=measure_columns,
        definitions=definitions,
        membership_counts=membership_counts,
        membership_counties_by_msa=membership_counties_by_msa,
        population=population,
        msa_definition_version=msa_definition_version,
    )

    for row in rows:
        row.pop("_covered_county_fips", None)
    return pd.DataFrame(rows)


def _apply_mpi_multi_county_msa_rows(
    row_by_key: dict[tuple[str, int], dict[str, object]],
    rows: list[dict[str, object]],
    *,
    mpi_multi_county_rows: list[dict[str, object]],
    measure_columns: list[str],
    membership: pd.DataFrame,
    membership_counts: dict[str, int],
    membership_counties_by_msa: dict[str, set[str]],
    population: pd.DataFrame,
    source_counties: set[str],
    msa_definition_version: str,
) -> None:
    if not mpi_multi_county_rows:
        return
    county_to_msa = membership.set_index("county_fips")["msa_id"].astype(str).to_dict()
    population_lookup = population.set_index(["county_fips", "year"])["population"].to_dict()
    for source_row in mpi_multi_county_rows:
        member_counties = sorted(
            {str(value).zfill(5) for value in source_row.get("member_county_fips", [])}
        )
        if not member_counties or any(county in source_counties for county in member_counties):
            continue
        msa_ids = {county_to_msa.get(county) for county in member_counties}
        msa_ids.discard(None)
        if len(msa_ids) != 1:
            continue
        year = int(source_row.get("year", MPI_ESTIMATE_YEAR))
        msa_id = next(iter(msa_ids))
        key = (msa_id, year)
        row = row_by_key.get(key)
        if row is None:
            row = {
                "geo_type": "msa",
                "geo_id": msa_id,
                "msa_id": msa_id,
                "year": year,
                "population_weight_denominator": 0.0,
                "county_count": 0,
                "_covered_county_fips": set(),
                "membership_county_count": int(membership_counts.get(msa_id, 0)),
                "definition_version": msa_definition_version,
                "unmatched_source_county_count": 0,
                "coverage_ratio": pd.NA,
                "mpi_multi_county_source_row_count": 0,
                "mpi_msa_source_row_count": 0,
            }
            for column in measure_columns:
                row[column] = 0.0
            rows.append(row)
            row_by_key[key] = row
        for column in measure_columns:
            row[column] = float(row.get(column) or 0.0) + float(source_row[column])
        covered_counties = row.setdefault("_covered_county_fips", set())
        if not isinstance(covered_counties, set):
            covered_counties = set()
            row["_covered_county_fips"] = covered_counties
        eligible_counties = [
            county
            for county in member_counties
            if county in membership_counties_by_msa.get(msa_id, set())
        ]
        newly_covered_counties = [
            county for county in eligible_counties if county not in covered_counties
        ]
        row["population_weight_denominator"] = float(
            row.get("population_weight_denominator") or 0.0
        ) + float(
            sum(population_lookup.get((county, year), 0.0) for county in newly_covered_counties)
        )
        covered_counties.update(newly_covered_counties)
        row["county_count"] = len(covered_counties)
        row["mpi_multi_county_source_row_count"] = int(
            row.get("mpi_multi_county_source_row_count") or 0
        ) + 1
        row["coverage_ratio"] = (
            min(row["county_count"] / row["membership_county_count"], 1.0)
            if row["membership_county_count"]
            else pd.NA
        )


def _apply_mpi_msa_rows(
    row_by_key: dict[tuple[str, int], dict[str, object]],
    rows: list[dict[str, object]],
    *,
    mpi_msa_rows: list[dict[str, object]],
    measure_columns: list[str],
    definitions: pd.DataFrame | None,
    membership_counts: dict[str, int],
    membership_counties_by_msa: dict[str, set[str]],
    population: pd.DataFrame,
    msa_definition_version: str,
) -> None:
    if not mpi_msa_rows or definitions is None:
        return
    msa_lookup = _unique_msa_name_lookup(definitions)
    population_lookup = population.set_index(["county_fips", "year"])["population"].to_dict()
    for source_row in mpi_msa_rows:
        lookup_keys = _mpi_msa_label_lookup_keys(str(source_row.get("county_label") or ""))
        msa_id = next((msa_lookup[key] for key in lookup_keys if key in msa_lookup), None)
        if msa_id is None:
            continue
        year = int(source_row.get("year", MPI_ESTIMATE_YEAR))
        key = (msa_id, year)
        member_counties = membership_counties_by_msa.get(msa_id, set())
        row = row_by_key.get(key)
        if row is None:
            row = {
                "geo_type": "msa",
                "geo_id": msa_id,
                "msa_id": msa_id,
                "year": year,
                "definition_version": msa_definition_version,
                "unmatched_source_county_count": 0,
                "mpi_multi_county_source_row_count": 0,
            }
            rows.append(row)
            row_by_key[key] = row
        for column in measure_columns:
            row[column] = float(source_row[column])
        for column in ("source_estimate_year", "static_year_policy"):
            if column in source_row:
                row[column] = source_row[column]
        row["_covered_county_fips"] = set(member_counties)
        row["population_weight_denominator"] = float(
            sum(population_lookup.get((county, year), 0.0) for county in member_counties)
        )
        row["county_count"] = len(member_counties)
        row["membership_county_count"] = int(membership_counts.get(msa_id, 0))
        row["coverage_ratio"] = 1.0 if row["membership_county_count"] else pd.NA
        row["mpi_msa_source_row_count"] = int(row.get("mpi_msa_source_row_count") or 0) + 1


def _unique_msa_name_lookup(definitions: pd.DataFrame) -> dict[str, str]:
    if "msa_id" not in definitions.columns or "msa_name" not in definitions.columns:
        return {}
    keyed = definitions[["msa_id", "msa_name"]].dropna().copy()
    keyed["name_key"] = keyed["msa_name"].map(_msa_definition_name_key)
    keyed["state_keys"] = keyed["msa_name"].map(_msa_definition_state_keys)
    counts = keyed["name_key"].value_counts()
    unique = keyed[keyed["name_key"].map(counts) == 1]
    lookup = dict(zip(unique["name_key"], unique["msa_id"].astype(str), strict=False))
    state_key_rows = [
        {"name_key": state_key, "msa_id": row.msa_id}
        for row in keyed.itertuples(index=False)
        for state_key in row.state_keys
    ]
    if state_key_rows:
        state_keyed = pd.DataFrame(state_key_rows)
        state_counts = state_keyed["name_key"].value_counts()
        state_unique = state_keyed[state_keyed["name_key"].map(state_counts) == 1]
        lookup.update(
            dict(zip(state_unique["name_key"], state_unique["msa_id"].astype(str), strict=False))
        )
    return lookup


def _mpi_msa_label_lookup_keys(label: str) -> list[str]:
    marker = " MSA"
    if marker not in label:
        return []
    base, suffix = label.split(marker, 1)
    name_key = _normalize_msa_name_key(base)
    state_keys = [
        _msa_state_name_key(name_key, state_abbrev)
        for state_abbrev in _state_abbrevs_from_mpi_label_suffix(suffix)
    ]
    return [*state_keys, name_key]


def _msa_definition_name_key(name: str) -> str:
    base = name.split(",", 1)[0]
    return _normalize_msa_name_key(base)


def _msa_definition_state_keys(name: str) -> list[str]:
    if "," not in name:
        return []
    name_key = _msa_definition_name_key(name)
    state_suffix = name.rsplit(",", 1)[1]
    return [
        _msa_state_name_key(name_key, state_abbrev)
        for state_abbrev in re.findall(r"\b[A-Z]{2}\b", state_suffix.upper())
    ]


def _state_abbrevs_from_mpi_label_suffix(suffix: str) -> list[str]:
    cleaned = suffix.replace(",++", " ").replace("++", " ")
    state_abbrevs: list[str] = []
    for state_name in re.split(r"[-/;,]", cleaned):
        normalized = " ".join(state_name.upper().replace(".", "").split())
        state_abbrev = STATE_NAME_TO_ABBREV.get(normalized)
        if state_abbrev is not None:
            state_abbrevs.append(state_abbrev)
    return state_abbrevs


def _msa_state_name_key(name_key: str, state_abbrev: str) -> str:
    return f"{name_key}__state_{state_abbrev}"


def _normalize_msa_name_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _coverage_diagnostics(df: pd.DataFrame) -> dict[str, object]:
    if df.empty:
        return {"row_count": 0}
    diagnostics: dict[str, object] = {"row_count": int(len(df))}
    if "coverage_ratio" in df.columns:
        coverage = pd.to_numeric(df["coverage_ratio"], errors="coerce")
        diagnostics["min_coverage_ratio"] = (
            float(coverage.min()) if coverage.notna().any() else None
        )
        diagnostics["partial_target_count"] = int((coverage < 1.0).sum())
    if "unmatched_source_county_count" in df.columns:
        diagnostics["unmatched_source_county_count"] = int(
            pd.to_numeric(df["unmatched_source_county_count"], errors="coerce").max()
        )
    if "year" in df.columns:
        metadata_columns = {
            "geo_type",
            "geo_id",
            "msa_id",
            "year",
            "definition_version",
            "coverage_ratio",
            "membership_county_count",
            "county_count",
            "population_weight_denominator",
            "unmatched_source_county_count",
            "mpi_multi_county_source_row_count",
            "mpi_msa_source_row_count",
        }
        value_columns = [
            column
            for column in df.columns
            if column not in metadata_columns
            and pd.api.types.is_numeric_dtype(pd.to_numeric(df[column], errors="coerce"))
        ]
        if value_columns:
            per_year_counts: dict[str, dict[str, int]] = {}
            for year, group in df.groupby("year", dropna=True):
                per_year_counts[str(int(year))] = {
                    column: int(group[column].notna().sum()) for column in value_columns
                }
            diagnostics["per_year_non_null_counts"] = per_year_counts
    return diagnostics


def _coverage_policy(
    df: pd.DataFrame,
    *,
    target_geo: str,
    min_coverage_ratio: float | None,
    drop_below_min_coverage: bool,
) -> dict[str, object] | None:
    if target_geo != "msa" or "coverage_ratio" not in df.columns:
        return None
    if min_coverage_ratio is None:
        return {
            "min_coverage_ratio": None,
            "action": "keep",
            "below_threshold_count": 0,
            "dropped_row_count": 0,
        }
    coverage = pd.to_numeric(df["coverage_ratio"], errors="coerce")
    below_threshold = coverage < min_coverage_ratio
    below_count = int(below_threshold.sum())
    return {
        "min_coverage_ratio": float(min_coverage_ratio),
        "action": "drop" if drop_below_min_coverage else "warn",
        "below_threshold_count": below_count,
        "dropped_row_count": below_count if drop_below_min_coverage else 0,
        "min_observed_coverage_ratio": (
            float(coverage.min()) if coverage.notna().any() else None
        ),
    }
