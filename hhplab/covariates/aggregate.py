"""Panel-ready aggregation helpers for expanded covariates."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from hhplab.covariates.catalog import MeasureAggregation, covariate_source_spec
from hhplab.covariates.ingest import default_covariate_output_path
from hhplab.covariates.mpi_contract import MPI_ESTIMATE_YEAR, MPI_SOURCE_ID
from hhplab.msa import DEFINITION_VERSION as DEFAULT_MSA_DEFINITION_VERSION
from hhplab.msa.msa_io import read_msa_county_membership, read_msa_definitions
from hhplab.naming import covariate_panel_filename
from hhplab.paths import curated_dir
from hhplab.pep.pep_aggregate import load_pep_county
from hhplab.provenance import ProvenanceBlock, read_provenance, write_parquet_with_provenance

FREEZING_C = 0.0
EMERGENCY_SHELTER_ACTIVATION_C = 4.4
PRISM_TMIN_SOURCE_ID = "prism_tmin_january"


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
    if target_geo == "msa" and spec.native_geo != "county":
        raise ValueError(
            f"Covariate source '{source_id}' is native to {spec.native_geo} geography. "
            "Only county-native covariates can be population-weighted to --target-geo msa."
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
        else default_covariate_output_path(source_id, output_dir=output_dir)
    )
    if not input_path.exists():
        raise FileNotFoundError(
            f"Curated covariate file not found: {input_path}. "
            f"Run `hhplab ingest covariate --source {source_id}` first."
        )
    destination = (
        Path(output_path)
        if output_path is not None
        else default_covariate_panel_path(source_id, output_dir=output_dir)
    )
    if destination.exists() and not force:
        return destination

    df = pd.read_parquet(input_path)
    if source_id == PRISM_TMIN_SOURCE_ID:
        df = derive_prism_temperature_basis(df)
    required = ["geo_type", "geo_id", "year", *spec.measure_columns]
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"Curated covariate file missing required columns: {missing}")
    static_year_policy = None
    if source_id == MPI_SOURCE_ID and years is not None:
        df = expand_static_covariate_years(
            df,
            years=years,
            source_year=MPI_ESTIMATE_YEAR,
            source_id=source_id,
        )
        static_year_policy = {
            "policy": "carry_forward_static_estimate_to_requested_years",
            "source_year": MPI_ESTIMATE_YEAR,
            "target_years": list(years),
        }

    input_provenance = read_provenance(input_path)
    if target_geo == "msa":
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
        result = df[required].copy()
    if years is not None:
        result = result[result["year"].isin(years)].copy()
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
            "measure_columns": list(spec.measure_columns),
            "measure_aggregations": dict(spec.measure_aggregations),
            "coverage_policy": coverage_policy,
            "input_path": str(input_path),
            "input_provenance": input_provenance.to_dict() if input_provenance else None,
            "coverage_diagnostics": _coverage_diagnostics(result),
        },
    )
    write_parquet_with_provenance(result, destination, provenance)
    return destination


def derive_prism_temperature_basis(df: pd.DataFrame) -> pd.DataFrame:
    """Add policy-threshold PRISM tmin basis columns when ``tmin_c`` is present."""
    if "tmin_c" not in df.columns:
        return df
    result = df.copy()
    tmin = pd.to_numeric(result["tmin_c"], errors="coerce")
    result["tmin_below_freezing"] = tmin.clip(upper=FREEZING_C)
    result["tmin_code_blue_band"] = tmin.clip(
        lower=FREEZING_C,
        upper=EMERGENCY_SHELTER_ACTIVATION_C,
    )
    result["tmin_above_code_blue"] = (tmin - EMERGENCY_SHELTER_ACTIVATION_C).clip(lower=0.0)
    return result


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
    if rows:
        return list(rows)
    return [
        row
        for row in provenance.extra.get("skipped_preview", [])
        if row.get("exclusion_reason") == "msa_row"
    ]


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
    member_counties = set(membership["county_fips"].dropna().astype(str))
    unmatched_source_counties = sorted(source_counties - member_counties)
    county = county.merge(membership, on="county_fips", how="inner")
    county = county.merge(population, on=["county_fips", "year"], how="left")
    if county["population"].isna().any():
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
        name_key = _mpi_msa_label_key(str(source_row.get("county_label") or ""))
        if not name_key:
            continue
        msa_id = msa_lookup.get(name_key)
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
    counts = keyed["name_key"].value_counts()
    unique = keyed[keyed["name_key"].map(counts) == 1]
    return dict(zip(unique["name_key"], unique["msa_id"].astype(str), strict=False))


def _mpi_msa_label_key(label: str) -> str | None:
    marker = " MSA"
    if marker not in label:
        return None
    return _normalize_msa_name_key(label.split(marker, 1)[0])


def _msa_definition_name_key(name: str) -> str:
    base = name.split(",", 1)[0]
    return _normalize_msa_name_key(base)


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
