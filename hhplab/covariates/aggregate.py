"""Panel-ready aggregation helpers for expanded covariates."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from hhplab.covariates.catalog import covariate_source_spec
from hhplab.covariates.ingest import default_covariate_output_path
from hhplab.covariates.mpi_contract import MPI_ESTIMATE_YEAR, MPI_SOURCE_ID
from hhplab.msa import DEFINITION_VERSION as DEFAULT_MSA_DEFINITION_VERSION
from hhplab.msa.msa_io import read_msa_county_membership
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

    if target_geo == "msa":
        result = aggregate_county_covariate_to_msa(
            df,
            measure_columns=list(spec.measure_columns),
            msa_definition_version=msa_definition_version,
            county_population_path=county_population_path,
            data_root=data_root,
        )
    else:
        result = df[required].copy()
    if years is not None:
        result = result[result["year"].isin(years)].copy()
    result = result.sort_values(["geo_id", "year"]).reset_index(drop=True)

    input_provenance = read_provenance(input_path)
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


def aggregate_county_covariate_to_msa(
    df: pd.DataFrame,
    *,
    measure_columns: list[str],
    msa_definition_version: str = DEFAULT_MSA_DEFINITION_VERSION,
    county_population_path: Path | str | None = None,
    data_root: Path | str | None = None,
    msa_county_membership: pd.DataFrame | None = None,
    county_population: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Population-weight county-native covariates to MSA-year rows."""
    required = {"county_fips", "year", *measure_columns}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(
            f"County covariate data missing required columns for MSA rollup: {missing}"
        )

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
    grouped = county.groupby(["msa_id", "year"], dropna=False, sort=True)
    for (msa_id, year), group in grouped:
        row: dict[str, object] = {
            "geo_type": "msa",
            "geo_id": str(msa_id),
            "msa_id": str(msa_id),
            "year": int(year),
            "population_weight_denominator": float(group["population"].sum()),
            "county_count": int(group["county_fips"].nunique()),
            "membership_county_count": int(membership_counts.get(str(msa_id), 0)),
            "definition_version": msa_definition_version,
            "unmatched_source_county_count": len(unmatched_source_counties),
        }
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
            row[column] = (
                float((values[valid] * group.loc[valid, "population"]).sum() / denominator)
                if denominator > 0
                else pd.NA
            )
        rows.append(row)

    return pd.DataFrame(rows)


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
