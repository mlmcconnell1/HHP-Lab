"""CDC provisional county overdose death normalization and MSA rollup."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from hhplab.msa import DEFINITION_VERSION, read_msa_county_membership
from hhplab.naming import cdc_overdose_county_filename, cdc_overdose_msa_filename
from hhplab.paths import curated_dir
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance

DEFAULT_RAW_OVERDOSE_CSV = (
    Path("data")
    / "raw"
    / "cdc"
    / "VSRR_Provisional_County-Level_Drug_Overdose_Death_Counts.csv"
)
DEFAULT_MIN_COVERAGE = 0.0

RAW_YEAR_COLUMN = "Year"
RAW_MONTH_COLUMN = "Month"
RAW_FIPS_COLUMN = "FIPS"
RAW_DEATHS_COLUMN = "Provisional Drug Overdose Deaths"
RAW_MONTH_ENDING_COLUMN = "MonthEndingDate"
RAW_DATA_AS_OF_COLUMN = "Data as of"
RAW_COUNTY_NAME_COLUMN = "COUNTYNAME"
RAW_STATE_NAME_COLUMN = "STATE_NAME"
RAW_STATE_ABBREV_COLUMN = "ST_ABBREV"

CDC_OVERDOSE_COUNTY_COLUMNS: tuple[str, ...] = (
    "county_fips",
    "year",
    "reference_month",
    "reference_date",
    "overdose_deaths_12mo",
    "overdose_deaths_suppressed",
    "state_abbrev",
    "state_name",
    "county_name",
    "data_as_of",
)

CDC_OVERDOSE_MSA_COLUMNS: tuple[str, ...] = (
    "msa_id",
    "year",
    "reference_month",
    "reference_date",
    "overdose_deaths_12mo",
    "coverage_ratio",
    "county_coverage_ratio",
    "covered_population",
    "total_population",
    "population_weight_denominator",
    "county_count",
    "county_expected",
    "missing_counties",
    "suppressed_counties",
    "definition_version",
)


def _validate_required_columns(df: pd.DataFrame, required: tuple[str, ...], label: str) -> None:
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise ValueError(
            f"{label} missing required column(s): {', '.join(missing)}. "
            "Use the CDC VSRR provisional county overdose CSV export."
        )


def _normalize_county_fips(series: pd.Series) -> pd.Series:
    return series.astype("string").str.replace(r"\.0$", "", regex=True).str.zfill(5)


def _parse_overdose_deaths_12mo(series: pd.Series) -> pd.Series:
    normalized = series.astype("string").str.replace(",", "", regex=False)
    return pd.to_numeric(normalized, errors="coerce")


def load_county_overdose_csv(path: Path | str = DEFAULT_RAW_OVERDOSE_CSV) -> pd.DataFrame:
    """Load the CDC county overdose CSV as raw strings where needed."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(
            f"CDC overdose CSV not found: {path}. "
            "Place the VSRR county overdose export under data/raw/cdc/."
        )

    df = pd.read_csv(
        path,
        dtype={
            RAW_FIPS_COLUMN: "string",
            RAW_STATE_ABBREV_COLUMN: "string",
            RAW_STATE_NAME_COLUMN: "string",
            RAW_COUNTY_NAME_COLUMN: "string",
        },
        low_memory=False,
    )
    required = (
        RAW_YEAR_COLUMN,
        RAW_MONTH_COLUMN,
        RAW_FIPS_COLUMN,
        RAW_DEATHS_COLUMN,
        RAW_MONTH_ENDING_COLUMN,
    )
    _validate_required_columns(df, required, "CDC overdose CSV")
    return df


def normalize_county_overdose(
    raw_df: pd.DataFrame,
    *,
    years: list[int] | tuple[int, ...] | None = None,
    reference_month: int = 1,
) -> pd.DataFrame:
    """Normalize county overdose rows, using January as the annual value by default.

    CDC's provisional county export is monthly. For annual HHP-Lab panels,
    January rows are treated as the requested year because their monthly
    value represents the trailing 12-month window.
    """
    if reference_month < 1 or reference_month > 12:
        raise ValueError("reference_month must be between 1 and 12.")

    selected = raw_df.loc[raw_df[RAW_MONTH_COLUMN].astype(int) == reference_month].copy()
    if years is not None:
        year_set = {int(year) for year in years}
        selected = selected.loc[selected[RAW_YEAR_COLUMN].astype(int).isin(year_set)].copy()

    if selected.empty:
        raise ValueError(
            "No CDC overdose rows matched the requested year/month selection. "
            "Check --years and the raw CSV coverage."
        )

    selected["county_fips"] = _normalize_county_fips(selected[RAW_FIPS_COLUMN])
    selected["year"] = selected[RAW_YEAR_COLUMN].astype(int)
    selected["reference_month"] = selected[RAW_MONTH_COLUMN].astype(int)
    selected["reference_date"] = pd.to_datetime(selected[RAW_MONTH_ENDING_COLUMN]).dt.date.astype(
        "string"
    )
    selected["overdose_deaths_12mo"] = _parse_overdose_deaths_12mo(selected[RAW_DEATHS_COLUMN])
    selected["overdose_deaths_suppressed"] = selected["overdose_deaths_12mo"].isna()

    for raw_col, out_col in (
        (RAW_STATE_ABBREV_COLUMN, "state_abbrev"),
        (RAW_STATE_NAME_COLUMN, "state_name"),
        (RAW_COUNTY_NAME_COLUMN, "county_name"),
        (RAW_DATA_AS_OF_COLUMN, "data_as_of"),
    ):
        selected[out_col] = selected[raw_col].astype("string") if raw_col in selected else pd.NA

    result = selected.loc[:, CDC_OVERDOSE_COUNTY_COLUMNS].copy()
    result = result.drop_duplicates(subset=["county_fips", "year", "reference_month"])
    return result.sort_values(["year", "county_fips"]).reset_index(drop=True)


def ingest_county_overdose(
    *,
    raw_path: Path | str = DEFAULT_RAW_OVERDOSE_CSV,
    years: list[int] | tuple[int, ...] | None = None,
    reference_month: int = 1,
    county_vintage: str | int = "2023",
    output_dir: Path | str | None = None,
) -> tuple[pd.DataFrame, Path]:
    """Normalize CDC county overdose data and persist a curated county artifact."""
    county_df = normalize_county_overdose(
        load_county_overdose_csv(raw_path),
        years=years,
        reference_month=reference_month,
    )
    output_dir = Path(output_dir) if output_dir is not None else curated_dir("cdc")
    start_year = int(county_df["year"].min())
    end_year = int(county_df["year"].max())
    output_path = output_dir / cdc_overdose_county_filename(
        start_year,
        end_year,
        county_vintage,
    )
    provenance = ProvenanceBlock(
        county_vintage=str(county_vintage),
        geo_type="county",
        extra={
            "dataset_type": "cdc_overdose_county",
            "source": "cdc_vsrr_provisional_county_overdose",
            "raw_path": str(raw_path),
            "reference_month": reference_month,
            "temporal_alignment": "january_trailing_12_months",
            "measure_columns": ["overdose_deaths_12mo"],
        },
    )
    write_parquet_with_provenance(county_df, output_path, provenance)
    return county_df, output_path


def _normalize_msa_membership(msa_county_membership: pd.DataFrame) -> pd.DataFrame:
    required = ("msa_id", "county_fips")
    _validate_required_columns(msa_county_membership, required, "MSA county membership")
    columns = ["msa_id", "county_fips"]
    if "cbsa_code" in msa_county_membership.columns:
        columns.append("cbsa_code")
    membership = msa_county_membership[columns].drop_duplicates().copy()
    membership["msa_id"] = membership["msa_id"].astype(str).str.zfill(5)
    membership["county_fips"] = _normalize_county_fips(membership["county_fips"])
    return membership


def _normalize_county_population(county_population: pd.DataFrame) -> pd.DataFrame:
    required = ("county_fips", "year", "population")
    _validate_required_columns(county_population, required, "County population weights")
    population = county_population.loc[:, required].drop_duplicates().copy()
    population["county_fips"] = _normalize_county_fips(population["county_fips"])
    population["year"] = population["year"].astype(int)
    population["population"] = pd.to_numeric(population["population"], errors="coerce")
    if population["population"].isna().any():
        sample = (
            population.loc[population["population"].isna(), ["county_fips", "year"]]
            .head(10)
            .to_dict(orient="records")
        )
        raise ValueError(f"County population weights contain non-numeric values; sample: {sample}.")
    return population


def aggregate_county_overdose_to_msa(
    county_df: pd.DataFrame,
    msa_county_membership: pd.DataFrame | None = None,
    county_population: pd.DataFrame | None = None,
    *,
    definition_version: str = DEFINITION_VERSION,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
) -> pd.DataFrame:
    """Aggregate additive county overdose counts to MSAs by county membership."""
    if min_coverage < 0 or min_coverage > 1:
        raise ValueError("min_coverage must be between 0 and 1.")
    if county_population is None:
        raise ValueError(
            "CDC MSA overdose coverage_ratio requires county population weights. "
            "Provide county_population with county_fips, year, population columns "
            "or use hhplab aggregate cdc-overdose with PEP county population available."
        )
    _validate_required_columns(
        county_df,
        ("county_fips", "year", "reference_month", "reference_date", "overdose_deaths_12mo"),
        "CDC county overdose data",
    )

    membership = (
        _normalize_msa_membership(msa_county_membership)
        if msa_county_membership is not None
        else _normalize_msa_membership(read_msa_county_membership(definition_version))
    )
    population = _normalize_county_population(county_population)
    county = county_df.copy()
    county["county_fips"] = _normalize_county_fips(county["county_fips"])
    county["year"] = county["year"].astype(int)
    county["overdose_deaths_12mo"] = pd.to_numeric(
        county["overdose_deaths_12mo"],
        errors="coerce",
    )

    joined = membership.merge(county, on="county_fips", how="left")
    years = sorted(county["year"].dropna().astype(int).unique())
    rows: list[dict[str, object]] = []

    for msa_id, msa_membership in membership.groupby("msa_id", sort=True):
        expected_counties = set(msa_membership["county_fips"])
        expected_count = len(expected_counties)
        for year in years:
            group = joined.loc[(joined["msa_id"] == msa_id) & (joined["year"] == year)].copy()
            available = set(group.loc[group["overdose_deaths_12mo"].notna(), "county_fips"])
            present = set(group["county_fips"])
            missing = expected_counties - present
            suppressed = present - available
            population_rows = population.loc[
                (population["year"] == year)
                & (population["county_fips"].isin(expected_counties))
            ]
            population_counties = set(population_rows["county_fips"])
            missing_population = sorted(expected_counties - population_counties)
            if missing_population:
                raise ValueError(
                    "County population weights are missing for CDC overdose MSA "
                    f"coverage in {msa_id} year {year}: {', '.join(missing_population)}."
                )

            total_population = float(population_rows["population"].sum())
            covered_population = float(
                population_rows.loc[
                    population_rows["county_fips"].isin(available), "population"
                ].sum()
            )
            coverage_ratio = (
                covered_population / total_population if total_population > 0 else pd.NA
            )
            county_coverage_ratio = len(available) / expected_count if expected_count else 0.0
            deaths = group["overdose_deaths_12mo"].sum(min_count=1)
            if pd.notna(coverage_ratio) and coverage_ratio < min_coverage:
                deaths = pd.NA

            rows.append(
                {
                    "msa_id": msa_id,
                    "year": year,
                    "reference_month": int(group["reference_month"].dropna().iloc[0])
                    if not group["reference_month"].dropna().empty
                    else pd.NA,
                    "reference_date": group["reference_date"].dropna().iloc[0]
                    if not group["reference_date"].dropna().empty
                    else pd.NA,
                    "overdose_deaths_12mo": deaths,
                    "coverage_ratio": coverage_ratio,
                    "county_coverage_ratio": county_coverage_ratio,
                    "covered_population": covered_population,
                    "total_population": total_population,
                    "population_weight_denominator": total_population,
                    "county_count": len(available),
                    "county_expected": expected_count,
                    "missing_counties": ",".join(sorted(missing)),
                    "suppressed_counties": ",".join(sorted(suppressed)),
                    "definition_version": definition_version,
                }
            )

    result = pd.DataFrame(rows)
    return result.loc[:, CDC_OVERDOSE_MSA_COLUMNS].sort_values(["msa_id", "year"]).reset_index(
        drop=True
    )


def write_msa_overdose(
    msa_df: pd.DataFrame,
    *,
    definition_version: str = DEFINITION_VERSION,
    county_vintage: str | int = "2023",
    min_coverage: float = DEFAULT_MIN_COVERAGE,
    input_artifacts: dict[str, str] | None = None,
    output_dir: Path | str | None = None,
) -> Path:
    """Persist an MSA overdose rollup with embedded provenance."""
    output_dir = Path(output_dir) if output_dir is not None else curated_dir("cdc")
    start_year = int(msa_df["year"].min())
    end_year = int(msa_df["year"].max())
    output_path = output_dir / cdc_overdose_msa_filename(
        start_year,
        end_year,
        definition_version,
        county_vintage,
    )
    provenance = ProvenanceBlock(
        county_vintage=str(county_vintage),
        geo_type="msa",
        definition_version=definition_version,
        extra={
            "dataset_type": "cdc_overdose_msa",
            "source": "cdc_vsrr_provisional_county_overdose",
            "row_grain": "msa_id x year",
            "aggregation": "sum_county_member_january_trailing_12_month_counts",
            "min_coverage": min_coverage,
            "coverage_ratio_basis": "county_population",
            "measure_columns": ["overdose_deaths_12mo"],
            "input_artifacts": input_artifacts or {},
        },
    )
    return write_parquet_with_provenance(msa_df, output_path, provenance)


def ingest_and_aggregate_overdose_to_msa(
    *,
    raw_path: Path | str = DEFAULT_RAW_OVERDOSE_CSV,
    years: list[int] | tuple[int, ...] | None = None,
    reference_month: int = 1,
    definition_version: str = DEFINITION_VERSION,
    county_vintage: str | int = "2023",
    min_coverage: float = DEFAULT_MIN_COVERAGE,
    county_population: pd.DataFrame | None = None,
    output_dir: Path | str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, Path, Path]:
    """Normalize CDC county data and write county plus MSA artifacts."""
    if county_population is None:
        from hhplab.pep.pep_aggregate import load_pep_county

        county_population = load_pep_county()
    county_df, county_path = ingest_county_overdose(
        raw_path=raw_path,
        years=years,
        reference_month=reference_month,
        county_vintage=county_vintage,
        output_dir=output_dir,
    )
    membership = read_msa_county_membership(definition_version)
    msa_df = aggregate_county_overdose_to_msa(
        county_df,
        membership,
        county_population,
        definition_version=definition_version,
        min_coverage=min_coverage,
    )
    msa_path = write_msa_overdose(
        msa_df,
        definition_version=definition_version,
        county_vintage=county_vintage,
        min_coverage=min_coverage,
        input_artifacts={"county_overdose": str(county_path)},
        output_dir=output_dir,
    )
    return county_df, msa_df, county_path, msa_path
