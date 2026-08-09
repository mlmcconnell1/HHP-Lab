"""Conservative DOJ sanctuary jurisdiction to MSA matching."""

from __future__ import annotations

import pandas as pd

from .contracts import (
    DOJ_LISTED_CITIES,
    DOJ_LISTED_COUNTIES,
    DOJ_LISTED_STATES,
    SANCTUARY_MSA_MATCH_COLUMNS,
)


def _join_labels(values: list[str] | pd.Series) -> str:
    labels = sorted({str(value) for value in values if pd.notna(value) and str(value)})
    return "; ".join(labels)


def _build_county_designation_lookup() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "county_fips": [item.county_fips for item in DOJ_LISTED_COUNTIES],
            "county_label": [item.label for item in DOJ_LISTED_COUNTIES],
        }
    )


def _build_city_designation_lookup() -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for item in DOJ_LISTED_CITIES:
        for county_fips in item.county_fips:
            rows.append(
                {
                    "county_fips": county_fips,
                    "city_label": item.label,
                    "city_match_note": item.match_note,
                }
            )
    return pd.DataFrame(rows)


def _build_county_exposure(
    msa_county_membership: pd.DataFrame,
    county_population: pd.DataFrame,
    *,
    population_year: int,
) -> pd.DataFrame:
    """Return one row per MSA with population-weighted sanctuary exposure."""
    required_membership = {"msa_id", "county_fips", "state_name"}
    missing_membership = sorted(required_membership - set(msa_county_membership.columns))
    if missing_membership:
        raise ValueError(
            "MSA county membership missing required column(s): "
            f"{', '.join(missing_membership)}."
        )

    required_population = {"county_fips", "year", "population"}
    missing_population = sorted(required_population - set(county_population.columns))
    if missing_population:
        raise ValueError(
            "County population data missing required column(s): "
            f"{', '.join(missing_population)}."
        )

    membership = msa_county_membership.loc[:, ["msa_id", "county_fips", "state_name"]].copy()
    membership["msa_id"] = membership["msa_id"].astype(str).str.zfill(5)
    membership["county_fips"] = membership["county_fips"].astype(str).str.zfill(5)
    membership["state_name"] = membership["state_name"].astype(str)

    population = county_population.loc[
        county_population["year"].astype("int64") == population_year,
        ["county_fips", "population"],
    ].copy()
    population["county_fips"] = population["county_fips"].astype(str).str.zfill(5)
    population["population"] = pd.to_numeric(population["population"], errors="coerce")
    population = population.dropna(subset=["population"])

    county_designations = set(_build_county_designation_lookup()["county_fips"].tolist())
    city_designations = set(_build_city_designation_lookup()["county_fips"].tolist())
    membership["state_sanctuary_county"] = membership["state_name"].isin(DOJ_LISTED_STATES)
    membership["county_sanctuary_county"] = membership["county_fips"].isin(county_designations)
    membership["city_sanctuary_county"] = membership["county_fips"].isin(city_designations)
    membership["sanctuary_county"] = membership[
        ["state_sanctuary_county", "county_sanctuary_county", "city_sanctuary_county"]
    ].any(axis=1)

    weighted = membership.merge(population, on="county_fips", how="left")
    weighted["population"] = weighted["population"].fillna(0.0)
    weighted["sanctuary_population"] = weighted["population"].where(
        weighted["sanctuary_county"],
        0.0,
    )
    exposure = (
        weighted.groupby("msa_id", as_index=False)
        .agg(
            doj_sanctuary_population=("sanctuary_population", "sum"),
            doj_sanctuary_population_denominator=("population", "sum"),
        )
        .sort_values("msa_id")
    )
    denominator = exposure["doj_sanctuary_population_denominator"]
    exposure["doj_sanctuary_population_share"] = (
        exposure["doj_sanctuary_population"].where(denominator > 0, 0.0)
        / denominator.where(denominator > 0, 1.0)
    )
    exposure["doj_sanctuary_population_year"] = population_year
    return exposure


def build_sanctuary_msa_matches(
    msa_definitions: pd.DataFrame,
    msa_county_membership: pd.DataFrame,
) -> pd.DataFrame:
    """Build conservative DOJ sanctuary jurisdiction matches at MSA grain."""
    required_definitions = {"msa_id", "cbsa_code", "msa_name"}
    missing_definitions = sorted(required_definitions - set(msa_definitions.columns))
    if missing_definitions:
        raise ValueError(
            "MSA definitions missing required column(s): "
            f"{', '.join(missing_definitions)}."
        )

    required_membership = {"msa_id", "county_fips", "state_name"}
    missing_membership = sorted(required_membership - set(msa_county_membership.columns))
    if missing_membership:
        raise ValueError(
            "MSA county membership missing required column(s): "
            f"{', '.join(missing_membership)}."
        )

    definitions = msa_definitions[["msa_id", "cbsa_code", "msa_name"]].drop_duplicates().copy()
    definitions["msa_id"] = definitions["msa_id"].astype(str).str.zfill(5)
    definitions["cbsa_code"] = definitions["cbsa_code"].astype(str).str.zfill(5)

    membership = msa_county_membership.copy()
    membership["msa_id"] = membership["msa_id"].astype(str).str.zfill(5)
    membership["county_fips"] = membership["county_fips"].astype(str).str.zfill(5)
    membership["state_name"] = membership["state_name"].astype(str)

    state_rows = membership.loc[membership["state_name"].isin(DOJ_LISTED_STATES)].copy()
    state_matches = (
        state_rows.groupby("msa_id", as_index=False)["state_name"]
        .agg(_join_labels)
        .rename(columns={"state_name": "matched_states"})
    )

    county_lookup = _build_county_designation_lookup()
    county_rows = membership.merge(county_lookup, on="county_fips", how="inner")
    county_matches = (
        county_rows.groupby("msa_id", as_index=False)["county_label"]
        .agg(_join_labels)
        .rename(columns={"county_label": "matched_counties"})
    )

    city_lookup = _build_city_designation_lookup()
    city_rows = membership.merge(city_lookup, on="county_fips", how="inner")
    city_matches = (
        city_rows.groupby("msa_id", as_index=False)["city_label"]
        .agg(_join_labels)
        .rename(columns={"city_label": "matched_cities"})
    )

    result = (
        definitions.merge(state_matches, on="msa_id", how="left")
        .merge(county_matches, on="msa_id", how="left")
        .merge(city_matches, on="msa_id", how="left")
    )
    for column in ("matched_states", "matched_counties", "matched_cities"):
        result[column] = result[column].fillna("")

    result["state_match"] = result["matched_states"] != ""
    result["county_match"] = result["matched_counties"] != ""
    result["city_match"] = result["matched_cities"] != ""
    result = result[result[["state_match", "county_match", "city_match"]].any(axis=1)].copy()

    def match_basis(row: pd.Series) -> str:
        basis = []
        if bool(row["state_match"]):
            basis.append("state")
        if bool(row["county_match"]):
            basis.append("county")
        if bool(row["city_match"]):
            basis.append("city")
        return "+".join(basis)

    result["match_basis"] = result.apply(match_basis, axis=1)
    return result.loc[:, SANCTUARY_MSA_MATCH_COLUMNS].sort_values("cbsa_code").reset_index(
        drop=True
    )
