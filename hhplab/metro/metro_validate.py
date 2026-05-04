"""Validation for metro definition artifacts.

Checks structural integrity, identifier formats, and cross-table
referential consistency of the three metro definition tables.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import pandas as pd

from hhplab.metro.metro_definitions import (
    CANONICAL_UNIVERSE_DEFINITION_VERSION,
    DEFINITION_VERSION,
    METRO_COUNT,
    PROFILE_NAME,
)


@dataclass
class MetroValidationResult:
    """Result of metro definition validation."""

    passed: bool
    errors: list[str]
    warnings: list[str]

    def summary(self) -> str:
        lines = [
            f"Metro validation: {'PASS' if self.passed else 'FAIL'} "
            f"({len(self.errors)} error(s), {len(self.warnings)} warning(s))"
        ]
        for e in self.errors:
            lines.append(f"  ERROR: {e}")
        for w in self.warnings:
            lines.append(f"  WARN:  {w}")
        return "\n".join(lines)


def validate_metro_artifacts(
    definitions_df: pd.DataFrame,
    coc_membership_df: pd.DataFrame,
    county_membership_df: pd.DataFrame,
) -> MetroValidationResult:
    """Validate metro definition DataFrames for structural and referential integrity.

    Checks:
    - Required columns present in each table.
    - metro_id format: ``GFnn`` (two-digit zero-padded).
    - county_fips format: 5-digit string.
    - coc_id format: ``XX-NNN`` (state abbreviation + 3 digits).
    - Expected metro count matches.
    - All metro_ids in membership tables exist in definitions.
    - definition_version consistency across tables.
    - No duplicate rows.

    Parameters
    ----------
    definitions_df : pd.DataFrame
        Metro definitions table.
    coc_membership_df : pd.DataFrame
        Metro-to-CoC membership table.
    county_membership_df : pd.DataFrame
        Metro-to-county membership table.

    Returns
    -------
    MetroValidationResult
    """
    errors: list[str] = []
    warnings: list[str] = []

    # -- Required columns --------------------------------------------------
    def _check_cols(df: pd.DataFrame, name: str, required: list[str]) -> None:
        missing = [c for c in required if c not in df.columns]
        if missing:
            errors.append(f"{name}: missing columns {missing}")

    _check_cols(
        definitions_df,
        "definitions",
        ["metro_id", "metro_name", "membership_type", "definition_version"],
    )
    _check_cols(
        coc_membership_df,
        "coc_membership",
        ["metro_id", "coc_id", "definition_version"],
    )
    _check_cols(
        county_membership_df,
        "county_membership",
        ["metro_id", "county_fips", "definition_version"],
    )

    # -- metro_id format: GFnn ---------------------------------------------
    gf_pattern = re.compile(r"^GF\d{2}$")
    for name, df, col in [
        ("definitions", definitions_df, "metro_id"),
        ("coc_membership", coc_membership_df, "metro_id"),
        ("county_membership", county_membership_df, "metro_id"),
    ]:
        if col not in df.columns:
            continue
        bad = [v for v in df[col].unique() if not gf_pattern.match(str(v))]
        if bad:
            errors.append(f"{name}: invalid metro_id format (expected GFnn): {bad[:5]}")

    # -- coc_id format: XX-NNN --------------------------------------------
    coc_pattern = re.compile(r"^[A-Z]{2}-\d{3}$")
    if "coc_id" in coc_membership_df.columns:
        bad_cocs = [
            v for v in coc_membership_df["coc_id"].unique() if not coc_pattern.match(str(v))
        ]
        if bad_cocs:
            errors.append(
                f"coc_membership: invalid coc_id format (expected XX-NNN): {bad_cocs[:5]}"
            )

    # -- county_fips format: 5 digits --------------------------------------
    fips_pattern = re.compile(r"^\d{5}$")
    if "county_fips" in county_membership_df.columns:
        bad_fips = [
            v
            for v in county_membership_df["county_fips"].unique()
            if not fips_pattern.match(str(v))
        ]
        if bad_fips:
            errors.append(
                f"county_membership: invalid county_fips format (expected 5 digits): {bad_fips[:5]}"
            )

    # -- Expected metro count ----------------------------------------------
    if "metro_id" in definitions_df.columns:
        actual_count = definitions_df["metro_id"].nunique()
        if actual_count != METRO_COUNT:
            errors.append(f"definitions: expected {METRO_COUNT} metros, found {actual_count}")

    # -- Referential integrity: all membership metro_ids in definitions -----
    if "metro_id" in definitions_df.columns:
        def_ids = set(definitions_df["metro_id"].unique())

        if "metro_id" in coc_membership_df.columns:
            coc_ids = set(coc_membership_df["metro_id"].unique())
            orphan_coc = coc_ids - def_ids
            if orphan_coc:
                errors.append(f"coc_membership: metro_ids not in definitions: {sorted(orphan_coc)}")
            missing_coc = def_ids - coc_ids
            if missing_coc:
                warnings.append(
                    f"definitions: metros with no CoC membership: {sorted(missing_coc)}"
                )

        if "metro_id" in county_membership_df.columns:
            county_ids = set(county_membership_df["metro_id"].unique())
            orphan_county = county_ids - def_ids
            if orphan_county:
                errors.append(
                    f"county_membership: metro_ids not in definitions: {sorted(orphan_county)}"
                )
            missing_county = def_ids - county_ids
            if missing_county:
                warnings.append(
                    f"definitions: metros with no county membership: {sorted(missing_county)}"
                )

    # -- definition_version consistency ------------------------------------
    for name, df in [
        ("definitions", definitions_df),
        ("coc_membership", coc_membership_df),
        ("county_membership", county_membership_df),
    ]:
        if "definition_version" in df.columns:
            versions = df["definition_version"].unique()
            if len(versions) != 1 or versions[0] != DEFINITION_VERSION:
                errors.append(
                    f"{name}: definition_version mismatch; "
                    f"expected '{DEFINITION_VERSION}', found {list(versions)}"
                )

    # -- No duplicate rows -------------------------------------------------
    if "metro_id" in definitions_df.columns:
        dups = definitions_df["metro_id"].duplicated().sum()
        if dups:
            errors.append(f"definitions: {dups} duplicate metro_id(s)")

    if {"metro_id", "coc_id"} <= set(coc_membership_df.columns):
        dups = coc_membership_df.duplicated(subset=["metro_id", "coc_id"]).sum()
        if dups:
            errors.append(f"coc_membership: {dups} duplicate (metro_id, coc_id) pair(s)")

    if {"metro_id", "county_fips"} <= set(county_membership_df.columns):
        dups = county_membership_df.duplicated(subset=["metro_id", "county_fips"]).sum()
        if dups:
            errors.append(f"county_membership: {dups} duplicate (metro_id, county_fips) pair(s)")

    return MetroValidationResult(
        passed=len(errors) == 0,
        errors=errors,
        warnings=warnings,
    )


def validate_metro_universe_artifacts(
    universe_df: pd.DataFrame,
    subset_profile_df: pd.DataFrame,
) -> MetroValidationResult:
    """Validate canonical metro-universe and subset-profile artifacts."""
    errors: list[str] = []
    warnings: list[str] = []

    def _check_cols(df: pd.DataFrame, name: str, required: list[str]) -> None:
        missing = [c for c in required if c not in df.columns]
        if missing:
            errors.append(f"{name}: missing columns {missing}")

    _check_cols(
        universe_df,
        "metro_universe",
        [
            "metro_id",
            "cbsa_code",
            "metro_name",
            "area_type",
            "definition_version",
            "source_definition_version",
            "source",
            "source_ref",
        ],
    )
    _check_cols(
        subset_profile_df,
        "metro_subset_profile",
        [
            "profile",
            "profile_definition_version",
            "metro_definition_version",
            "metro_id",
            "cbsa_code",
            "metro_name",
            "profile_metro_id",
            "profile_metro_name",
            "profile_rank",
            "source",
            "source_ref",
        ],
    )

    code_pattern = re.compile(r"^\d{5}$")
    gf_pattern = re.compile(r"^GF\d{2}$")

    if {"metro_id", "cbsa_code"} <= set(universe_df.columns):
        bad_metro_ids = [
            value
            for value in universe_df["metro_id"].unique()
            if not code_pattern.match(str(value))
        ]
        if bad_metro_ids:
            errors.append(
                "metro_universe: invalid metro_id format "
                f"(expected 5-digit CBSA code): {bad_metro_ids[:5]}"
            )
        mismatched = universe_df[
            universe_df["metro_id"].astype(str) != universe_df["cbsa_code"].astype(str)
        ]
        if not mismatched.empty:
            errors.append(
                "metro_universe: metro_id must equal cbsa_code for the canonical "
                "metro-universe contract"
            )

    if "definition_version" in universe_df.columns:
        versions = universe_df["definition_version"].unique()
        if len(versions) != 1 or versions[0] != CANONICAL_UNIVERSE_DEFINITION_VERSION:
            errors.append(
                "metro_universe: definition_version mismatch; expected "
                f"'{CANONICAL_UNIVERSE_DEFINITION_VERSION}', found {list(versions)}"
            )

    if {"metro_id", "cbsa_code"} <= set(subset_profile_df.columns):
        bad_subset_ids = [
            value
            for value in subset_profile_df["metro_id"].unique()
            if not code_pattern.match(str(value))
        ]
        if bad_subset_ids:
            errors.append(
                "metro_subset_profile: invalid metro_id format "
                f"(expected 5-digit CBSA code): {bad_subset_ids[:5]}"
            )
        mismatched = subset_profile_df[
            subset_profile_df["metro_id"].astype(str) != subset_profile_df["cbsa_code"].astype(str)
        ]
        if not mismatched.empty:
            errors.append(
                "metro_subset_profile: metro_id must equal cbsa_code for "
                "canonical-universe references"
            )

    if "profile_metro_id" in subset_profile_df.columns:
        bad_profile_ids = [
            value
            for value in subset_profile_df["profile_metro_id"].unique()
            if not gf_pattern.match(str(value))
        ]
        if bad_profile_ids:
            errors.append(
                "metro_subset_profile: invalid profile_metro_id format "
                f"(expected GFnn): {bad_profile_ids[:5]}"
            )

    if "profile" in subset_profile_df.columns:
        profiles = subset_profile_df["profile"].unique()
        if len(profiles) != 1 or profiles[0] != PROFILE_NAME:
            errors.append(
                f"metro_subset_profile: expected profile '{PROFILE_NAME}', found {list(profiles)}"
            )

    if "profile_definition_version" in subset_profile_df.columns:
        versions = subset_profile_df["profile_definition_version"].unique()
        if len(versions) != 1 or versions[0] != DEFINITION_VERSION:
            errors.append(
                "metro_subset_profile: profile_definition_version mismatch; "
                f"expected '{DEFINITION_VERSION}', found {list(versions)}"
            )

    if "metro_definition_version" in subset_profile_df.columns:
        versions = subset_profile_df["metro_definition_version"].unique()
        if len(versions) != 1 or versions[0] != CANONICAL_UNIVERSE_DEFINITION_VERSION:
            errors.append(
                "metro_subset_profile: metro_definition_version mismatch; "
                f"expected '{CANONICAL_UNIVERSE_DEFINITION_VERSION}', "
                f"found {list(versions)}"
            )

    if "metro_id" in universe_df.columns and "metro_id" in subset_profile_df.columns:
        universe_ids = set(universe_df["metro_id"].astype(str).unique())
        subset_ids = set(subset_profile_df["metro_id"].astype(str).unique())
        missing = sorted(subset_ids - universe_ids)
        if missing:
            errors.append(
                f"metro_subset_profile: metro_ids not in canonical metro universe: {missing[:5]}"
            )

    if "profile_rank" in subset_profile_df.columns:
        ranks = sorted(int(value) for value in subset_profile_df["profile_rank"].tolist())
        expected = list(range(1, METRO_COUNT + 1))
        if ranks != expected:
            errors.append(
                "metro_subset_profile: expected profile_rank values "
                f"{expected[0]}..{expected[-1]}, found {ranks[:5]}..."
            )

    if "metro_id" in universe_df.columns:
        dups = universe_df["metro_id"].duplicated().sum()
        if dups:
            errors.append(f"metro_universe: {dups} duplicate metro_id(s)")

    if {"profile_metro_id", "metro_id"} <= set(subset_profile_df.columns):
        dups = subset_profile_df.duplicated(subset=["profile_metro_id", "metro_id"]).sum()
        if dups:
            errors.append(
                f"metro_subset_profile: duplicate (profile_metro_id, metro_id) pair(s): {dups}"
            )

    return MetroValidationResult(
        passed=len(errors) == 0,
        errors=errors,
        warnings=warnings,
    )


def validate_metro_boundaries(
    boundaries_df: pd.DataFrame,
    definitions_df: pd.DataFrame,
    *,
    county_vintage: str | int,
) -> MetroValidationResult:
    """Validate materialized metro boundary polygons against definitions."""
    errors: list[str] = []
    warnings: list[str] = []

    def _check_cols(df: pd.DataFrame, name: str, required: list[str]) -> None:
        missing = [c for c in required if c not in df.columns]
        if missing:
            errors.append(f"{name}: missing columns {missing}")

    _check_cols(
        boundaries_df,
        "boundaries",
        [
            "metro_id",
            "metro_name",
            "definition_version",
            "geometry_vintage",
            "source",
            "source_ref",
            "ingested_at",
            "geometry",
        ],
    )

    if errors:
        return MetroValidationResult(
            passed=False,
            errors=errors,
            warnings=warnings,
        )

    gf_pattern = re.compile(r"^GF\d{2}$")
    bad_ids = [
        v for v in boundaries_df["metro_id"].dropna().unique() if not gf_pattern.match(str(v))
    ]
    if bad_ids:
        errors.append(f"boundaries: invalid metro_id format (expected GFnn): {bad_ids[:5]}")

    if not pd.api.types.is_datetime64_any_dtype(boundaries_df["ingested_at"]):
        errors.append("boundaries: ingested_at must be datetime-like")

    if boundaries_df["metro_id"].duplicated().any():
        errors.append(
            f"boundaries: {int(boundaries_df['metro_id'].duplicated().sum())} duplicate metro_id(s)"
        )

    if boundaries_df["geometry"].isna().any():
        errors.append("boundaries: null geometry values are not allowed")
    elif (
        hasattr(boundaries_df["geometry"], "is_empty") and boundaries_df["geometry"].is_empty.any()
    ):
        errors.append("boundaries: empty geometry values are not allowed")

    if "geometry_vintage" in boundaries_df.columns:
        vintages = list(pd.Series(boundaries_df["geometry_vintage"]).dropna().astype(str).unique())
        expected = str(county_vintage)
        if vintages != [expected]:
            errors.append(
                f"boundaries: geometry_vintage mismatch; expected '{expected}', found {vintages}"
            )

    versions = list(pd.Series(boundaries_df["definition_version"]).dropna().unique())
    if versions != [DEFINITION_VERSION]:
        errors.append(
            f"boundaries: definition_version mismatch; expected "
            f"'{DEFINITION_VERSION}', found {versions}"
        )

    def_ids = set(definitions_df["metro_id"].dropna().unique())
    boundary_ids = set(boundaries_df["metro_id"].dropna().unique())
    missing = sorted(def_ids - boundary_ids)
    extra = sorted(boundary_ids - def_ids)
    if missing:
        errors.append(f"boundaries: missing polygons for metro ids {missing[:10]}")
    if extra:
        errors.append(f"boundaries: found polygons without matching definitions {extra[:10]}")

    return MetroValidationResult(
        passed=len(errors) == 0,
        errors=errors,
        warnings=warnings,
    )
