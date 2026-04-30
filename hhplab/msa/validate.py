"""Validation for curated MSA definition artifacts."""

from __future__ import annotations

import re
from dataclasses import dataclass

import pandas as pd

from hhplab.msa.definitions import DEFINITION_VERSION, MSA_AREA_TYPE


@dataclass
class MSAValidationResult:
    """Result of MSA definition validation."""

    passed: bool
    errors: list[str]
    warnings: list[str]

    def summary(self) -> str:
        lines = [
            f"MSA validation: {'PASS' if self.passed else 'FAIL'} "
            f"({len(self.errors)} error(s), {len(self.warnings)} warning(s))"
        ]
        for error in self.errors:
            lines.append(f"  ERROR: {error}")
        for warning in self.warnings:
            lines.append(f"  WARN:  {warning}")
        return "\n".join(lines)


def validate_msa_artifacts(
    definitions_df: pd.DataFrame,
    county_membership_df: pd.DataFrame,
) -> MSAValidationResult:
    """Validate the curated MSA definitions and county membership tables."""
    errors: list[str] = []
    warnings: list[str] = []

    def _check_cols(df: pd.DataFrame, name: str, required: list[str]) -> None:
        missing = [col for col in required if col not in df.columns]
        if missing:
            errors.append(f"{name}: missing columns {missing}")

    _check_cols(
        definitions_df,
        "definitions",
        [
            "msa_id",
            "cbsa_code",
            "msa_name",
            "area_type",
            "definition_version",
            "source",
            "source_ref",
        ],
    )
    _check_cols(
        county_membership_df,
        "county_membership",
        [
            "msa_id",
            "cbsa_code",
            "county_fips",
            "county_name",
            "state_name",
            "central_outlying",
            "definition_version",
        ],
    )

    code_pattern = re.compile(r"^\d{5}$")
    county_pattern = re.compile(r"^\d{5}$")
    for name, df, col in [
        ("definitions", definitions_df, "msa_id"),
        ("definitions", definitions_df, "cbsa_code"),
        ("county_membership", county_membership_df, "msa_id"),
        ("county_membership", county_membership_df, "cbsa_code"),
    ]:
        if col not in df.columns:
            continue
        bad = [value for value in df[col].unique() if not code_pattern.match(str(value))]
        if bad:
            errors.append(f"{name}: invalid {col} format (expected 5 digits): {bad[:5]}")

    if "county_fips" in county_membership_df.columns:
        bad_counties = [
            value
            for value in county_membership_df["county_fips"].unique()
            if not county_pattern.match(str(value))
        ]
        if bad_counties:
            errors.append(
                "county_membership: invalid county_fips format "
                f"(expected 5 digits): {bad_counties[:5]}"
            )

    if "area_type" in definitions_df.columns:
        bad_types = sorted(set(definitions_df["area_type"]) - {MSA_AREA_TYPE})
        if bad_types:
            errors.append(
                f"definitions: unexpected area_type values {bad_types}; "
                f"expected only '{MSA_AREA_TYPE}'"
            )

    if {"msa_id", "cbsa_code"} <= set(definitions_df.columns):
        mismatched = definitions_df[definitions_df["msa_id"] != definitions_df["cbsa_code"]]
        if not mismatched.empty:
            errors.append(
                "definitions: msa_id must match cbsa_code for the current "
                "identifier contract"
            )

    if {"msa_id", "cbsa_code"} <= set(county_membership_df.columns):
        mismatched = county_membership_df[
            county_membership_df["msa_id"] != county_membership_df["cbsa_code"]
        ]
        if not mismatched.empty:
            errors.append(
                "county_membership: msa_id must match cbsa_code for the current "
                "identifier contract"
            )

    if "msa_id" in definitions_df.columns:
        def_ids = set(definitions_df["msa_id"].unique())
        member_ids = (
            set(county_membership_df["msa_id"].unique())
            if "msa_id" in county_membership_df.columns
            else set()
        )
        orphan_membership = member_ids - def_ids
        if orphan_membership:
            errors.append(
                f"county_membership: msa_ids not in definitions: {sorted(orphan_membership)}"
            )
        missing_membership = def_ids - member_ids
        if missing_membership:
            warnings.append(
                f"definitions: MSAs with no county membership: {sorted(missing_membership)}"
            )

    for name, df in [
        ("definitions", definitions_df),
        ("county_membership", county_membership_df),
    ]:
        if "definition_version" not in df.columns:
            continue
        versions = list(pd.Series(df["definition_version"]).dropna().unique())
        if versions != [DEFINITION_VERSION]:
            errors.append(
                f"{name}: definition_version mismatch; expected "
                f"'{DEFINITION_VERSION}', found {versions}"
            )

    if "msa_id" in definitions_df.columns:
        duplicate_defs = definitions_df["msa_id"].duplicated().sum()
        if duplicate_defs:
            errors.append(f"definitions: {duplicate_defs} duplicate msa_id(s)")

    if {"msa_id", "county_fips"} <= set(county_membership_df.columns):
        duplicate_pairs = county_membership_df.duplicated(
            subset=["msa_id", "county_fips"]
        ).sum()
        if duplicate_pairs:
            errors.append(
                "county_membership: "
                f"{duplicate_pairs} duplicate (msa_id, county_fips) pair(s)"
            )

    return MSAValidationResult(
        passed=not errors,
        errors=errors,
        warnings=warnings,
    )

