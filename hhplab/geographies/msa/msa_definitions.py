"""Census MSA definition builders from the July 2023 delineation workbook."""

from __future__ import annotations

from io import BytesIO

import pandas as pd

DEFINITION_VERSION: str = "census_msa_2023"
DELINEATION_FILE_YEAR: int = 2023
WORKBOOK_FILENAME: str = "list1_2023.xlsx"
MSA_AREA_TYPE: str = "Metropolitan Statistical Area"
SOURCE_NAME: str = "Census CBSA/MSA Delineation File (July 2023)"
SOURCE_SLUG: str = "census_msa_delineation_2023"
SOURCE_REF: str = (
    "https://www.census.gov/geographies/reference-files/time-series/demo/"
    "metro-micro/delineation-files.html"
)

STANDARDIZED_COLUMNS: tuple[str, ...] = (
    "cbsa_code",
    "cbsa_title",
    "area_type",
    "county_name",
    "state_name",
    "county_fips",
    "central_outlying",
)

WORKBOOK_COLUMNS: dict[str, str] = {
    "CBSA Code": "cbsa_code",
    "CBSA Title": "cbsa_title",
    "Metropolitan/Micropolitan Statistical Area": "area_type",
    "County/County Equivalent": "county_name",
    "State Name": "state_name",
    "FIPS State Code": "fips_state_code",
    "FIPS County Code": "fips_county_code",
    "Central/Outlying County": "central_outlying",
}


def parse_delineation_workbook(raw_content: bytes) -> pd.DataFrame:
    """Parse the Census delineation workbook into a standardized row table."""
    df = pd.read_excel(BytesIO(raw_content), header=2)
    missing = [col for col in WORKBOOK_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            f"Delineation workbook is missing expected columns {missing}. "
            f"Available columns: {list(df.columns)}"
        )

    result = df.rename(columns=WORKBOOK_COLUMNS)[list(WORKBOOK_COLUMNS.values())].copy()
    for column in ("cbsa_code", "fips_state_code", "fips_county_code"):
        result[column] = pd.to_numeric(result[column], errors="coerce").astype("Int64")

    result = result.dropna(subset=["cbsa_code", "fips_state_code", "fips_county_code"]).copy()
    result["cbsa_code"] = result["cbsa_code"].astype(str).str.zfill(5)
    result["fips_state_code"] = result["fips_state_code"].astype(str).str.zfill(2)
    result["fips_county_code"] = result["fips_county_code"].astype(str).str.zfill(3)
    result["county_fips"] = result["fips_state_code"] + result["fips_county_code"]
    result["cbsa_title"] = result["cbsa_title"].astype(str).str.strip()
    result["area_type"] = result["area_type"].astype(str).str.strip()
    result["county_name"] = result["county_name"].astype(str).str.strip()
    result["state_name"] = result["state_name"].astype(str).str.strip()
    result["central_outlying"] = result["central_outlying"].astype(str).str.strip()
    return result[list(STANDARDIZED_COLUMNS)]


def _metro_rows(delineation_df: pd.DataFrame) -> pd.DataFrame:
    missing = [col for col in STANDARDIZED_COLUMNS if col not in delineation_df.columns]
    if missing:
        raise ValueError(
            f"Delineation rows missing required columns {missing}. "
            f"Available columns: {list(delineation_df.columns)}"
        )
    metro = delineation_df[delineation_df["area_type"] == MSA_AREA_TYPE].copy()
    metro["msa_id"] = metro["cbsa_code"]
    return metro


def build_definitions_df(delineation_df: pd.DataFrame) -> pd.DataFrame:
    """Build the MSA definitions table from standardized delineation rows."""
    metro = _metro_rows(delineation_df)
    definitions = (
        metro[["msa_id", "cbsa_code", "cbsa_title", "area_type"]]
        .drop_duplicates()
        .rename(columns={"cbsa_title": "msa_name"})
        .sort_values("msa_id")
        .reset_index(drop=True)
    )
    definitions["definition_version"] = DEFINITION_VERSION
    definitions["source"] = SOURCE_SLUG
    definitions["source_ref"] = SOURCE_REF
    return definitions[
        [
            "msa_id",
            "cbsa_code",
            "msa_name",
            "area_type",
            "definition_version",
            "source",
            "source_ref",
        ]
    ]


def build_county_membership_df(delineation_df: pd.DataFrame) -> pd.DataFrame:
    """Build the MSA-to-county membership table from delineation rows."""
    metro = _metro_rows(delineation_df)
    membership = (
        metro[
            [
                "msa_id",
                "cbsa_code",
                "county_fips",
                "county_name",
                "state_name",
                "central_outlying",
            ]
        ]
        .drop_duplicates()
        .sort_values(["msa_id", "county_fips"])
        .reset_index(drop=True)
    )
    membership["definition_version"] = DEFINITION_VERSION
    return membership[
        [
            "msa_id",
            "cbsa_code",
            "county_fips",
            "county_name",
            "state_name",
            "central_outlying",
            "definition_version",
        ]
    ]
