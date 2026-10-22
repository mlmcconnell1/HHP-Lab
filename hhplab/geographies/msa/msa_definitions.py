"""Census MSA definition builders for supported delineation vintages."""

from __future__ import annotations

import csv
import io
import subprocess
from dataclasses import dataclass

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

HISTORICAL_DEFINITION_VERSION = "census_msa_2010"
HISTORICAL_SOURCE_RELEASE_DATE = "2009-12-01"
HISTORICAL_GEOMETRY_VINTAGE = 2010
HISTORICAL_WORKBOOK_FILENAME = "list3_2009-12.xls"
HISTORICAL_SOURCE_NAME = "Census CBSA/CSA County Components (December 2009)"
HISTORICAL_SOURCE_SLUG = "census_msa_delineation_2009_12"
HISTORICAL_SOURCE_REF = (
    "https://www2.census.gov/programs-surveys/metro-micro/geographies/"
    "reference-files/2009/historical-delineation-files/list3.xls"
)
HISTORICAL_STANDARDS_BASIS = (
    "OMB 2000 standards applied to Census 2000 and 2007/2008 population estimates"
)
HISTORICAL_OMB_REF = (
    "https://www.whitehouse.gov/wp-content/uploads/legacy_drupal_files/omb/"
    "bulletins/2010/b10-02.pdf"
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

HISTORICAL_CSV_COLUMNS = (
    "CBSA Code",
    "Metro Division Code",
    "CSA Code",
    "CBSA Title",
    "Level of CBSA",
    "Status, 1=metro 2=micro",
    "Metropolitan Division Title",
    "CSA Title",
    "Component Name",
    "State",
    "FIPS",
    "County Status",
)
HISTORICAL_EXPECTED_MSA_COUNT = 374
HISTORICAL_EXPECTED_MEMBERSHIP_COUNT = 1168


@dataclass(frozen=True)
class MSASourceSpec:
    definition_version: str
    delineation_file_year: int
    workbook_filename: str
    source_name: str
    source_slug: str
    source_ref: str
    source_url: str
    source_release_date: str
    geometry_vintage: int
    geometry_source: str
    geometry_url: str


SOURCE_SPECS = {
    DEFINITION_VERSION: MSASourceSpec(
        definition_version=DEFINITION_VERSION,
        delineation_file_year=2023,
        workbook_filename=WORKBOOK_FILENAME,
        source_name=SOURCE_NAME,
        source_slug=SOURCE_SLUG,
        source_ref=SOURCE_REF,
        source_url=(
            "https://www2.census.gov/programs-surveys/metro-micro/"
            "geographies/reference-files/2023/delineation-files/list1_2023.xlsx"
        ),
        source_release_date="2023-07",
        geometry_vintage=2023,
        geometry_source="census_tiger_cbsa",
        geometry_url="https://www2.census.gov/geo/tiger/TIGER2023/CBSA/tl_2023_us_cbsa.zip",
    ),
    HISTORICAL_DEFINITION_VERSION: MSASourceSpec(
        definition_version=HISTORICAL_DEFINITION_VERSION,
        delineation_file_year=2009,
        workbook_filename=HISTORICAL_WORKBOOK_FILENAME,
        source_name=HISTORICAL_SOURCE_NAME,
        source_slug=HISTORICAL_SOURCE_SLUG,
        source_ref=HISTORICAL_SOURCE_REF,
        source_url=HISTORICAL_SOURCE_REF,
        source_release_date=HISTORICAL_SOURCE_RELEASE_DATE,
        geometry_vintage=HISTORICAL_GEOMETRY_VINTAGE,
        geometry_source="census_tiger_county10_dissolved_by_msa",
        geometry_url=(
            "https://www2.census.gov/geo/tiger/TIGER2010/COUNTY/2010/"
            "tl_2010_us_county10.zip"
        ),
    ),
}


def get_msa_source_spec(definition_version: str = DEFINITION_VERSION) -> MSASourceSpec:
    """Return a supported source specification with an actionable error."""
    try:
        return SOURCE_SPECS[definition_version]
    except KeyError:
        versions = ", ".join(sorted(SOURCE_SPECS))
        raise ValueError(
            f"Unsupported MSA definition version '{definition_version}'. "
            f"Choose one of: {versions}."
        ) from None


def parse_delineation_workbook(raw_content: bytes) -> pd.DataFrame:
    """Parse the Census July 2023 workbook into standardized rows."""
    df = pd.read_excel(io.BytesIO(raw_content), header=2)
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
    for column in ("cbsa_title", "area_type", "county_name", "state_name", "central_outlying"):
        result[column] = result[column].astype(str).str.strip()
    return result[list(STANDARDIZED_COLUMNS)]


def parse_historical_delineation_csv(csv_content: str) -> pd.DataFrame:
    """Parse the December 2009 List 3 rows emitted by the ``xls2csv`` tool."""
    rows = list(csv.reader(io.StringIO(csv_content)))
    header_index = next(
        (index for index, row in enumerate(rows) if tuple(row) == HISTORICAL_CSV_COLUMNS),
        None,
    )
    if header_index is None:
        raise ValueError(
            "December 2009 Census List 3 workbook has an unexpected header; "
            "expected its 12-column CBSA/component layout."
        )

    standardized_rows: list[dict[str, str]] = []
    for row in rows[header_index + 1 :]:
        if len(row) != len(HISTORICAL_CSV_COLUMNS) or not row[0].strip():
            continue
        values = dict(zip(HISTORICAL_CSV_COLUMNS, (value.strip() for value in row), strict=True))
        status = values["Status, 1=metro 2=micro"]
        level = values["Level of CBSA"]
        if status not in {"1", "2"}:
            raise ValueError(f"December 2009 List 3 contains unsupported status value {status!r}.")
        expected_level = (
            "Metropolitan Statistical Area"
            if status == "1"
            else "Micropolitan Statistical Area"
        )
        if level != expected_level:
            raise ValueError(
                f"December 2009 List 3 status {status} conflicts with Level of CBSA {level!r}."
            )
        fips = values["FIPS"]
        if (
            len(values["CBSA Code"]) != 5
            or not values["CBSA Code"].isdigit()
            or len(fips) != 5
            or not fips.isdigit()
        ):
            raise ValueError(
                "December 2009 List 3 contains a malformed CBSA code or county FIPS; "
                "expected five-digit text identifiers."
            )
        standardized_rows.append(
            {
                "cbsa_code": values["CBSA Code"],
                "cbsa_title": values["CBSA Title"],
                "area_type": expected_level,
                "county_name": values["Component Name"],
                "state_name": values["State"],
                "county_fips": fips,
                "central_outlying": values["County Status"],
            }
        )

    result = pd.DataFrame(standardized_rows, columns=STANDARDIZED_COLUMNS)
    if result.empty:
        raise ValueError("December 2009 List 3 contains no county-component rows.")
    return result


def parse_historical_delineation_file(source_path: str) -> pd.DataFrame:
    """Parse a frozen `.xls` source file with the system `xls2csv` utility."""
    try:
        completed = subprocess.run(
            ["xls2csv", source_path], check=True, capture_output=True, text=True, encoding="utf-8"
        )
    except FileNotFoundError:
        raise ValueError(
            "Parsing the historical `.xls` source requires `xls2csv`; install it and rerun "
            "`hhplab generate msa --definition-version census_msa_2010`."
        ) from None
    except subprocess.CalledProcessError as exc:
        raise ValueError(
            "Could not convert the December 2009 Census List 3 workbook with `xls2csv`: "
            f"{exc.stderr.strip()}"
        ) from None
    return parse_historical_delineation_csv(completed.stdout)


def parse_delineation_source(source_path: str, definition_version: str) -> pd.DataFrame:
    """Dispatch parsing by the explicitly selected source vintage."""
    get_msa_source_spec(definition_version)
    if definition_version == HISTORICAL_DEFINITION_VERSION:
        return parse_historical_delineation_file(source_path)
    with open(source_path, "rb") as source_file:
        return parse_delineation_workbook(source_file.read())


def _metro_rows(
    delineation_df: pd.DataFrame,
    definition_version: str = DEFINITION_VERSION,
) -> pd.DataFrame:
    missing = [col for col in STANDARDIZED_COLUMNS if col not in delineation_df.columns]
    if missing:
        raise ValueError(
            f"Delineation rows missing required columns {missing}. "
            f"Available columns: {list(delineation_df.columns)}"
        )
    metro = delineation_df[delineation_df["area_type"] == MSA_AREA_TYPE].copy()
    metro["msa_id"] = metro["cbsa_code"].astype(str).str.zfill(5)
    if definition_version == HISTORICAL_DEFINITION_VERSION:
        msa_count = metro["msa_id"].nunique()
        membership_count = metro[["msa_id", "county_fips"]].drop_duplicates().shape[0]
        counts_changed = (
            msa_count != HISTORICAL_EXPECTED_MSA_COUNT
            or membership_count != HISTORICAL_EXPECTED_MEMBERSHIP_COUNT
        )
        if counts_changed:
            raise ValueError(
                "December 2009 MSA source counts changed: expected "
                f"{HISTORICAL_EXPECTED_MSA_COUNT} MSAs and "
                f"{HISTORICAL_EXPECTED_MEMBERSHIP_COUNT} county memberships, got "
                f"{msa_count} and {membership_count}. Review the frozen source before ingest."
            )
    return metro


def build_definitions_df(
    delineation_df: pd.DataFrame,
    definition_version: str = DEFINITION_VERSION,
) -> pd.DataFrame:
    """Build the MSA definitions table from standardized delineation rows."""
    spec = get_msa_source_spec(definition_version)
    metro = _metro_rows(delineation_df, definition_version)
    definitions = (
        metro[["msa_id", "cbsa_code", "cbsa_title", "area_type"]]
        .drop_duplicates()
        .rename(columns={"cbsa_title": "msa_name"})
        .sort_values("msa_id")
        .reset_index(drop=True)
    )
    definitions["definition_version"] = spec.definition_version
    definitions["source"] = spec.source_slug
    definitions["source_ref"] = spec.source_ref
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


def build_county_membership_df(
    delineation_df: pd.DataFrame,
    definition_version: str = DEFINITION_VERSION,
) -> pd.DataFrame:
    """Build the MSA-to-county membership table from delineation rows."""
    spec = get_msa_source_spec(definition_version)
    metro = _metro_rows(delineation_df, definition_version)
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
    membership["definition_version"] = spec.definition_version
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
