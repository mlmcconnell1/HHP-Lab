"""Tests for HUD HIC ingestion and canonicalization."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from typer.testing import CliRunner

from hhplab.cli.main import app
from hhplab.hic.hud_user import (
    HICManualDownloadRequired,
    discover_local_hic_file,
    download_hic_data,
    get_hic_source_url,
)
from hhplab.hic.parser import (
    HICParseError,
    normalize_column_name,
    parse_hic_file,
    write_hic_parquet,
)
from hhplab.provenance import read_provenance

runner = CliRunner()

HIC_ROWS = [
    {
        "CocState": "co",
        "CoC": "Denver",
        "Coc\\ID": "CO-500",
        "year": "2020",
        "Project Type": "ES",
        "Total Beds": "1,200",
        "Total Units": "450",
    },
    {
        "CocState": "CO",
        "CoC": "Denver",
        "Coc\\ID": "CO-500",
        "year": "2020",
        "Project Type": "TH",
        "Total Beds": "300",
        "Total Units": "50",
    },
    {
        "CocState": "MO",
        "CoC": "Kansas City",
        "Coc\\ID": "MO-604a",
        "year": "2020",
        "Project Type": "ES",
        "Total Beds": "10",
        "Total Units": "4",
    },
]


def _write_hic_csv(path: Path, rows: list[dict] | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(HIC_ROWS if rows is None else rows).to_csv(path, index=False)
    return path


def test_normalize_column_name_handles_hud_escape_and_punctuation() -> None:
    assert normalize_column_name("Coc\\ID") == "coc_id"
    assert normalize_column_name("Beds HH w/ Children") == "beds_hh_w_children"


def test_parse_hic_file_aggregates_coc_year_rows(tmp_path: Path) -> None:
    raw_path = _write_hic_csv(tmp_path / "2020-HIC-Counts-by-State.csv")

    result = parse_hic_file(raw_path, year=2020, source_ref="https://example.com/hic.csv")

    assert result.rows_read == len(HIC_ROWS)
    assert result.rows_skipped == 0
    assert list(result.df["coc_id"]) == ["CO-500", "MO-604"]
    denver = result.df[result.df["coc_id"] == "CO-500"].iloc[0]
    assert denver["hic_year"] == 2020
    assert denver["state"] == "CO"
    assert denver["total_beds"] == 1500
    assert denver["total_units"] == 500


def test_parse_hic_file_sums_component_bed_and_unit_columns(tmp_path: Path) -> None:
    rows = [
        {
            "CocState": "NY",
            "CoC": "New York",
            "CoC ID": "NY-600",
            "year": "2021",
            "Beds HH w/ Children": "10",
            "Beds HH w/o Children": "20",
            "HMIS Beds": "999",
            "Units HH w/ Children": "3",
            "Units HH w/o Children": "4",
        }
    ]
    raw_path = _write_hic_csv(tmp_path / "2021-HIC-Counts-by-State.csv", rows)

    result = parse_hic_file(raw_path, year=2021)

    row = result.df.iloc[0]
    assert row["total_beds"] == 30
    assert row["total_units"] == 7


def test_parse_hic_file_prefers_unaliased_total_over_components(tmp_path: Path) -> None:
    rows = [
        {
            "CocState": "NY",
            "CoC": "New York",
            "CoC ID": "NY-600",
            "year": "2021",
            "All Program Total Bed Count": "30",
            "Emergency Shelter Beds": "10",
            "Transitional Housing Beds": "20",
            "All Program Total Unit Count": "7",
            "Family Units": "3",
            "Adult Units": "4",
        }
    ]
    raw_path = _write_hic_csv(tmp_path / "2021-HIC-Counts-by-State.csv", rows)

    result = parse_hic_file(raw_path, year=2021)

    row = result.df.iloc[0]
    assert row["total_beds"] == 30
    assert row["total_units"] == 7


def test_parse_hic_file_supports_xlsx(tmp_path: Path) -> None:
    raw_path = tmp_path / "2020-HIC-Counts-by-State.xlsx"
    pd.DataFrame(HIC_ROWS).to_excel(raw_path, index=False)

    result = parse_hic_file(raw_path, year=2020)

    assert len(result.df) == 2


def test_parse_hic_file_extracts_year_from_wide_hud_workbook(tmp_path: Path) -> None:
    raw_path = tmp_path / "2007-2024-HIC-Counts-by-CoC.xlsx"
    pd.DataFrame(
        [
            {
                "CocState": "CO",
                "CoC": "Denver",
                "Coc\\ID": "CO-500",
                "Total Beds 2020": "99",
                "Total Units 2020": "44",
                "Total Beds 2024": "1,200",
                "Total Units 2024": "450",
            },
            {
                "CocState": "MO",
                "CoC": "Kansas City",
                "Coc\\ID": "MO-604a",
                "Total Beds 2020": "10",
                "Total Units 2020": "4",
                "Total Beds 2024": "20",
                "Total Units 2024": "8",
            },
        ]
    ).to_excel(raw_path, index=False)

    result = parse_hic_file(raw_path, year=2024)

    assert result.rows_read == 2
    assert list(result.df["hic_year"].unique()) == [2024]
    denver = result.df[result.df["coc_id"] == "CO-500"].iloc[0]
    assert denver["total_beds"] == 1200
    assert denver["total_units"] == 450


def test_parse_hic_file_handles_coc_number_and_title_row_excel(tmp_path: Path) -> None:
    raw_path = tmp_path / "2007-2020-HIC-Counts-by-CoC.xlsx"
    rows = [
        ["Total Beds (ES, TH, SH)", "Emergency Shelter (ES)", None],
        [
            "CoC Number",
            "Total Year-Round Beds (ES, TH, SH)",
            "Total Units for Households with Children (ES, TH, SH)",
        ],
        ["AK-500", "920", "74"],
        ["MO-604a", "10", "4"],
    ]
    pd.DataFrame(rows).to_excel(raw_path, index=False, header=False, sheet_name="2020")

    result = parse_hic_file(raw_path, year=2020)

    assert list(result.df["coc_id"]) == ["AK-500", "MO-604"]
    alaska = result.df[result.df["coc_id"] == "AK-500"].iloc[0]
    assert alaska["state"] == "AK"
    assert alaska["total_beds"] == 920
    assert alaska["total_units"] == 74


@pytest.mark.parametrize(
    ("year", "bed_header", "unit_header", "expected_beds", "expected_units"),
    [
        (
            2010,
            "Total Year-Round Beds (ES,TH,SH)",
            "Total Units for Households with Children (ES,TH)",
            920,
            74,
        ),
        (
            2013,
            "Total Year-Round Beds (ES,TH,RRH,SH)",
            "Total Units for Households with Children (ES,TH,RRH)",
            1015,
            90,
        ),
    ],
)
def test_parse_hic_file_handles_old_aggregate_workbook_sheets(
    tmp_path: Path,
    year: int,
    bed_header: str,
    unit_header: str,
    expected_beds: int,
    expected_units: int,
) -> None:
    raw_path = tmp_path / "2007-2013-HIC-Counts-by-CoC.xlsx"
    rows = [
        ["Total Beds (ES,TH,SH)", "Emergency Shelter (ES)", None, None, None],
        [
            "CoC",
            bed_header,
            "Total Emergency Shelter (ES) Beds (excluding seasonal+overflow)",
            "Total Transitional Housing (TH) Beds",
            unit_header,
        ],
        ["AK-500", str(expected_beds), "574", "441", str(expected_units)],
        ["MO-604a", "10", "7", "3", "4"],
    ]
    pd.DataFrame(rows).to_excel(raw_path, index=False, header=False, sheet_name=str(year))

    result = parse_hic_file(raw_path, year=year)

    assert list(result.df["coc_id"]) == ["AK-500", "MO-604"]
    alaska = result.df[result.df["coc_id"] == "AK-500"].iloc[0]
    assert alaska["state"] == "AK"
    assert alaska["total_beds"] == expected_beds
    assert alaska["total_units"] == expected_units


def test_parse_hic_file_rejects_wide_workbook_without_requested_year(tmp_path: Path) -> None:
    raw_path = tmp_path / "2007-2024-HIC-Counts-by-CoC.xlsx"
    pd.DataFrame(
        [
            {
                "CocState": "CO",
                "CoC": "Denver",
                "Coc\\ID": "CO-500",
                "Total Beds 2024": "1,200",
                "Total Units 2024": "450",
            }
        ]
    ).to_excel(raw_path, index=False)

    with pytest.raises(HICParseError, match="total_beds"):
        parse_hic_file(raw_path, year=2025)


def test_parse_hic_file_rejects_empty_file(tmp_path: Path) -> None:
    raw_path = _write_hic_csv(tmp_path / "empty.csv", [])

    with pytest.raises(HICParseError, match="empty"):
        parse_hic_file(raw_path, year=2020)


def test_parse_hic_file_reports_missing_counts(tmp_path: Path) -> None:
    raw_path = _write_hic_csv(
        tmp_path / "bad.csv",
        [{"CocState": "CO", "CoC": "Denver", "Coc\\ID": "CO-500", "year": "2020"}],
    )

    with pytest.raises(HICParseError, match="total_beds"):
        parse_hic_file(raw_path, year=2020)


def test_write_hic_parquet_embeds_provenance(tmp_path: Path) -> None:
    raw_path = _write_hic_csv(tmp_path / "2020-HIC-Counts-by-State.csv")
    result = parse_hic_file(raw_path, year=2020, source_ref="https://example.com/hic.csv")
    output_path = tmp_path / "curated" / "hic__H2020.parquet"

    write_hic_parquet(
        result.df,
        output_path,
        raw_path=raw_path,
        raw_sha256=result.raw_sha256,
        source_ref="https://example.com/hic.csv",
        rows_read=result.rows_read,
        rows_skipped=result.rows_skipped,
    )

    roundtrip = pd.read_parquet(output_path)
    assert list(roundtrip.columns) == list(result.df.columns)
    provenance = read_provenance(output_path)
    assert provenance is not None
    assert provenance.extra["dataset"] == "hic"
    assert provenance.extra["raw_sha256"] == result.raw_sha256
    assert provenance.extra["rows_read"] == len(HIC_ROWS)


def test_discover_local_hic_file_finds_manual_download(tmp_path: Path) -> None:
    raw_path = _write_hic_csv(tmp_path / "2020-HIC-Counts-by-State.csv")

    assert discover_local_hic_file(2020, tmp_path) == raw_path


def test_discover_local_hic_file_finds_aggregate_hic_workbook(tmp_path: Path) -> None:
    raw_path = tmp_path / "2007-2024-HIC-Counts-by-CoC.xlsx"
    raw_path.write_bytes(b"placeholder")

    assert discover_local_hic_file(2020, tmp_path) == raw_path


def test_discover_local_hic_file_accepts_year_named_file(tmp_path: Path) -> None:
    raw_path = tmp_path / "2025"
    raw_path.write_bytes(b"placeholder")

    assert discover_local_hic_file(2025, raw_path) == raw_path


def test_download_hic_detects_hud_waf_challenge(tmp_path: Path, httpx_mock) -> None:
    url = get_hic_source_url(2020)
    httpx_mock.add_response(
        url=url,
        status_code=202,
        headers={"x-amzn-waf-action": "challenge", "content-type": "text/html"},
        text="JavaScript is disabled. verify that you're not a robot.",
    )

    with pytest.raises(HICManualDownloadRequired, match="--parse-only"):
        download_hic_data(2020, output_dir=tmp_path)


def test_download_hic_reports_missing_file_with_manual_path(tmp_path: Path, httpx_mock) -> None:
    url = get_hic_source_url(2020)
    httpx_mock.add_response(url=url, status_code=404)

    with pytest.raises(FileNotFoundError, match=str(tmp_path)):
        download_hic_data(2020, output_dir=tmp_path)


def test_download_hic_writes_raw_file(tmp_path: Path, httpx_mock, monkeypatch) -> None:
    url = get_hic_source_url(2020)
    content = b"CocState,CoC,Coc\\ID,year,Total Beds,Total Units\nCO,Denver,CO-500,2020,1,1\n"
    httpx_mock.add_response(url=url, content=content)
    monkeypatch.setattr("hhplab.hic.hud_user.check_source_changed", lambda **_kw: (False, {}))
    monkeypatch.setattr("hhplab.hic.hud_user.register_source", lambda **_kw: None)

    result = download_hic_data(2020, output_dir=tmp_path)

    assert result.path == tmp_path / "2020-HIC-Counts-by-State.csv"
    assert result.path.read_bytes() == content
    assert result.file_size == len(content)


def test_ingest_hic_cli_parse_only_writes_canonical_output(tmp_path: Path, monkeypatch) -> None:
    raw_path = tmp_path / "data" / "raw" / "hic" / "2020" / "2020-HIC-Counts-by-State.csv"
    _write_hic_csv(raw_path)
    monkeypatch.setenv("HHPLAB_ASSET_STORE_ROOT", str(tmp_path / "data"))

    result = runner.invoke(app, ["ingest", "hic", "--year", "2020", "--parse-only", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    output_path = tmp_path / "data" / "curated" / "hic" / "hic__H2020.parquet"
    assert payload["status"] == "ok"
    assert Path(payload["output_path"]) == output_path
    assert output_path.exists()


def test_ingest_hic_cli_json_reports_hash_mismatch_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_path = tmp_path / "data" / "raw" / "hic" / "2020" / "2020-HIC-Counts-by-State.csv"
    _write_hic_csv(raw_path)
    monkeypatch.setenv("HHPLAB_ASSET_STORE_ROOT", str(tmp_path / "data"))
    monkeypatch.setattr(
        "hhplab.cli.ingest_hic.download_hic_data",
        lambda *_args, **_kwargs: SimpleNamespace(
            path=raw_path,
            source_url=get_hic_source_url(2020),
            raw_sha256="not-the-parsed-file-hash",
        ),
    )

    result = runner.invoke(app, ["ingest", "hic", "--year", "2020", "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload == {
        "status": "error",
        "error": "Downloaded HIC hash changed before parsing completed.",
    }
    assert not (tmp_path / "data" / "curated" / "hic" / "hic__H2020.parquet").exists()


def test_ingest_hic_cli_parse_only_accepts_old_aggregate_workbook(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_path = (
        tmp_path / "data" / "raw" / "hic" / "2010" / "2007-2013-HIC-Counts-by-CoC.xlsx"
    )
    raw_path.parent.mkdir(parents=True)
    rows = [
        ["Total Beds (ES,TH,SH)", "Emergency Shelter (ES)", None],
        [
            "CoC",
            "Total Year-Round Beds (ES,TH,SH)",
            "Total Units for Households with Children (ES,TH)",
        ],
        ["AK-500", "920", "74"],
    ]
    pd.DataFrame(rows).to_excel(raw_path, index=False, header=False, sheet_name="2010")
    monkeypatch.setenv("HHPLAB_ASSET_STORE_ROOT", str(tmp_path / "data"))

    result = runner.invoke(app, ["ingest", "hic", "--year", "2010", "--parse-only", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    output_path = tmp_path / "data" / "curated" / "hic" / "hic__H2010.parquet"
    assert Path(payload["output_path"]) == output_path
    output = pd.read_parquet(output_path)
    assert output[["hic_year", "coc_id", "total_beds", "total_units"]].to_dict(
        orient="records"
    ) == [
        {
            "hic_year": 2010,
            "coc_id": "AK-500",
            "total_beds": 920,
            "total_units": 74,
        }
    ]
