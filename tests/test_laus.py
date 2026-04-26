"""Tests for BLS LAUS metro ingest and contract modules.

Truth table — expected BLS LAUS series IDs for key metros
----------------------------------------------------------
The BLS LAUS metro series ID format is 20 characters:
  LA + U + MT + state_fips(2) + cbsa(5) + 000000(6) + measure(2)

| metro_id | cbsa  | state_fips | unemployment_rate series    |
|----------|-------|------------|-----------------------------|
| GF01     | 35620 | 36 (NY)    | LAUMT363562000000003        |
| GF02     | 31080 | 06 (CA)    | LAUMT063108000000003        |
| GF07     | 47900 | 11 (DC)    | LAUMT114790000000003        |

Measure codes: 03=rate, 04=unemployed, 05=employed, 06=labor_force.
Codes 07/08 are NOT available for metro-area series (national/state only).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest

from coclab.bls import (
    LAUS_MEASURE_CODES,
    build_all_series_ids,
    build_laus_series_id,
)
from coclab.ingest.bls_laus import (
    BlsQuotaExhausted,
    _build_metro_series_map,
    _chunked,
    fetch_laus_annual_averages,
    ingest_laus_metro,
)
from coclab.metro.definitions import METRO_CBSA_MAPPING, METRO_STATE_FIPS
from coclab.naming import laus_metro_filename, laus_metro_path
from coclab.panel.conformance import (
    ACS1_MEASURE_COLUMNS,
    ACS_MEASURE_COLUMNS,
    LAUS_MEASURE_COLUMNS,
    PanelRequest,
    _effective_measure_columns,
)

# ---------------------------------------------------------------------------
# Series ID contract tests  (coclab-7isb.1)
# ---------------------------------------------------------------------------

#: Expected series IDs for New York (GF01, state_fips=36, cbsa=35620).
#: Used as the reference case to guard the BLS series ID format.
NY_SERIES_IDS = {
    "unemployment_rate": "LAUMT363562000000003",
    "unemployed":        "LAUMT363562000000004",
    "employed":          "LAUMT363562000000005",
    "labor_force":       "LAUMT363562000000006",
}

#: Expected series IDs for Los Angeles (GF02, state_fips=06, cbsa=31080).
LA_SERIES_IDS = {
    "unemployment_rate": "LAUMT063108000000003",
    "unemployed":        "LAUMT063108000000004",
    "employed":          "LAUMT063108000000005",
    "labor_force":       "LAUMT063108000000006",
}

#: Expected series ID for Washington DC (GF07, state_fips=11, cbsa=47900).
DC_UNEMPLOYMENT_RATE_SERIES = "LAUMT114790000000003"

LAUS_RECIPE_YEARS = tuple(range(2015, 2024))

LAUS_RECIPE_ACS_TRANSFORM_BY_YEAR = {
    2015: "tract_to_metro_2010",
    2019: "tract_to_metro_2010",
    2020: "tract_to_metro_2020",
    2023: "tract_to_metro_2020",
}

LAUS_RECIPE_PEP_PATH_BY_YEAR = {
    2015: "data/curated/pep/pep_county__v2020.parquet",
    2023: "data/curated/pep/pep_county__v2024.parquet",
}

LAUS_RECIPE_DATASET_PATH_BY_YEAR = {
    2015: "data/curated/laus/laus_metro__A2015@Dglynnfoxv1.parquet",
    2023: "data/curated/laus/laus_metro__A2023@Dglynnfoxv1.parquet",
}

#: Expected ACS5 tract input paths for representative years after applying the
#: ACS lag rule (acs_vintage = PIT_year - 1).  One entry from each tract era:
#: 2010-era tracts cover PIT years 2015-2019; 2020-era tracts cover 2020-2023.
#: With acs_end: -1 in the recipe, year 2015 → A2014xT2010, year 2023 → A2022xT2020.
LAUS_RECIPE_ACS_PATH_BY_YEAR = {
    2015: "data/curated/acs/acs5_tracts__A2014xT2010.parquet",  # 2010-era tracts
    2023: "data/curated/acs/acs5_tracts__A2022xT2020.parquet",  # 2020-era tracts
}


class TestLausSeriesIds:
    @pytest.mark.parametrize("measure,expected", list(NY_SERIES_IDS.items()))
    def test_new_york_series_ids(self, measure, expected):
        sid = build_laus_series_id("35620", measure, "36")
        assert sid == expected
        assert len(sid) == 20

    @pytest.mark.parametrize("measure,expected", list(LA_SERIES_IDS.items()))
    def test_los_angeles_series_ids(self, measure, expected):
        sid = build_laus_series_id("31080", measure, "06")
        assert sid == expected

    def test_washington_dc_unemployment_rate(self):
        sid = build_laus_series_id("47900", "unemployment_rate", "11")
        assert sid == DC_UNEMPLOYMENT_RATE_SERIES

    def test_all_series_are_20_chars(self):
        for metro_id, cbsa in METRO_CBSA_MAPPING.items():
            state_fips = METRO_STATE_FIPS[metro_id]
            for measure in LAUS_MEASURE_CODES:
                sid = build_laus_series_id(cbsa, measure, state_fips)
                assert len(sid) == 20, (
                    f"Series ID for {metro_id}/{measure} is not 20 chars: {sid}"
                )

    def test_all_series_start_with_laumt(self):
        for metro_id, cbsa in list(METRO_CBSA_MAPPING.items())[:5]:
            state_fips = METRO_STATE_FIPS[metro_id]
            for measure in LAUS_MEASURE_CODES:
                sid = build_laus_series_id(cbsa, measure, state_fips)
                assert sid.startswith("LAUMT"), f"Unexpected prefix in {sid}"

    def test_series_encodes_state_fips_before_cbsa(self):
        # NY: state_fips=36, cbsa=35620 → area segment = "3635620"
        sid = build_laus_series_id("35620", "unemployment_rate", "36")
        # Characters 5-11 (0-indexed) should be state_fips + cbsa
        assert sid[5:7] == "36", f"Expected state FIPS '36' at positions 5-6: {sid}"
        assert sid[7:12] == "35620", f"Expected CBSA '35620' at positions 7-11: {sid}"

    def test_unknown_measure_raises(self):
        with pytest.raises(ValueError, match="Unknown LAUS measure"):
            build_laus_series_id("35620", "bogus_measure", "36")

    def test_build_all_series_ids(self):
        ids = build_all_series_ids("35620", "36")
        assert set(ids) == set(LAUS_MEASURE_CODES)
        assert ids["unemployment_rate"] == NY_SERIES_IDS["unemployment_rate"]
        assert ids["labor_force"] == NY_SERIES_IDS["labor_force"]

    def test_measure_codes_match_bls_spec(self):
        assert LAUS_MEASURE_CODES["unemployment_rate"] == "03"
        assert LAUS_MEASURE_CODES["unemployed"] == "04"
        assert LAUS_MEASURE_CODES["employed"] == "05"
        assert LAUS_MEASURE_CODES["labor_force"] == "06"

    def test_all_25_metros_have_state_fips(self):
        assert set(METRO_STATE_FIPS) == set(METRO_CBSA_MAPPING), (
            "METRO_STATE_FIPS must cover all 25 metros in METRO_CBSA_MAPPING"
        )

    def test_state_fips_are_two_digit_strings(self):
        for metro_id, fips in METRO_STATE_FIPS.items():
            assert len(fips) == 2 and fips.isdigit(), (
                f"State FIPS for {metro_id} must be a 2-digit string, got {fips!r}"
            )


class TestMetroSeriesMap:
    def test_all_25_metros_present(self):
        mapping = _build_metro_series_map()
        assert len(mapping) == 25
        for gf_id in [f"GF{i:02d}" for i in range(1, 26)]:
            assert gf_id in mapping

    def test_each_metro_has_four_measures(self):
        mapping = _build_metro_series_map()
        for metro_id, series in mapping.items():
            assert set(series) == set(LAUS_MEASURE_CODES), (
                f"Metro {metro_id} missing measures"
            )

    def test_series_ids_are_unique(self):
        mapping = _build_metro_series_map()
        all_ids = [sid for series in mapping.values() for sid in series.values()]
        assert len(all_ids) == len(set(all_ids)), "Duplicate series IDs found"

    def test_total_series_count(self):
        mapping = _build_metro_series_map()
        total = sum(len(v) for v in mapping.values())
        assert total == 25 * 4  # 25 metros × 4 measures

    def test_new_york_series_ids_in_map(self):
        mapping = _build_metro_series_map()
        assert mapping["GF01"]["unemployment_rate"] == NY_SERIES_IDS["unemployment_rate"]


# ---------------------------------------------------------------------------
# Chunking helper
# ---------------------------------------------------------------------------


class TestChunked:
    def test_exact_multiple(self):
        chunks = list(_chunked([1, 2, 3, 4], 2))
        assert chunks == [[1, 2], [3, 4]]

    def test_remainder(self):
        chunks = list(_chunked([1, 2, 3], 2))
        assert chunks == [[1, 2], [3]]

    def test_single_chunk(self):
        chunks = list(_chunked([1, 2, 3], 10))
        assert chunks == [[1, 2, 3]]

    def test_empty(self):
        assert list(_chunked([], 5)) == []


# ---------------------------------------------------------------------------
# BLS API fetch tests (mocked)
# ---------------------------------------------------------------------------


def _make_bls_response(series_values: dict[str, float], year: int = 2023) -> dict:
    """Build a mock BLS API v2 response payload."""
    series_list = []
    for sid, value in series_values.items():
        series_list.append(
            {
                "seriesID": sid,
                "data": [
                    {"year": str(year), "period": "M13", "value": str(value)},
                    {"year": str(year), "period": "M01", "value": str(value * 0.9)},
                ],
            }
        )
    return {
        "status": "REQUEST_SUCCEEDED",
        "Results": {"series": series_list},
    }


class TestFetchLausAnnualAverages:
    def test_extracts_annual_average_period(self):
        # Use a correctly-formatted series ID (state_fips=36, cbsa=35620)
        sid = NY_SERIES_IDS["unemployment_rate"]
        mock_response = _make_bls_response({sid: 4.2})

        with patch("coclab.ingest.bls_laus.httpx.Client") as mock_client:
            post_rv = (
                mock_client.return_value.__enter__.return_value
                .post.return_value
            )
            post_rv.json.return_value = mock_response
            post_rv.raise_for_status.return_value = None

            result = fetch_laus_annual_averages([sid], 2023)

        assert sid in result
        assert result[sid] == pytest.approx(4.2)

    def test_ignores_monthly_values(self):
        sid = NY_SERIES_IDS["unemployment_rate"]
        monthly_only_response = {
            "status": "REQUEST_SUCCEEDED",
            "Results": {
                "series": [
                    {
                        "seriesID": sid,
                        "data": [
                            {"year": "2023", "period": "M01", "value": "3.9"},
                        ],
                    }
                ]
            },
        }
        with patch("coclab.ingest.bls_laus.httpx.Client") as mock_client:
            post_rv = (
                mock_client.return_value.__enter__.return_value
                .post.return_value
            )
            post_rv.json.return_value = monthly_only_response
            post_rv.raise_for_status.return_value = None

            result = fetch_laus_annual_averages([sid], 2023)

        assert sid not in result

    def test_raises_on_api_failure(self):
        failed_response = {
            "status": "REQUEST_FAILED",
            "message": ["Bad request"],
        }
        with patch("coclab.ingest.bls_laus.httpx.Client") as mock_client:
            post_rv = (
                mock_client.return_value.__enter__.return_value
                .post.return_value
            )
            post_rv.json.return_value = failed_response
            post_rv.raise_for_status.return_value = None

            with pytest.raises(ValueError, match="BLS API request failed"):
                fetch_laus_annual_averages([NY_SERIES_IDS["unemployment_rate"]], 2023)

    def test_anon_batches_large_series_lists_at_25(self):
        # Anonymous requests must batch at 25 (not 50) to respect the BLS API limit.
        # Use correctly-formatted 20-char IDs (LAUMT + 2-char fips + 5-char cbsa + 6 zeros + 2 code)
        sids = [f"LAUMT36{i:05d}00000003" for i in range(60)]
        empty_response = {"status": "REQUEST_SUCCEEDED", "Results": {"series": []}}

        with patch("coclab.ingest.bls_laus.httpx.Client") as mock_client:
            mock_post = mock_client.return_value.__enter__.return_value.post
            mock_post.return_value.json.return_value = empty_response
            mock_post.return_value.raise_for_status.return_value = None

            fetch_laus_annual_averages(sids, 2023)

        # 60 series split into 3 anonymous batches: 25 + 25 + 10
        assert mock_post.call_count == 3

    def test_registered_batches_at_50(self):
        # With an API key, the limit is 50 series per request.
        sids = [f"LAUMT36{i:05d}00000003" for i in range(60)]
        empty_response = {"status": "REQUEST_SUCCEEDED", "Results": {"series": []}}

        with patch("coclab.ingest.bls_laus.httpx.Client") as mock_client:
            mock_post = mock_client.return_value.__enter__.return_value.post
            mock_post.return_value.json.return_value = empty_response
            mock_post.return_value.raise_for_status.return_value = None

            fetch_laus_annual_averages(sids, 2023, api_key="test_registration_key")

        # 60 series split into 2 registered batches: 50 + 10
        assert mock_post.call_count == 2


# ---------------------------------------------------------------------------
# BLS quota-exhausted detection  (coclab-qh4v)
# ---------------------------------------------------------------------------


#: Documented BLS quota-exhausted response shapes.  When the daily query
#: threshold is hit BLS may respond with REQUEST_NOT_PROCESSED, REQUEST_FAILED,
#: or even an empty status; the message wording also varies.  Each entry below
#: is a (status, message[]) tuple known to indicate quota exhaustion in the
#: wild — fetch_laus_annual_averages must raise BlsQuotaExhausted for all of
#: them so the CLI can render an actionable hint.
BLS_QUOTA_RESPONSE_SHAPES = [
    pytest.param(
        "REQUEST_NOT_PROCESSED",
        ["daily threshold for total number of requests has been reached"],
        id="not-processed-with-threshold-message",
    ),
    pytest.param(
        "REQUEST_FAILED",
        ["Daily query limit reached for unregistered users"],
        id="failed-with-daily-query-limit",
    ),
    pytest.param(
        "REQUEST_NOT_PROCESSED",
        [],
        id="not-processed-empty-message",
    ),
    pytest.param(
        "REQUEST_FAILED",
        ["Throttle: too many queries"],
        id="failed-throttle",
    ),
]


class TestBlsQuotaExhausted:
    @pytest.mark.parametrize("status,message", BLS_QUOTA_RESPONSE_SHAPES)
    def test_fetch_raises_quota_exhausted(self, status, message):
        quota_response = {"status": status, "message": message}
        sid = NY_SERIES_IDS["unemployment_rate"]

        with patch("coclab.ingest.bls_laus.httpx.Client") as mock_client:
            post_rv = (
                mock_client.return_value.__enter__.return_value
                .post.return_value
            )
            post_rv.json.return_value = quota_response
            post_rv.raise_for_status.return_value = None

            with pytest.raises(BlsQuotaExhausted) as exc_info:
                fetch_laus_annual_averages([sid], 2023)

        # Anonymous request — message must point the user at registration.
        text = str(exc_info.value)
        assert "BLS API key" in text
        assert "BLS_API_KEY" in text or "--api-key" in text
        assert "wait" in text.lower()

    def test_fetch_quota_message_with_api_key_omits_registration_url(self):
        quota_response = {
            "status": "REQUEST_NOT_PROCESSED",
            "message": ["daily threshold for total number of requests has been reached"],
        }
        sid = NY_SERIES_IDS["unemployment_rate"]

        with patch("coclab.ingest.bls_laus.httpx.Client") as mock_client:
            post_rv = (
                mock_client.return_value.__enter__.return_value
                .post.return_value
            )
            post_rv.json.return_value = quota_response
            post_rv.raise_for_status.return_value = None

            with pytest.raises(BlsQuotaExhausted) as exc_info:
                fetch_laus_annual_averages([sid], 2023, api_key="present")

        text = str(exc_info.value)
        # When the user already has a key the actionable advice is "wait
        # for the threshold to reset" — registering again would not help.
        assert "wait" in text.lower()
        assert "reset" in text.lower()
        # Should not tell a user with a key to "register for" a (new) key
        assert "register for" not in text.lower()
        assert "BLS_API_KEY" not in text

    def test_non_quota_failure_still_raises_value_error(self):
        """Other REQUEST_FAILED responses must keep raising ValueError, not
        BlsQuotaExhausted, so callers can distinguish quota from real errors."""
        bad_request = {"status": "REQUEST_FAILED", "message": ["Invalid series id"]}
        sid = NY_SERIES_IDS["unemployment_rate"]

        with patch("coclab.ingest.bls_laus.httpx.Client") as mock_client:
            post_rv = (
                mock_client.return_value.__enter__.return_value
                .post.return_value
            )
            post_rv.json.return_value = bad_request
            post_rv.raise_for_status.return_value = None

            with pytest.raises(ValueError, match="BLS API request failed"):
                fetch_laus_annual_averages([sid], 2023)


# ---------------------------------------------------------------------------
# Full ingest integration tests (mocked BLS API)
# ---------------------------------------------------------------------------


def _make_full_bls_response(year: int = 2023) -> dict:
    """Build a mock BLS response covering all 25 metros × 4 measures.

    Uses the corrected series ID format including state FIPS prefix.
    """
    sample_values = {
        "unemployment_rate": 4.5,
        "unemployed": 50000,
        "employed": 1000000,
        "labor_force": 1050000,
    }

    series_list = []
    for metro_id, cbsa_code in METRO_CBSA_MAPPING.items():
        state_fips = METRO_STATE_FIPS[metro_id]
        for measure, value in sample_values.items():
            sid = build_laus_series_id(cbsa_code, measure, state_fips)
            series_list.append(
                {
                    "seriesID": sid,
                    "data": [
                        {"year": str(year), "period": "M13", "value": str(value)},
                    ],
                }
            )

    return {"status": "REQUEST_SUCCEEDED", "Results": {"series": series_list}}


def _mock_ingest(tmp_path: Path, year: int) -> Path:
    """Helper: run ingest with mocked BLS API returning valid data."""
    responses = [_make_full_bls_response(year), _make_full_bls_response(year)]
    call_count = [0]

    def mock_post(*args, **kwargs):
        resp = responses[min(call_count[0], len(responses) - 1)]
        call_count[0] += 1

        class FakeResp:
            def json(self):
                return resp

            def raise_for_status(self):
                pass

        return FakeResp()

    with patch("coclab.ingest.bls_laus.httpx.Client") as mock_client:
        mock_client.return_value.__enter__.return_value.post = mock_post
        return ingest_laus_metro(year=year, project_root=tmp_path)


class TestIngestLausMetro:
    def test_writes_parquet_with_25_metros(self, tmp_path):
        path = _mock_ingest(tmp_path, 2023)
        assert path.exists()
        df = pd.read_parquet(path)
        assert len(df) == 25
        assert set(df["metro_id"]) == {f"GF{i:02d}" for i in range(1, 26)}

    def test_output_has_canonical_columns(self, tmp_path):
        path = _mock_ingest(tmp_path, 2022)
        df = pd.read_parquet(path)
        expected_cols = [
            "metro_id", "year", "unemployment_rate",
            "unemployed", "employed", "labor_force",
        ]
        for col in expected_cols:
            assert col in df.columns, f"Missing column: {col}"

    def test_unemployment_rate_is_float(self, tmp_path):
        path = _mock_ingest(tmp_path, 2021)
        df = pd.read_parquet(path)
        assert str(df["unemployment_rate"].dtype) == "Float64"
        assert str(df["unemployed"].dtype) == "Int64"
        assert str(df["employed"].dtype) == "Int64"
        assert str(df["labor_force"].dtype) == "Int64"

    def test_output_sorted_by_metro_id(self, tmp_path):
        path = _mock_ingest(tmp_path, 2020)
        df = pd.read_parquet(path)
        assert list(df["metro_id"]) == sorted(df["metro_id"].tolist())

    def test_output_path_matches_naming_convention(self, tmp_path):
        year = 2023
        path = _mock_ingest(tmp_path, year)
        expected = laus_metro_path(year, "glynn_fox_v1", base_dir=tmp_path / "data")
        assert path == expected

    def test_data_source_column(self, tmp_path):
        path = _mock_ingest(tmp_path, 2023)
        df = pd.read_parquet(path)
        assert (df["data_source"] == "bls_laus").all()

    def test_no_null_measures_when_api_returns_data(self, tmp_path):
        path = _mock_ingest(tmp_path, 2023)
        df = pd.read_parquet(path)
        # Mock returns data for all metros; no measure should be null
        assert df["unemployment_rate"].notna().all(), (
            "unemployment_rate should be non-null when API returns data for all metros"
        )
        assert df["labor_force"].notna().all()

    def test_raises_when_all_measures_null(self, tmp_path):
        """Ingest must fail fast rather than write an all-null parquet."""
        empty_response = {"status": "REQUEST_SUCCEEDED", "Results": {"series": []}}

        with patch("coclab.ingest.bls_laus.httpx.Client") as mock_client:
            mock_post = mock_client.return_value.__enter__.return_value.post
            mock_post.return_value.json.return_value = empty_response
            mock_post.return_value.raise_for_status.return_value = None

            with pytest.raises(ValueError, match="metro\\(s\\) have no data for any measure"):
                ingest_laus_metro(year=2023, project_root=tmp_path)

    def test_raises_on_partial_metro_data(self, tmp_path):
        """Ingest must fail fast when the API returns data for only some metros.

        Truth table
        -----------
        Scenario: API returns M13 values for metros GF01–GF13 only (13/25).
        The remaining 12 metros have all-null measures.
        Expected: ValueError — partial output must not be written silently.
        """
        from coclab.ingest.bls_laus import _build_metro_series_map

        # Build values for only the first 13 metros (sorted order)
        metro_series = _build_metro_series_map()
        partial_metro_ids = sorted(metro_series)[:13]
        partial_values: dict[str, float] = {}
        for mid in partial_metro_ids:
            for _measure, sid in metro_series[mid].items():
                partial_values[sid] = 4.5

        def _partial_fetch(series_ids, year, api_key=None):
            return {sid: v for sid, v in partial_values.items() if sid in series_ids}

        with patch("coclab.ingest.bls_laus.fetch_laus_annual_averages", _partial_fetch):
            with pytest.raises(ValueError, match="metro\\(s\\) have no data for any measure"):
                ingest_laus_metro(year=2023, project_root=tmp_path)

    def test_raises_on_single_missing_measure(self, tmp_path):
        """Ingest must fail when one measure is missing across all metros (coclab-q3uz).

        Truth table
        -----------
        Scenario: All unemployment_rate series IDs return no data; the three count
        series (labor_force, employed, unemployed) return normal values for all 25
        metros.  The all-null-metro check passes (each metro has three valid
        measures), but the parquet would be silently written with 25 null rates.
        Expected: ValueError naming the missing measure.
        """
        from coclab.bls import LAUS_MEASURE_CODES
        from coclab.ingest.bls_laus import _build_metro_series_map

        # Collect series IDs for every measure except unemployment_rate
        metro_series = _build_metro_series_map()
        rate_measure = "unemployment_rate"
        LAUS_MEASURE_CODES[rate_measure]
        non_rate_values: dict[str, float] = {}
        for _mid, measure_map in metro_series.items():
            for measure, sid in measure_map.items():
                if measure != rate_measure:
                    non_rate_values[sid] = 50000.0

        def _no_rate_fetch(series_ids, year, api_key=None):
            return {sid: v for sid, v in non_rate_values.items() if sid in series_ids}

        with patch("coclab.ingest.bls_laus.fetch_laus_annual_averages", _no_rate_fetch):
            with pytest.raises(ValueError, match="unemployment_rate"):
                ingest_laus_metro(year=2023, project_root=tmp_path)

    def test_raises_on_partial_measure_for_one_metro(self, tmp_path):
        """Ingest must fail when one metro is missing just one measure (coclab-q3uz).

        Truth table
        -----------
        Scenario: All 25 metros have labor_force, employed, unemployed populated.
        GF01's unemployment_rate series returns no data; the other 24 metros have
        all four measures.  The all-null-metro check passes and the "entirely null"
        check passes (24 metros do have unemployment_rate), but GF01 would be
        written with a null rate.
        Expected: ValueError naming GF01 and unemployment_rate.
        """
        from coclab.ingest.bls_laus import _build_metro_series_map

        metro_series = _build_metro_series_map()
        skip_metro = sorted(metro_series)[0]  # GF01
        skip_measure = "unemployment_rate"

        all_values: dict[str, float] = {}
        for mid, measure_map in metro_series.items():
            for measure, sid in measure_map.items():
                if mid == skip_metro and measure == skip_measure:
                    continue  # omit this single series
                value = 4.5 if measure == "unemployment_rate" else 50000.0
                all_values[sid] = value

        def _one_missing_fetch(series_ids, year, api_key=None):
            return {sid: v for sid, v in all_values.items() if sid in series_ids}

        with patch("coclab.ingest.bls_laus.fetch_laus_annual_averages", _one_missing_fetch):
            with pytest.raises(ValueError, match="partial measure data"):
                ingest_laus_metro(year=2023, project_root=tmp_path)


# ---------------------------------------------------------------------------
# Naming tests
# ---------------------------------------------------------------------------


class TestLausNaming:
    def test_filename(self):
        assert laus_metro_filename(2023, "glynn_fox_v1") == "laus_metro__A2023@Dglynnfoxv1.parquet"

    def test_filename_string_year(self):
        assert (
            laus_metro_filename("2022", "glynn_fox_v1")
            == "laus_metro__A2022@Dglynnfoxv1.parquet"
        )

    def test_path_default_base(self):
        p = laus_metro_path(2023, "glynn_fox_v1")
        assert str(p) == "data/curated/laus/laus_metro__A2023@Dglynnfoxv1.parquet"

    def test_path_custom_base(self, tmp_path):
        p = laus_metro_path(2023, "glynn_fox_v1", base_dir=tmp_path)
        assert p.parent == tmp_path / "curated" / "laus"
        assert p.name == "laus_metro__A2023@Dglynnfoxv1.parquet"


# ---------------------------------------------------------------------------
# Conformance and ACS regression-guard tests  (coclab-7isb.6)
# ---------------------------------------------------------------------------


class TestLausConformanceColumns:
    def test_laus_measure_columns_defined(self):
        assert "unemployment_rate" in LAUS_MEASURE_COLUMNS
        assert "unemployed" in LAUS_MEASURE_COLUMNS
        assert "employed" in LAUS_MEASURE_COLUMNS
        assert "labor_force" in LAUS_MEASURE_COLUMNS

    def test_laus_columns_excluded_by_default(self):
        req = PanelRequest(start_year=2015, end_year=2020)
        cols = _effective_measure_columns(req)
        assert "unemployed" not in cols
        assert "employed" not in cols
        assert "labor_force" not in cols

    def test_laus_columns_included_when_requested(self):
        req = PanelRequest(start_year=2015, end_year=2020, include_laus=True)
        cols = _effective_measure_columns(req)
        assert "unemployment_rate" in cols
        assert "unemployed" in cols
        assert "employed" in cols
        assert "labor_force" in cols

    def test_acs_unemployment_rate_still_validated(self):
        """Regression guard: unemployment_rate must remain in ACS_MEASURE_COLUMNS.

        The LAUS work must not silently remove unemployment_rate from the default
        ACS conformance set; CoC panels carry this column from ACS1-derived data.
        """
        assert "unemployment_rate" in ACS_MEASURE_COLUMNS, (
            "unemployment_rate was removed from ACS_MEASURE_COLUMNS. "
            "This is a regression: CoC panels rely on this column in conformance checks."
        )

    def test_acs1_unemployment_rate_is_tracked(self):
        """ACS1 unemployment rate must be in ACS1_MEASURE_COLUMNS."""
        assert "unemployment_rate_acs1" in ACS1_MEASURE_COLUMNS

    def test_no_duplicate_columns_when_acs_and_laus_combined(self):
        """unemployment_rate appears in both ACS and LAUS; must not duplicate."""
        req = PanelRequest(
            start_year=2015,
            end_year=2020,
            acs_products=["acs5"],
            include_laus=True,
        )
        cols = _effective_measure_columns(req)
        assert len(cols) == len(set(cols)), (
            f"Duplicate columns in _effective_measure_columns: {cols}"
        )

    def test_laus_and_acs1_are_distinct_columns(self):
        req = PanelRequest(
            start_year=2015,
            end_year=2020,
            acs_products=["acs5", "acs1"],
            include_laus=True,
        )
        cols = _effective_measure_columns(req)
        # ACS1 rate and LAUS rate are distinct named columns
        assert "unemployment_rate_acs1" in cols
        assert "unemployment_rate" in cols
        # No duplicates
        assert len(cols) == len(set(cols))

    def test_laus_columns_present_when_measure_columns_set_explicitly(self):
        """measure_columns=<explicit list> must not silently drop LAUS columns (coclab-xt72).

        When executor.py translates measure_columns through column aliases it sets
        request.measure_columns to an explicit list, causing _effective_measure_columns
        to return early before adding LAUS columns.  This test guards against that
        regression by verifying that the explicitly-set list contains all LAUS columns
        (i.e. the caller is responsible for including them before setting the field).
        """
        # Simulate what executor builds after alias translation with include_laus=True:
        # ACS_MEASURE_COLUMNS + unique LAUS extras, all translated through aliases.
        aliases = {
            "total_population": "total_population_acs5",
            "unemployment_rate": "unemployment_rate",  # identity — not renamed
        }
        base_cols = list(ACS_MEASURE_COLUMNS) + [
            c for c in LAUS_MEASURE_COLUMNS if c not in ACS_MEASURE_COLUMNS
        ]
        translated = [aliases.get(c, c) for c in base_cols]

        req = PanelRequest(
            start_year=2020,
            end_year=2023,
            measure_columns=translated,
            include_laus=True,
        )
        cols = _effective_measure_columns(req)
        # All LAUS columns must appear (possibly aliased)
        laus_translated = {aliases.get(c, c) for c in LAUS_MEASURE_COLUMNS}
        assert laus_translated.issubset(set(cols)), (
            f"LAUS columns {laus_translated - set(cols)} missing from conformance columns"
        )


# ---------------------------------------------------------------------------
# Panel integration tests  (coclab-7isb.6)
# ---------------------------------------------------------------------------


class TestLausPanelIntegration:
    """Tests for LAUS loading into metro panels via _load_laus_metro_measures."""

    def _write_laus_artifact(self, tmp_path: Path, year: int) -> Path:
        """Write a minimal valid LAUS artifact for testing panel integration."""
        from coclab.metro.definitions import METRO_CBSA_MAPPING, METRO_STATE_FIPS
        from coclab.naming import laus_metro_path

        rows = []
        for metro_id, cbsa in METRO_CBSA_MAPPING.items():
            METRO_STATE_FIPS[metro_id]
            rows.append({
                "metro_id": metro_id,
                "year": year,
                "cbsa_code": cbsa,
                "labor_force": 1_000_000,
                "employed": 950_000,
                "unemployed": 50_000,
                "unemployment_rate": 5.0,
                "data_source": "bls_laus",
            })

        df = pd.DataFrame(rows)
        df["labor_force"] = df["labor_force"].astype("Int64")
        df["employed"] = df["employed"].astype("Int64")
        df["unemployed"] = df["unemployed"].astype("Int64")
        df["unemployment_rate"] = df["unemployment_rate"].astype("Float64")

        laus_dir = tmp_path / "curated" / "laus"
        laus_dir.mkdir(parents=True, exist_ok=True)
        out_path = laus_metro_path(year, "glynn_fox_v1", base_dir=tmp_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out_path, index=False)
        return out_path

    def test_load_laus_metro_measures_returns_dataframe(self, tmp_path):
        from coclab.panel.assemble import _load_laus_metro_measures

        year = 2023
        self._write_laus_artifact(tmp_path, year)
        laus_dir = tmp_path / "curated" / "laus"

        df = _load_laus_metro_measures(
            year=year,
            definition_version="glynn_fox_v1",
            laus_dir=laus_dir,
        )

        assert df is not None
        assert len(df) == 25
        assert "metro_id" in df.columns
        assert "unemployment_rate" in df.columns
        assert "labor_force" in df.columns

    def test_load_laus_metro_measures_returns_none_when_missing(self, tmp_path):
        from coclab.panel.assemble import _load_laus_metro_measures

        df = _load_laus_metro_measures(
            year=2023,
            definition_version="glynn_fox_v1",
            laus_dir=tmp_path / "laus",
        )
        assert df is None

    def test_laus_measures_appear_in_metro_panel_columns(self):
        from coclab.panel.assemble import METRO_PANEL_COLUMNS
        for col in ["labor_force", "employed", "unemployed", "unemployment_rate"]:
            assert col in METRO_PANEL_COLUMNS, (
                f"LAUS column '{col}' missing from METRO_PANEL_COLUMNS"
            )


# ---------------------------------------------------------------------------
# CLI tests  (coclab-7isb.5)
# ---------------------------------------------------------------------------


def _make_mock_ingest_fn(tmp_path: Path, year_override: int | None = None) -> Any:
    """Return a mock for coclab.ingest.bls_laus.ingest_laus_metro that writes a
    minimal parquet and returns the path."""
    import pandas as pd

    from coclab.naming import laus_metro_path

    def _mock_ingest(year: int, definition_version: str = "glynn_fox_v1",
                     api_key: Any = None, project_root: Path | None = None) -> Path:
        effective_year = year_override if year_override is not None else year
        rows = [{"metro_id": f"GF{i:02d}", "year": effective_year,
                 "labor_force": 1_000_000, "employed": 950_000,
                 "unemployed": 50_000, "unemployment_rate": 5.0,
                 "data_source": "bls_laus",
                 "metro_name": f"Metro {i}"}
                for i in range(1, 26)]
        df = pd.DataFrame(rows)
        df["labor_force"] = df["labor_force"].astype("Int64")
        df["employed"] = df["employed"].astype("Int64")
        df["unemployed"] = df["unemployed"].astype("Int64")
        df["unemployment_rate"] = df["unemployment_rate"].astype("Float64")

        base = project_root / "data" if project_root else tmp_path / "data"
        out = laus_metro_path(effective_year, definition_version, base_dir=base)
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False)
        return out

    return _mock_ingest


class TestCliLausMetro:
    """CLI tests for `coclab ingest laus-metro`."""

    def _runner(self):
        from typer.testing import CliRunner
        return CliRunner()

    def _app(self):
        from coclab.cli.main import app
        return app

    def test_single_year_exits_zero(self, tmp_path):
        mock_fn = _make_mock_ingest_fn(tmp_path)
        with patch("coclab.ingest.bls_laus.ingest_laus_metro", mock_fn), \
             patch("coclab.cli.main._check_working_directory"):
            result = self._runner().invoke(self._app(), ["ingest", "laus-metro", "--year", "2023"])
        assert result.exit_code == 0, result.output

    def test_single_year_output_mentions_metros(self, tmp_path):
        mock_fn = _make_mock_ingest_fn(tmp_path)
        with patch("coclab.ingest.bls_laus.ingest_laus_metro", mock_fn), \
             patch("coclab.cli.main._check_working_directory"):
            result = self._runner().invoke(self._app(), ["ingest", "laus-metro", "--year", "2023"])
        assert result.exit_code == 0, result.output
        assert "25" in result.output or "Metro" in result.output

    def test_json_output_flag(self, tmp_path):
        mock_fn = _make_mock_ingest_fn(tmp_path)
        with patch("coclab.ingest.bls_laus.ingest_laus_metro", mock_fn), \
             patch("coclab.cli.main._check_working_directory"):
            result = self._runner().invoke(self._app(),
                                           ["ingest", "laus-metro", "--year", "2023", "--json"])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["status"] == "ok"
        assert data["year"] == 2023
        assert data["metros"] == 25
        assert "output_path" in data
        assert "unemployment_rate_mean" in data

    def test_year_range_backfill(self, tmp_path):
        mock_fn = _make_mock_ingest_fn(tmp_path)
        with patch("coclab.ingest.bls_laus.ingest_laus_metro", mock_fn), \
             patch("coclab.cli.main._check_working_directory"):
            result = self._runner().invoke(
                self._app(),
                ["ingest", "laus-metro", "--start-year", "2021", "--end-year", "2023"]
            )
        assert result.exit_code == 0, result.output

    def test_year_range_json_output(self, tmp_path):
        mock_fn = _make_mock_ingest_fn(tmp_path)
        with patch("coclab.ingest.bls_laus.ingest_laus_metro", mock_fn), \
             patch("coclab.cli.main._check_working_directory"):
            result = self._runner().invoke(
                self._app(),
                ["ingest", "laus-metro", "--start-year", "2021", "--end-year", "2022", "--json"]
            )
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["status"] == "ok"
        assert set(data["years_succeeded"]) == {2021, 2022}

    def test_no_year_arg_exits_nonzero(self, tmp_path):
        with patch("coclab.cli.main._check_working_directory"):
            result = self._runner().invoke(self._app(), ["ingest", "laus-metro"])
        assert result.exit_code != 0

    def test_year_and_start_year_conflict_exits_nonzero(self, tmp_path):
        with patch("coclab.cli.main._check_working_directory"):
            result = self._runner().invoke(
                self._app(),
                ["ingest", "laus-metro", "--year", "2023", "--start-year", "2021"]
            )
        assert result.exit_code != 0

    def test_reversed_range_exits_nonzero(self, tmp_path):
        with patch("coclab.cli.main._check_working_directory"):
            result = self._runner().invoke(
                self._app(),
                ["ingest", "laus-metro", "--start-year", "2023", "--end-year", "2021"]
            )
        assert result.exit_code != 0

    def test_ingest_error_exits_nonzero(self, tmp_path):
        def _failing_ingest(**kwargs):
            raise ValueError("BLS API down")

        with patch("coclab.ingest.bls_laus.ingest_laus_metro", _failing_ingest), \
             patch("coclab.cli.main._check_working_directory"):
            result = self._runner().invoke(
                self._app(), ["ingest", "laus-metro", "--year", "2023"]
            )
        assert result.exit_code != 0

    def test_partial_backfill_json_exits_nonzero(self, tmp_path):
        """--json mode must exit 1 when any year in a backfill fails.

        Truth table
        -----------
        years_requested: [2021, 2022]
        2021: succeeds
        2022: raises ValueError
        Expected: exit_code == 1, JSON status == "partial"
        """
        mock_fn = _make_mock_ingest_fn(tmp_path)

        def _partial_ingest(year, **kwargs):
            if year == 2022:
                raise ValueError("No data for 2022")
            return mock_fn(year=year, **kwargs)

        with patch("coclab.ingest.bls_laus.ingest_laus_metro", _partial_ingest), \
             patch("coclab.cli.main._check_working_directory"):
            result = self._runner().invoke(
                self._app(),
                ["ingest", "laus-metro", "--start-year", "2021", "--end-year", "2022", "--json"],
            )

        assert result.exit_code == 1, f"Expected exit 1, got {result.exit_code}: {result.output}"
        data = json.loads(result.output)
        assert data["status"] == "partial"
        assert 2021 in data["years_succeeded"]
        assert 2022 in data["years_failed"]

    def test_all_failed_backfill_json_exits_nonzero(self, tmp_path):
        """--json mode must exit 1 and report status 'error' when all years fail."""
        def _failing_ingest(**kwargs):
            raise ValueError("BLS API unavailable")

        with patch("coclab.ingest.bls_laus.ingest_laus_metro", _failing_ingest), \
             patch("coclab.cli.main._check_working_directory"):
            result = self._runner().invoke(
                self._app(),
                ["ingest", "laus-metro", "--start-year", "2021", "--end-year", "2022", "--json"],
            )

        assert result.exit_code == 1, f"Expected exit 1, got {result.exit_code}: {result.output}"
        data = json.loads(result.output)
        assert data["status"] == "error"
        assert data["years_succeeded"] == []
        assert set(data["years_failed"]) == {2021, 2022}

    # ------------------------------------------------------------------
    # Quota-exhausted CLI behaviour  (coclab-qh4v)
    # ------------------------------------------------------------------

    def test_quota_exhausted_single_year_text_includes_actionable_hint(self, tmp_path):
        """Single-year ingest must surface the BlsQuotaExhausted message verbatim
        so the user immediately sees how to recover (API key or wait)."""
        from coclab.ingest.bls_laus import BlsQuotaExhausted

        actionable = (
            "The anonymous BLS API daily threshold has been reached. "
            "Either register for a free BLS API key at https://example/register "
            "and re-run with --api-key <KEY> (or set BLS_API_KEY in the environment), "
            "or wait for the threshold to reset (midnight US Eastern time) and retry."
        )

        def _quota_ingest(**kwargs):
            raise BlsQuotaExhausted(actionable)

        with patch("coclab.ingest.bls_laus.ingest_laus_metro", _quota_ingest), \
             patch("coclab.cli.main._check_working_directory"):
            result = self._runner().invoke(
                self._app(), ["ingest", "laus-metro", "--year", "2023"]
            )

        assert result.exit_code == 1, result.output
        combined = (result.output or "") + (result.stderr if hasattr(result, "stderr") else "")
        # Recovery hints must appear (API key OR wait), not just a generic failure
        assert "BLS quota exhausted" in combined
        assert "BLS_API_KEY" in combined or "--api-key" in combined
        assert "wait" in combined.lower()

    def test_quota_exhausted_single_year_json_carries_reason(self, tmp_path):
        from coclab.ingest.bls_laus import BlsQuotaExhausted

        def _quota_ingest(**kwargs):
            raise BlsQuotaExhausted(
                "register for a free BLS API key and re-run with --api-key <KEY>, "
                "or wait for the threshold to reset"
            )

        with patch("coclab.ingest.bls_laus.ingest_laus_metro", _quota_ingest), \
             patch("coclab.cli.main._check_working_directory"):
            result = self._runner().invoke(
                self._app(), ["ingest", "laus-metro", "--year", "2023", "--json"]
            )

        assert result.exit_code == 1, result.output
        data = json.loads(result.output)
        assert data["status"] == "error"
        assert data["reason"] == "bls_quota_exhausted"
        assert "--api-key" in data["error"] or "BLS_API_KEY" in data["error"]
        assert "wait" in data["error"].lower()

    def test_quota_exhausted_backfill_short_circuits_remaining_years(self, tmp_path):
        """When the first year of a backfill hits a quota error, remaining years
        must NOT be retried (they would all fail with the same condition).  All
        years should be reported as failed and the JSON payload must carry the
        bls_quota_exhausted reason."""
        from coclab.ingest.bls_laus import BlsQuotaExhausted

        call_log: list[int] = []

        def _quota_ingest(year, **kwargs):
            call_log.append(year)
            raise BlsQuotaExhausted(
                "register for a BLS API key and re-run with --api-key <KEY>, "
                "or wait for the threshold to reset"
            )

        with patch("coclab.ingest.bls_laus.ingest_laus_metro", _quota_ingest), \
             patch("coclab.cli.main._check_working_directory"):
            result = self._runner().invoke(
                self._app(),
                ["ingest", "laus-metro", "--start-year", "2015", "--end-year", "2023", "--json"],
            )

        assert result.exit_code == 1, result.output
        # Only the first year should have been attempted
        assert call_log == [2015], f"Backfill kept calling after quota: {call_log}"

        data = json.loads(result.output)
        assert data["status"] == "error"
        assert data["reason"] == "bls_quota_exhausted"
        assert data["years_succeeded"] == []
        assert set(data["years_failed"]) == set(range(2015, 2024))

    def test_quota_exhausted_backfill_after_partial_success_marks_partial(self, tmp_path):
        """If some early years succeed and a later year hits the quota, the
        backfill must report status=partial, mark only the unattempted years
        as failed, and tag the payload with bls_quota_exhausted."""
        from coclab.ingest.bls_laus import BlsQuotaExhausted

        mock_fn = _make_mock_ingest_fn(tmp_path)
        call_log: list[int] = []

        def _mixed_ingest(year, **kwargs):
            call_log.append(year)
            if year >= 2017:
                raise BlsQuotaExhausted(
                    "register for a BLS API key and re-run with --api-key <KEY>, "
                    "or wait for the threshold to reset"
                )
            return mock_fn(year=year, **kwargs)

        with patch("coclab.ingest.bls_laus.ingest_laus_metro", _mixed_ingest), \
             patch("coclab.cli.main._check_working_directory"):
            result = self._runner().invoke(
                self._app(),
                ["ingest", "laus-metro", "--start-year", "2015", "--end-year", "2020", "--json"],
            )

        assert result.exit_code == 1, result.output
        # 2015 + 2016 succeed; 2017 raises and breaks the loop.
        assert call_log == [2015, 2016, 2017], f"Unexpected attempts: {call_log}"

        data = json.loads(result.output)
        assert data["status"] == "partial"
        assert data["reason"] == "bls_quota_exhausted"
        assert set(data["years_succeeded"]) == {2015, 2016}
        # 2017 (the year that hit the quota) plus all later years must be marked failed
        assert set(data["years_failed"]) == {2017, 2018, 2019, 2020}


# ---------------------------------------------------------------------------
# Recipe integration tests  (coclab-7isb)
# ---------------------------------------------------------------------------


class TestLausRecipeSchema:
    """Tests for LausPolicy round-trips through the recipe schema."""

    def test_laus_policy_default(self):
        from coclab.recipe.recipe_schema import LausPolicy
        p = LausPolicy()
        assert p.include is False

    def test_laus_policy_include_true(self):
        from coclab.recipe.recipe_schema import LausPolicy
        p = LausPolicy(include=True)
        assert p.include is True

    def test_laus_policy_forbids_extra_fields(self):
        from pydantic import ValidationError

        from coclab.recipe.recipe_schema import LausPolicy
        with pytest.raises(ValidationError):
            LausPolicy(include=True, bogus_field=42)

    def test_panel_policy_with_laus(self):
        from coclab.recipe.recipe_schema import LausPolicy, PanelPolicy
        policy = PanelPolicy(laus=LausPolicy(include=True))
        assert policy.laus is not None
        assert policy.laus.include is True

    def test_panel_policy_laus_none_by_default(self):
        from coclab.recipe.recipe_schema import PanelPolicy
        policy = PanelPolicy()
        assert policy.laus is None

    def test_laus_policy_round_trips_via_recipe_load(self):
        """LausPolicy is preserved after loading a recipe dict."""
        from coclab.recipe.loader import load_recipe
        recipe = load_recipe({
            "version": 1,
            "name": "test-laus",
            "universe": {"years": [2023]},
            "targets": [{
                "id": "metro_panel",
                "geometry": {"type": "metro", "source": "glynn_fox_v1"},
                "panel_policy": {
                    "laus": {"include": True},
                },
            }],
            "datasets": {
                "laus_metro": {
                    "provider": "bls",
                    "product": "laus",
                    "version": 1,
                    "native_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                    "path": "data/curated/laus/laus_metro__A2023@Dglynnfoxv1.parquet",
                },
            },
        })
        target = recipe.targets[0]
        assert target.panel_policy is not None
        assert target.panel_policy.laus is not None
        assert target.panel_policy.laus.include is True


class TestValidateBLSLaus:
    """Tests for the bls/laus dataset adapter validator."""

    def _make_spec(self, **overrides) -> Any:
        from coclab.recipe.recipe_schema import DatasetSpec, GeometryRef
        defaults: dict[str, Any] = {
            "provider": "bls",
            "product": "laus",
            "version": 1,
            "native_geometry": GeometryRef(type="metro", source="glynn_fox_v1"),
            "path": "data/curated/laus/laus_metro__A2023@Dglynnfoxv1.parquet",
        }
        defaults.update(overrides)
        return DatasetSpec(**defaults)

    def test_valid_spec_no_diagnostics(self):
        from coclab.recipe.default_dataset_adapters import _validate_bls_laus
        spec = self._make_spec()
        diags = _validate_bls_laus(spec)
        assert diags == []

    def test_wrong_version_is_error(self):
        from coclab.recipe.default_dataset_adapters import _validate_bls_laus
        spec = self._make_spec(version=2)
        diags = _validate_bls_laus(spec)
        assert any(d.level == "error" and "version" in d.message for d in diags)

    def test_wrong_geometry_type_is_error(self):
        from coclab.recipe.default_dataset_adapters import _validate_bls_laus
        from coclab.recipe.recipe_schema import GeometryRef
        # No path → validator cannot fall back to materialized artifact
        spec = self._make_spec(native_geometry=GeometryRef(type="county"), path=None)
        diags = _validate_bls_laus(spec)
        assert any(d.level == "error" and "metro" in d.message for d in diags)

    def test_no_source_warns(self):
        from coclab.recipe.default_dataset_adapters import _validate_bls_laus
        from coclab.recipe.recipe_schema import GeometryRef
        spec = self._make_spec(native_geometry=GeometryRef(type="metro"))
        diags = _validate_bls_laus(spec)
        assert any(d.level == "warning" and "source" in d.message for d in diags)

    def test_no_path_warns(self):
        from coclab.recipe.default_dataset_adapters import _validate_bls_laus
        spec = self._make_spec(path=None)
        diags = _validate_bls_laus(spec)
        assert any(d.level == "warning" and "path" in d.message.lower() for d in diags)

    def test_unknown_params_warns(self):
        from coclab.recipe.default_dataset_adapters import _validate_bls_laus
        spec = self._make_spec(params={"bogus": "value"})
        diags = _validate_bls_laus(spec)
        assert any(d.level == "warning" and "unrecognized" in d.message for d in diags)

    def test_bls_laus_registered_in_global_registry(self):
        """bls/laus must be registered in the default dataset registry."""
        from coclab.recipe.adapters import dataset_registry

        # Force a clean registry with defaults to verify registration
        local_registry = type(dataset_registry)()
        from coclab.recipe.default_dataset_adapters import register_dataset_defaults
        register_dataset_defaults(local_registry)

        # Build a minimal spec
        from coclab.recipe.recipe_schema import DatasetSpec, GeometryRef
        spec = DatasetSpec(
            provider="bls", product="laus", version=1,
            native_geometry=GeometryRef(type="metro", source="glynn_fox_v1"),
            path="data/curated/laus/laus_metro__A2023@Dglynnfoxv1.parquet",
        )
        diags = local_registry.validate(spec)
        # Valid spec should produce no errors
        assert not any(d.level == "error" for d in diags)

    def test_laus_recipe_yaml_loads_cleanly(self):
        """The example metro25-glynnfox-laus.yaml must load without errors."""
        import yaml

        from coclab.recipe.loader import load_recipe

        recipe_path = Path(__file__).parent.parent / "recipes" / "metro25-glynnfox-laus.yaml"
        assert recipe_path.exists(), f"Example LAUS recipe not found: {recipe_path}"

        with open(recipe_path) as f:
            recipe_dict = yaml.safe_load(f)

        recipe = load_recipe(recipe_dict)
        assert recipe.name == "glynn_fox_metro_panel_2015_2023_laus"

        # Verify bls/laus dataset present
        assert "laus_metro" in recipe.datasets
        laus_ds = recipe.datasets["laus_metro"]
        assert laus_ds.provider == "bls"
        assert laus_ds.product == "laus"

        # Verify panel_policy.laus.include is True
        target = recipe.targets[0]
        assert target.panel_policy is not None
        assert target.panel_policy.laus is not None
        assert target.panel_policy.laus.include is True
        assert target.panel_policy.column_aliases["population"] == "pep_population"

    def test_laus_recipe_plan_resolves_2015_2023_multiyear_inputs(self):
        """The committed LAUS recipe should resolve the full 2015-2023 window."""
        import yaml

        from coclab.recipe.loader import load_recipe
        from coclab.recipe.planner import resolve_plan

        recipe_path = Path(__file__).parent.parent / "recipes" / "metro25-glynnfox-laus.yaml"
        with open(recipe_path) as f:
            recipe_dict = yaml.safe_load(f)

        recipe = load_recipe(recipe_dict)
        plan = resolve_plan(recipe, "build_metro_panel")

        assert [task.year for task in plan.join_tasks] == list(LAUS_RECIPE_YEARS)
        assert tuple(plan.join_tasks[0].datasets) == (
            "pit",
            "pep_county",
            "acs_tract",
            "laus_metro",
        )

        acs_tasks = {
            task.year: task for task in plan.resample_tasks if task.dataset_id == "acs_tract"
        }
        pep_tasks = {
            task.year: task for task in plan.resample_tasks if task.dataset_id == "pep_county"
        }
        laus_tasks = {
            task.year: task for task in plan.resample_tasks if task.dataset_id == "laus_metro"
        }

        for year, transform_id in LAUS_RECIPE_ACS_TRANSFORM_BY_YEAR.items():
            assert acs_tasks[year].transform_id == transform_id

        for year, input_path in LAUS_RECIPE_PEP_PATH_BY_YEAR.items():
            assert pep_tasks[year].input_path == input_path

        for year, input_path in LAUS_RECIPE_DATASET_PATH_BY_YEAR.items():
            assert laus_tasks[year].input_path == input_path

    def test_laus_recipe_plan_resolves_acs_lag_offsets(self):
        """ACS5 inputs must use PIT-year-1 vintage (ACS lag rule, coclab-ua3i).

        Regression: the recipe formerly used acs_end: 0, which resolved A2015 for
        panel year 2015 instead of the correct A2014.  With acs_end: -1 the planner
        must produce lag-offset paths for representative years in each tract era.
        """
        import yaml

        from coclab.recipe.loader import load_recipe
        from coclab.recipe.planner import resolve_plan

        recipe_path = Path(__file__).parent.parent / "recipes" / "metro25-glynnfox-laus.yaml"
        with open(recipe_path) as f:
            recipe_dict = yaml.safe_load(f)

        recipe = load_recipe(recipe_dict)
        plan = resolve_plan(recipe, "build_metro_panel")

        acs_tasks = {
            task.year: task
            for task in plan.resample_tasks
            if task.dataset_id == "acs_tract"
        }

        for year, expected_path in LAUS_RECIPE_ACS_PATH_BY_YEAR.items():
            assert acs_tasks[year].input_path == expected_path, (
                f"ACS path for year {year}: expected {expected_path!r}, "
                f"got {acs_tasks[year].input_path!r} — check acs_end year_offset in recipe"
            )

    def test_laus_recipe_validates_no_adapter_errors(self):
        """The LAUS example recipe should pass adapter validation with no errors."""
        import yaml

        from coclab.recipe.adapters import (
            DatasetAdapterRegistry,
            GeometryAdapterRegistry,
            validate_recipe_adapters,
        )
        from coclab.recipe.default_dataset_adapters import register_dataset_defaults
        from coclab.recipe.default_geometry_adapters import register_geometry_defaults
        from coclab.recipe.loader import load_recipe

        recipe_path = Path(__file__).parent.parent / "recipes" / "metro25-glynnfox-laus.yaml"
        with open(recipe_path) as f:
            recipe_dict = yaml.safe_load(f)

        recipe = load_recipe(recipe_dict)

        geo_reg = GeometryAdapterRegistry()
        register_geometry_defaults(geo_reg)

        ds_reg = DatasetAdapterRegistry()
        register_dataset_defaults(ds_reg)

        diags = validate_recipe_adapters(recipe, geo_reg, ds_reg)
        errors = [d for d in diags if d.level == "error"]
        assert errors == [], f"Unexpected adapter errors: {[e.message for e in errors]}"
