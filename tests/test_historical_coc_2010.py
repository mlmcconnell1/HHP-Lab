"""Focused contracts for HUD's archived 2010 per-state CoC layers."""

from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from hhplab.cli.main import app
from hhplab.geographies.boundaries.hud.exchange_gis import (
    _historical_coverage_summary,
    _is_canonical_historical_layer_id,
)

runner = CliRunner()


@pytest.mark.parametrize(
    ("coc_id", "expected"),
    [
        pytest.param("MA-510", True, id="single-coc-id"),
        pytest.param("DC-500", True, id="district-of-columbia"),
        pytest.param(
            "MA-510_MA-513_MA-516_MA-518 Shared Jurisdiction",
            False,
            id="compound-massachusetts-shared-jurisdiction",
        ),
        pytest.param("ma-510", False, id="lowercase-is-not-canonical"),
        pytest.param("MA-51", False, id="short-number-is-not-canonical"),
        pytest.param("MA-510;MA-511", False, id="multiple-ids-are-ambiguous"),
    ],
)
def test_historical_layer_id_must_identify_exactly_one_coc(coc_id, expected):
    assert _is_canonical_historical_layer_id(coc_id) is expected


def test_historical_coverage_summary_keeps_listing_and_archive_gaps_explicit():
    native_ids = {"AR-502", "CA-525", "FL-515"}
    archive_ids = {"AR-502", "KS-507"}

    summary = _historical_coverage_summary(native_ids, archive_ids)

    assert summary == {
        "coverage_status": "partial_unresolved",
        "coverage_reference": "native_2010_listing",
        "native_listing_id_count": 3,
        "observed_archive_id_count": 2,
        "shared_native_listing_id_count": 1,
        "native_listing_missing_ids": ["CA-525", "FL-515"],
        "archive_only_ids": ["KS-507"],
    }


def test_compound_shared_layer_does_not_become_a_canonical_id():
    assert not _is_canonical_historical_layer_id(
        "MA-510_MA-513_MA-516_MA-518 Shared Jurisdiction"
    )


def test_historical_archive_ingest_cli_passes_archives_to_production_ingest():
    output_path = Path("data/curated/coc_boundaries/coc__B2010.parquet")
    with patch(
        "hhplab.geographies.boundaries.hud.ingest_hud_exchange_state_archives",
        return_value=output_path,
    ) as ingest:
        result = runner.invoke(
            app,
            [
                "ingest",
                "boundaries",
                "--source",
                "hud_exchange",
                "--vintage",
                "2010",
                "--archive-dir",
                "/archives",
                "--archive-manifest",
                "/archives/manifest.json",
                "--force",
                "--json",
            ],
        )

    assert result.exit_code == 0
    assert '"status": "ok"' in result.output
    ingest.assert_called_once_with(
        "2010", Path("/archives"), Path("/archives/manifest.json"), force=True
    )


def test_historical_archive_ingest_cli_requires_both_local_inputs():
    result = runner.invoke(
        app,
        [
            "ingest",
            "boundaries",
            "--source",
            "hud_exchange",
            "--vintage",
            "2010",
            "--archive-dir",
            "/archives",
            "--json",
        ],
    )

    assert result.exit_code == 1
    assert "Both --archive-dir and --archive-manifest are required" in result.output
