"""Tests for the provenance metadata module."""

import json

import pandas as pd
import pyarrow.parquet as pq
import pytest

from coclab.provenance import (
    PROVENANCE_KEY,
    ProvenanceBlock,
    has_provenance,
    read_provenance,
    write_parquet_with_provenance,
)


@pytest.fixture
def full_block():
    """ProvenanceBlock with every field populated."""
    return ProvenanceBlock(
        boundary_vintage="2025",
        tract_vintage="2023",
        county_vintage="2023",
        acs_vintage="2022",
        notation="A2022@B2025×T2023",
        weighting="population",
        geo_type="coc",
        definition_version="glynn_fox_v1",
        created_at="2025-01-15T12:00:00+00:00",
        coclab_version="0.1.0",
        extra={"source_url": "https://example.com", "note": "test run"},
    )


@pytest.fixture
def minimal_block():
    """ProvenanceBlock with only defaults (all optional fields None)."""
    return ProvenanceBlock(
        created_at="2025-06-01T00:00:00+00:00",
    )


@pytest.fixture
def sample_df():
    """Small DataFrame for Parquet round-trip tests."""
    return pd.DataFrame({"coc_id": ["CO-500", "CO-501"], "value": [1, 2]})


# ── to_dict / from_dict round-trip ──────────────────────────────────────────


class TestDictRoundTrip:
    """ProvenanceBlock.to_dict() / from_dict() preserves all fields."""

    def test_full_block_round_trip(self, full_block):
        restored = ProvenanceBlock.from_dict(full_block.to_dict())
        assert restored.boundary_vintage == full_block.boundary_vintage
        assert restored.tract_vintage == full_block.tract_vintage
        assert restored.county_vintage == full_block.county_vintage
        assert restored.acs_vintage == full_block.acs_vintage
        assert restored.notation == full_block.notation
        assert restored.weighting == full_block.weighting
        assert restored.geo_type == full_block.geo_type
        assert restored.definition_version == full_block.definition_version
        assert restored.created_at == full_block.created_at
        assert restored.coclab_version == full_block.coclab_version
        assert restored.extra == full_block.extra

    def test_to_dict_omits_none_and_empty(self, minimal_block):
        d = minimal_block.to_dict()
        for key in ("boundary_vintage", "tract_vintage", "acs_vintage", "weighting"):
            assert key not in d
        assert "extra" not in d

    def test_from_dict_unknown_keys_land_in_extra(self):
        data = {
            "boundary_vintage": "2025",
            "created_at": "2025-01-01T00:00:00+00:00",
            "custom_field": "custom_value",
            "run_id": 42,
        }
        block = ProvenanceBlock.from_dict(data)
        assert block.boundary_vintage == "2025"
        assert block.extra == {"custom_field": "custom_value", "run_id": 42}

    def test_from_dict_merges_unknown_keys_with_existing_extra(self):
        data = {
            "boundary_vintage": "2025",
            "created_at": "2025-01-01T00:00:00+00:00",
            "extra": {"existing": True},
            "surprise": "hello",
        }
        block = ProvenanceBlock.from_dict(data)
        assert block.extra == {"existing": True, "surprise": "hello"}


# ── to_json / from_json round-trip ──────────────────────────────────────────


class TestJsonRoundTrip:
    """ProvenanceBlock.to_json() / from_json() round-trip."""

    def test_full_block_json_round_trip(self, full_block):
        restored = ProvenanceBlock.from_json(full_block.to_json())
        assert restored.boundary_vintage == full_block.boundary_vintage
        assert restored.extra == full_block.extra

    def test_json_output_is_valid_json(self, full_block):
        parsed = json.loads(full_block.to_json())
        assert isinstance(parsed, dict)
        assert parsed["boundary_vintage"] == "2025"

    def test_minimal_block_json_round_trip(self, minimal_block):
        restored = ProvenanceBlock.from_json(minimal_block.to_json())
        assert restored.created_at == minimal_block.created_at
        assert restored.boundary_vintage is None


# ── generate_notation ────────────────────────────────────────────────────────


class TestGenerateNotation:
    """generate_notation() produces expected notation strings."""

    @pytest.mark.parametrize(
        ("acs", "boundary", "tract", "county", "expected"),
        [
            ("2022", "2025", "2023", None, "A2022@B2025×T2023"),
            ("2022", "2025", None, "2023", "A2022@B2025×C2023"),
            ("2022", "2025", "2023", "2023", "A2022@B2025×T2023"),
            ("2022", "2025", None, None, "A2022@B2025"),
            (None, "2025", "2023", None, "@B2025×T2023"),
        ],
        ids=[
            "acs-boundary-tract",
            "acs-boundary-county",
            "tract-beats-county",
            "no-intermediary",
            "no-acs",
        ],
    )
    def test_known_vintages(self, acs, boundary, tract, county, expected):
        block = ProvenanceBlock(
            acs_vintage=acs,
            boundary_vintage=boundary,
            tract_vintage=tract,
            county_vintage=county,
        )
        assert block.generate_notation() == expected

    @pytest.mark.parametrize(
        ("acs", "boundary", "tract"),
        [
            (None, None, None),
            ("2022", None, None),
            (None, None, "2023"),
        ],
        ids=["all-none", "only-acs", "only-tract"],
    )
    def test_insufficient_vintages_returns_none(self, acs, boundary, tract):
        block = ProvenanceBlock(
            acs_vintage=acs, boundary_vintage=boundary, tract_vintage=tract
        )
        assert block.generate_notation() is None


# ── Parquet round-trip ───────────────────────────────────────────────────────


class TestParquetRoundTrip:
    """write_parquet_with_provenance / read_provenance round-trip."""

    def test_write_then_read(self, tmp_path, sample_df, full_block):
        path = tmp_path / "out.parquet"
        returned = write_parquet_with_provenance(sample_df, path, full_block)
        assert returned == path
        assert path.exists()

        restored = read_provenance(path)
        assert restored is not None
        assert restored.boundary_vintage == full_block.boundary_vintage
        assert restored.extra == full_block.extra

    def test_data_survives_provenance_write(self, tmp_path, sample_df, full_block):
        path = tmp_path / "out.parquet"
        write_parquet_with_provenance(sample_df, path, full_block)
        df_back = pd.read_parquet(path)
        pd.testing.assert_frame_equal(df_back, sample_df)

    def test_creates_parent_directories(self, tmp_path, sample_df, minimal_block):
        path = tmp_path / "a" / "b" / "c" / "nested.parquet"
        write_parquet_with_provenance(sample_df, path, minimal_block)
        assert path.exists()

    def test_read_provenance_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            read_provenance(tmp_path / "nonexistent.parquet")


# ── has_provenance ───────────────────────────────────────────────────────────


class TestHasProvenance:
    """has_provenance() distinguishes provenance files from plain Parquet."""

    def test_true_for_provenance_file(self, tmp_path, sample_df, full_block):
        path = tmp_path / "with_prov.parquet"
        write_parquet_with_provenance(sample_df, path, full_block)
        assert has_provenance(path) is True

    def test_false_for_plain_parquet(self, tmp_path, sample_df):
        path = tmp_path / "plain.parquet"
        sample_df.to_parquet(path)
        assert has_provenance(path) is False

    def test_false_for_missing_file(self, tmp_path):
        assert has_provenance(tmp_path / "nope.parquet") is False


# ── Edge cases ───────────────────────────────────────────────────────────────


class TestEdgeCases:
    """Edge cases: empty extra, all-None optional fields, malformed metadata."""

    def test_empty_extra_excluded_from_dict(self):
        block = ProvenanceBlock(created_at="2025-01-01T00:00:00+00:00")
        assert "extra" not in block.to_dict()

    def test_all_none_optional_fields(self, minimal_block):
        d = minimal_block.to_dict()
        assert "created_at" in d
        assert "coclab_version" in d
        expected_absent = {
            "boundary_vintage",
            "tract_vintage",
            "county_vintage",
            "acs_vintage",
            "notation",
            "weighting",
            "geo_type",
            "definition_version",
        }
        for key in expected_absent:
            assert key not in d

    def test_malformed_provenance_returns_none(self, tmp_path, sample_df):
        import pyarrow as pa

        path = tmp_path / "bad_meta.parquet"
        table = pa.Table.from_pandas(sample_df, preserve_index=False)
        table = table.replace_schema_metadata({PROVENANCE_KEY: b"not valid json"})
        pq.write_table(table, path)

        assert read_provenance(path) is None
        assert has_provenance(path) is False
