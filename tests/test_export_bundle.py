"""Tests for export bundle generation.

Comprehensive tests covering:
- Export-N sequencing logic
- Copy mode operations (copy, hardlink, symlink)
- SHA-256 hashing
- Manifest generation
- Validation logic
- Integration (end-to-end bundle creation)
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from coclab.cli.export_bundle import _find_next_export_number
from coclab.export.copy import _copy_single_artifact, copy_artifacts, create_bundle_structure
from coclab.export.hashing import compute_sha256, hash_bundle_files, verify_file_hash
from coclab.export.manifest import (
    build_artifact_entry,
    build_manifest,
    get_zillow_attribution,
)
from coclab.export.selection import build_selection_plan
from coclab.export.types import ArtifactRecord, BundleConfig, SelectionPlan
from coclab.export.validate import (
    ExportValidationError,
    validate_panel_exists,
    validate_panel_schema,
    validate_selection_plan,
)

# ============================================================================
# Test Fixtures
# ============================================================================


def create_test_panel(path: Path, include_zori: bool = False) -> None:
    """Create a minimal test panel parquet file.

    Args:
        path: Path to write the parquet file
        include_zori: If True, include ZORI-related columns
    """
    data = {
        "coc_id": ["CO-500", "NY-600", "CA-501"],
        "year": [2024, 2024, 2024],
        "pit_total": [100, 200, 300],
    }

    if include_zori:
        data["rent_to_income"] = [0.3, 0.4, 0.35]
        data["zori_is_eligible"] = [True, True, False]

    table = pa.table(data)
    pq.write_table(table, path)


@pytest.fixture
def sample_artifact(tmp_path: Path) -> ArtifactRecord:
    """Create a sample artifact with a real source file."""
    source_file = tmp_path / "source" / "test_panel.parquet"
    source_file.parent.mkdir(parents=True)
    create_test_panel(source_file)

    return ArtifactRecord(
        role="panel",
        source_path=source_file,
        dest_path="data/panels/test_panel.parquet",
    )


@pytest.fixture
def sample_selection_plan(tmp_path: Path) -> SelectionPlan:
    """Create a sample selection plan with real files."""
    # Create panel artifact
    panel_path = tmp_path / "panels" / "panel.parquet"
    panel_path.parent.mkdir(parents=True)
    create_test_panel(panel_path)

    panel_artifact = ArtifactRecord(
        role="panel",
        source_path=panel_path,
        dest_path="data/panels/panel.parquet",
    )

    return SelectionPlan(
        panel_artifacts=[panel_artifact],
        input_artifacts=[],
        derived_artifacts=[],
        diagnostic_artifacts=[],
        codebook_artifacts=[],
        inferred_selections={},
    )


# ============================================================================
# Export-N Sequencing Tests
# ============================================================================


class TestFindNextExportNumber:
    """Tests for _find_next_export_number function."""

    def test_returns_1_for_empty_dir(self, tmp_path: Path):
        """Test that empty directory returns 1."""
        result = _find_next_export_number(tmp_path)
        assert result == 1

    def test_returns_1_for_nonexistent_dir(self, tmp_path: Path):
        """Test that non-existent directory returns 1."""
        nonexistent = tmp_path / "does_not_exist"
        result = _find_next_export_number(nonexistent)
        assert result == 1

    def test_returns_n_plus_1_when_exports_exist(self, tmp_path: Path):
        """Test returns N+1 when export-1, export-2 exist."""
        (tmp_path / "export-1").mkdir()
        (tmp_path / "export-2").mkdir()

        result = _find_next_export_number(tmp_path)
        assert result == 3

    def test_handles_gaps_in_sequence(self, tmp_path: Path):
        """Test handles gaps in export number sequence."""
        (tmp_path / "export-1").mkdir()
        (tmp_path / "export-5").mkdir()

        result = _find_next_export_number(tmp_path)
        assert result == 6

    def test_ignores_non_matching_directories(self, tmp_path: Path):
        """Test ignores directories that don't match export-N pattern."""
        (tmp_path / "export-1").mkdir()
        (tmp_path / "export-2").mkdir()
        (tmp_path / "other_dir").mkdir()
        (tmp_path / "export-not-a-number").mkdir()
        (tmp_path / "export_3").mkdir()  # Wrong delimiter

        result = _find_next_export_number(tmp_path)
        assert result == 3

    def test_ignores_files(self, tmp_path: Path):
        """Test ignores files that match pattern but are not directories."""
        (tmp_path / "export-1").mkdir()
        (tmp_path / "export-5").write_text("I am a file")

        result = _find_next_export_number(tmp_path)
        assert result == 2


# ============================================================================
# Copy Mode Tests
# ============================================================================


class TestCopyModes:
    """Tests for file copy operations with different modes."""

    def test_copy_mode_creates_independent_file(
        self, tmp_path: Path, sample_artifact: ArtifactRecord
    ):
        """Test that copy mode creates an independent file copy."""
        bundle_root = tmp_path / "bundle"
        bundle_root.mkdir()
        create_bundle_structure(bundle_root)

        result = _copy_single_artifact(sample_artifact, bundle_root, "copy")
        dest_path = bundle_root / result.dest_path

        assert dest_path.exists()
        assert dest_path.is_file()

        # Verify it's an independent copy (different inode)
        source_stat = sample_artifact.source_path.stat()
        dest_stat = dest_path.stat()
        assert source_stat.st_ino != dest_stat.st_ino

        # Verify bytes is populated
        assert result.bytes is not None
        assert result.bytes > 0

    def test_hardlink_mode_creates_hardlink(self, tmp_path: Path, sample_artifact: ArtifactRecord):
        """Test that hardlink mode creates a hardlink."""
        bundle_root = tmp_path / "bundle"
        bundle_root.mkdir()
        create_bundle_structure(bundle_root)

        result = _copy_single_artifact(sample_artifact, bundle_root, "hardlink")
        dest_path = bundle_root / result.dest_path

        assert dest_path.exists()
        assert dest_path.is_file()

        # Verify it's a hardlink (same inode)
        source_stat = sample_artifact.source_path.stat()
        dest_stat = dest_path.stat()
        assert source_stat.st_ino == dest_stat.st_ino

        # Verify link count increased
        assert dest_stat.st_nlink >= 2

    def test_symlink_mode_creates_symlink(self, tmp_path: Path, sample_artifact: ArtifactRecord):
        """Test that symlink mode creates a symbolic link."""
        bundle_root = tmp_path / "bundle"
        bundle_root.mkdir()
        create_bundle_structure(bundle_root)

        result = _copy_single_artifact(sample_artifact, bundle_root, "symlink")
        dest_path = bundle_root / result.dest_path

        assert dest_path.exists()
        assert dest_path.is_symlink()

        # Verify symlink resolves to original file
        assert dest_path.resolve() == sample_artifact.source_path.resolve()

    def test_invalid_copy_mode_raises(self, tmp_path: Path, sample_artifact: ArtifactRecord):
        """Test that invalid copy mode raises ValueError."""
        bundle_root = tmp_path / "bundle"
        bundle_root.mkdir()

        with pytest.raises(ValueError, match="Invalid copy_mode"):
            _copy_single_artifact(sample_artifact, bundle_root, "invalid_mode")


class TestCopyArtifacts:
    """Tests for copy_artifacts function."""

    def test_copies_all_artifacts(self, tmp_path: Path, sample_selection_plan: SelectionPlan):
        """Test that all artifacts are copied."""
        bundle_root = tmp_path / "bundle"
        bundle_root.mkdir()
        create_bundle_structure(bundle_root)

        results = copy_artifacts(sample_selection_plan, bundle_root, "copy")

        assert len(results) == 1
        dest_path = bundle_root / results[0].dest_path
        assert dest_path.exists()


class TestCreateBundleStructure:
    """Tests for create_bundle_structure function."""

    def test_creates_expected_directories(self, tmp_path: Path):
        """Test that all expected directories are created."""
        bundle_root = tmp_path / "bundle"
        bundle_root.mkdir()

        create_bundle_structure(bundle_root)

        expected_dirs = [
            "data/panels",
            "data/inputs/boundaries",
            "data/inputs/xwalks",
            "data/inputs/pit",
            "data/inputs/rents",
            "data/inputs/acs",
            "diagnostics",
            "codebook",
        ]

        for rel_dir in expected_dirs:
            dir_path = bundle_root / rel_dir
            assert dir_path.exists(), f"Missing directory: {rel_dir}"
            assert dir_path.is_dir()


# ============================================================================
# Hashing Tests
# ============================================================================


class TestComputeSha256:
    """Tests for compute_sha256 function."""

    def test_returns_correct_hash_for_known_content(self, tmp_path: Path):
        """Test SHA-256 hash matches expected value for known content."""
        test_file = tmp_path / "test.txt"
        test_content = b"hello world\n"
        test_file.write_bytes(test_content)

        result = compute_sha256(test_file)

        # Compute the expected hash
        import hashlib

        expected = hashlib.sha256(test_content).hexdigest()

        assert result == expected
        assert len(result) == 64  # SHA-256 produces 64 hex chars

    def test_raises_for_missing_file(self, tmp_path: Path):
        """Test raises FileNotFoundError for missing file."""
        missing = tmp_path / "does_not_exist.txt"

        with pytest.raises(FileNotFoundError):
            compute_sha256(missing)

    def test_hash_is_lowercase(self, tmp_path: Path):
        """Test that returned hash is lowercase."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        result = compute_sha256(test_file)

        assert result == result.lower()


class TestHashBundleFiles:
    """Tests for hash_bundle_files function."""

    def test_hashes_all_files(self, tmp_path: Path):
        """Test that all files in bundle are hashed."""
        bundle_root = tmp_path / "bundle"
        bundle_root.mkdir()
        (bundle_root / "file1.txt").write_text("content1")
        (bundle_root / "file2.txt").write_text("content2")
        subdir = bundle_root / "subdir"
        subdir.mkdir()
        (subdir / "file3.txt").write_text("content3")

        result = hash_bundle_files(bundle_root)

        assert "file1.txt" in result
        assert "file2.txt" in result
        assert "subdir/file3.txt" in result
        assert len(result) == 3

    def test_skips_manifest_json(self, tmp_path: Path):
        """Test that MANIFEST.json is skipped from hashing."""
        bundle_root = tmp_path / "bundle"
        bundle_root.mkdir()
        (bundle_root / "file1.txt").write_text("content1")
        (bundle_root / "MANIFEST.json").write_text('{"key": "value"}')

        result = hash_bundle_files(bundle_root)

        assert "file1.txt" in result
        assert "MANIFEST.json" not in result
        assert len(result) == 1


class TestVerifyFileHash:
    """Tests for verify_file_hash function."""

    def test_returns_true_for_matching_hash(self, tmp_path: Path):
        """Test returns True when hash matches."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        expected_hash = compute_sha256(test_file)

        assert verify_file_hash(test_file, expected_hash) is True

    def test_returns_true_for_case_insensitive_match(self, tmp_path: Path):
        """Test hash comparison is case-insensitive."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        expected_hash = compute_sha256(test_file)

        assert verify_file_hash(test_file, expected_hash.upper()) is True

    def test_returns_false_for_mismatched_hash(self, tmp_path: Path):
        """Test returns False when hash doesn't match."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        wrong_hash = "a" * 64

        assert verify_file_hash(test_file, wrong_hash) is False

    def test_returns_false_for_missing_file(self, tmp_path: Path):
        """Test returns False for non-existent file."""
        missing = tmp_path / "does_not_exist.txt"

        assert verify_file_hash(missing, "a" * 64) is False


# ============================================================================
# Manifest Generation Tests
# ============================================================================


class TestBuildManifest:
    """Tests for build_manifest function."""

    def test_includes_all_required_fields(self, tmp_path: Path):
        """Test manifest includes all required fields."""
        bundle_root = tmp_path / "bundle"
        bundle_root.mkdir()

        result = build_manifest(
            bundle_root=bundle_root,
            bundle_name="test_bundle",
            export_id="export-1",
            artifacts=[],
            parameters={"years": "2020-2024"},
            notes="Test notes",
        )

        assert "bundle_name" in result
        assert result["bundle_name"] == "test_bundle"
        assert "export_id" in result
        assert result["export_id"] == "export-1"
        assert "created_at_utc" in result
        assert "coclab" in result
        assert "parameters" in result
        assert "artifacts" in result
        assert "sources" in result
        assert "notes" in result

    def test_zillow_attribution_added_when_zori_present(self, tmp_path: Path):
        """Test Zillow attribution is added when ZORI files present."""
        bundle_root = tmp_path / "bundle"
        bundle_root.mkdir()

        # Create artifact with zori in path
        zori_artifact = ArtifactRecord(
            role="input",
            source_path=tmp_path / "zori.parquet",
            dest_path="data/inputs/rents/coc_zori.parquet",
        )

        result = build_manifest(
            bundle_root=bundle_root,
            bundle_name="test",
            export_id="export-1",
            artifacts=[zori_artifact],
            parameters={},
        )

        assert len(result["sources"]) > 0
        source_names = [s["name"] for s in result["sources"]]
        assert "Zillow Economic Research" in source_names

    def test_no_zillow_attribution_without_zori(self, tmp_path: Path):
        """Test no Zillow attribution when no ZORI files."""
        bundle_root = tmp_path / "bundle"
        bundle_root.mkdir()

        artifact = ArtifactRecord(
            role="panel",
            source_path=tmp_path / "panel.parquet",
            dest_path="data/panels/panel.parquet",
        )

        result = build_manifest(
            bundle_root=bundle_root,
            bundle_name="test",
            export_id="export-1",
            artifacts=[artifact],
            parameters={},
        )

        zillow_sources = [
            s for s in result["sources"] if s.get("name") == "Zillow Economic Research"
        ]
        assert len(zillow_sources) == 0


class TestBuildArtifactEntry:
    """Tests for build_artifact_entry function."""

    def test_includes_sha256_and_bytes(self, tmp_path: Path, sample_artifact: ArtifactRecord):
        """Test artifact entry includes sha256 and bytes."""
        bundle_root = tmp_path / "bundle"
        bundle_root.mkdir()
        create_bundle_structure(bundle_root)

        # Copy artifact to bundle first
        copied = _copy_single_artifact(sample_artifact, bundle_root, "copy")

        entry = build_artifact_entry(copied, bundle_root)

        assert "sha256" in entry
        assert entry["sha256"] is not None
        assert len(entry["sha256"]) == 64
        assert "bytes" in entry
        assert entry["bytes"] is not None
        assert entry["bytes"] > 0

    def test_includes_rows_for_parquet(self, tmp_path: Path, sample_artifact: ArtifactRecord):
        """Test artifact entry includes rows for parquet files."""
        bundle_root = tmp_path / "bundle"
        bundle_root.mkdir()
        create_bundle_structure(bundle_root)

        copied = _copy_single_artifact(sample_artifact, bundle_root, "copy")

        entry = build_artifact_entry(copied, bundle_root)

        assert "rows" in entry
        assert entry["rows"] == 3  # Our test panel has 3 rows


class TestGetZillowAttribution:
    """Tests for get_zillow_attribution function."""

    def test_returns_required_fields(self):
        """Test attribution dict has required fields."""
        result = get_zillow_attribution()

        assert "name" in result
        assert "metric" in result
        assert "attribution" in result
        assert "license_notes" in result

    def test_attribution_mentions_zillow(self):
        """Test attribution text mentions Zillow."""
        result = get_zillow_attribution()

        assert "Zillow" in result["attribution"]
        assert "ZORI" in result["attribution"]


# ============================================================================
# Validation Tests
# ============================================================================


class TestValidatePanelExists:
    """Tests for validate_panel_exists function."""

    def test_raises_for_missing_file(self, tmp_path: Path):
        """Test raises for non-existent file."""
        missing = tmp_path / "missing.parquet"

        with pytest.raises(ExportValidationError, match="does not exist"):
            validate_panel_exists(missing)

    def test_raises_for_directory(self, tmp_path: Path):
        """Test raises when path is a directory."""
        with pytest.raises(ExportValidationError, match="not a file"):
            validate_panel_exists(tmp_path)

    def test_passes_for_valid_parquet(self, tmp_path: Path):
        """Test passes for valid parquet file."""
        panel_path = tmp_path / "panel.parquet"
        create_test_panel(panel_path)

        # Should not raise
        validate_panel_exists(panel_path)


class TestValidatePanelSchema:
    """Tests for validate_panel_schema function."""

    def test_passes_for_valid_panel(self, tmp_path: Path):
        """Test passes for panel with required columns."""
        panel_path = tmp_path / "panel.parquet"
        create_test_panel(panel_path)

        # Should not raise
        validate_panel_schema(panel_path)

    def test_raises_for_missing_columns(self, tmp_path: Path):
        """Test raises when required columns missing."""
        panel_path = tmp_path / "panel.parquet"

        # Create panel without coc_id column
        table = pa.table(
            {
                "year": [2024, 2024],
                "pit_total": [100, 200],
            }
        )
        pq.write_table(table, panel_path)

        with pytest.raises(ExportValidationError, match="missing expected columns"):
            validate_panel_schema(panel_path)

    def test_accepts_custom_expected_columns(self, tmp_path: Path):
        """Test can specify custom expected columns."""
        panel_path = tmp_path / "panel.parquet"

        table = pa.table(
            {
                "custom_id": ["A", "B"],
                "value": [1, 2],
            }
        )
        pq.write_table(table, panel_path)

        # Should not raise with custom columns
        validate_panel_schema(panel_path, expected_cols=["custom_id", "value"])

    def test_detects_zori_panel_by_filename(self, tmp_path: Path):
        """Test detects ZORI panel by filename."""
        panel_path = tmp_path / "coc_panel__zori.parquet"
        create_test_panel(panel_path, include_zori=False)

        # Should raise because ZORI columns are expected but missing
        with pytest.raises(ExportValidationError, match="rent_to_income|zori_is_eligible"):
            validate_panel_schema(panel_path)

    def test_passes_for_zori_panel_with_columns(self, tmp_path: Path):
        """Test passes for ZORI panel with required columns."""
        panel_path = tmp_path / "coc_panel__zori.parquet"
        create_test_panel(panel_path, include_zori=True)

        # Should not raise
        validate_panel_schema(panel_path)


class TestValidateSelectionPlan:
    """Tests for validate_selection_plan function."""

    def test_raises_when_no_panel_artifacts(self, tmp_path: Path):
        """Test error when selection plan has no panel artifacts."""
        plan = SelectionPlan(
            panel_artifacts=[],
            input_artifacts=[],
            derived_artifacts=[],
            diagnostic_artifacts=[],
            codebook_artifacts=[],
        )

        errors = validate_selection_plan(plan)

        assert len(errors) > 0
        assert any("no panel artifacts" in e for e in errors)

    def test_checks_source_paths_exist(self, tmp_path: Path):
        """Test validates that source paths exist."""
        missing_path = tmp_path / "missing.parquet"

        plan = SelectionPlan(
            panel_artifacts=[
                ArtifactRecord(
                    role="panel",
                    source_path=missing_path,
                    dest_path="data/panels/missing.parquet",
                )
            ],
            input_artifacts=[],
            derived_artifacts=[],
            diagnostic_artifacts=[],
            codebook_artifacts=[],
        )

        errors = validate_selection_plan(plan)

        assert len(errors) > 0
        assert any("does not exist" in e for e in errors)

    def test_passes_for_valid_plan(self, tmp_path: Path, sample_selection_plan: SelectionPlan):
        """Test passes for valid selection plan."""
        errors = validate_selection_plan(sample_selection_plan)

        assert len(errors) == 0


# ============================================================================
# Integration Tests
# ============================================================================


class TestIntegration:
    """Integration tests for full export bundle workflow."""

    @pytest.fixture
    def curated_structure(self, tmp_path: Path) -> Path:
        """Create a minimal fake curated directory structure."""
        base = tmp_path / "project"
        base.mkdir()

        # Create panel directory and file
        panels_dir = base / "data" / "curated" / "panel"
        panels_dir.mkdir(parents=True)
        panel_path = panels_dir / "coc_panel__2020_2024.parquet"
        create_test_panel(panel_path)

        # Create diagnostics directory (empty but exists)
        diag_dir = base / "data" / "diagnostics"
        diag_dir.mkdir(parents=True)

        return base

    def test_end_to_end_bundle_creation(self, curated_structure: Path, tmp_path: Path):
        """Test full bundle creation workflow."""
        # Configure bundle
        config = BundleConfig(
            name="test_export",
            out_dir=tmp_path / "exports",
            panel_path=None,  # Will be inferred
            include={"panel", "manifest", "codebook"},
            copy_mode="copy",
        )

        # Build selection plan
        plan = build_selection_plan(config, base_dir=curated_structure)

        # Verify selection found our panel
        assert len(plan.panel_artifacts) == 1

        # Create bundle directory
        export_num = _find_next_export_number(config.out_dir)
        assert export_num == 1

        bundle_root = config.out_dir / f"export-{export_num}"
        bundle_root.mkdir(parents=True)
        create_bundle_structure(bundle_root)

        # Copy artifacts
        copied = copy_artifacts(plan, bundle_root, "copy")

        # Build manifest
        manifest = build_manifest(
            bundle_root=bundle_root,
            bundle_name=config.name,
            export_id=f"export-{export_num}",
            artifacts=copied,
            parameters={"include": sorted(config.include)},
        )

        # Write manifest
        manifest_path = bundle_root / "MANIFEST.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))

        # Verify bundle structure
        assert (bundle_root / "data" / "panels").exists()
        assert (bundle_root / "codebook").exists()
        assert manifest_path.exists()

        # Verify panel was copied
        panel_files = list((bundle_root / "data" / "panels").glob("*.parquet"))
        assert len(panel_files) == 1

        # Verify manifest content
        loaded_manifest = json.loads(manifest_path.read_text())
        assert loaded_manifest["bundle_name"] == "test_export"
        assert loaded_manifest["export_id"] == "export-1"
        assert len(loaded_manifest["artifacts"]) == 1

        # Verify artifact has sha256 and bytes
        artifact = loaded_manifest["artifacts"][0]
        assert artifact["sha256"] is not None
        assert artifact["bytes"] is not None
        assert artifact["rows"] == 3

    def test_manifest_hashes_match_actual_files(self, curated_structure: Path, tmp_path: Path):
        """Test that manifest hashes match actual file content."""
        config = BundleConfig(
            name="hash_test",
            out_dir=tmp_path / "exports",
            include={"panel"},
        )

        plan = build_selection_plan(config, base_dir=curated_structure)

        bundle_root = tmp_path / "exports" / "export-1"
        bundle_root.mkdir(parents=True)
        create_bundle_structure(bundle_root)

        copied = copy_artifacts(plan, bundle_root, "copy")
        manifest = build_manifest(
            bundle_root=bundle_root,
            bundle_name="hash_test",
            export_id="export-1",
            artifacts=copied,
            parameters={},
        )

        # Verify each artifact hash
        for artifact_entry in manifest["artifacts"]:
            artifact_path = bundle_root / artifact_entry["path"]
            expected_hash = artifact_entry["sha256"]

            assert verify_file_hash(artifact_path, expected_hash)

    def test_second_export_gets_next_number(self, curated_structure: Path, tmp_path: Path):
        """Test that subsequent exports get incrementing numbers."""
        exports_dir = tmp_path / "exports"

        # Create first export
        (exports_dir / "export-1").mkdir(parents=True)

        # Find next number
        next_num = _find_next_export_number(exports_dir)
        assert next_num == 2

        # Create second export
        (exports_dir / "export-2").mkdir()

        # Find next number again
        next_num = _find_next_export_number(exports_dir)
        assert next_num == 3


class TestBundleWithZori:
    """Tests for bundles containing ZORI data."""

    @pytest.fixture
    def zori_curated_structure(self, tmp_path: Path) -> Path:
        """Create curated structure with ZORI panel."""
        base = tmp_path / "project"
        base.mkdir()

        # Create panel with ZORI columns
        panels_dir = base / "data" / "curated" / "panel"
        panels_dir.mkdir(parents=True)
        panel_path = panels_dir / "coc_panel__2020_2024__zori.parquet"
        create_test_panel(panel_path, include_zori=True)

        return base

    def test_zori_panel_includes_zillow_attribution(
        self, zori_curated_structure: Path, tmp_path: Path
    ):
        """Test ZORI panel triggers Zillow attribution in manifest."""
        config = BundleConfig(
            name="zori_test",
            out_dir=tmp_path / "exports",
            include={"panel"},
        )

        plan = build_selection_plan(config, base_dir=zori_curated_structure)
        assert len(plan.panel_artifacts) == 1

        bundle_root = tmp_path / "exports" / "export-1"
        bundle_root.mkdir(parents=True)
        create_bundle_structure(bundle_root)

        copied = copy_artifacts(plan, bundle_root, "copy")

        # The artifact path contains 'zori', so attribution should be added
        manifest = build_manifest(
            bundle_root=bundle_root,
            bundle_name="zori_test",
            export_id="export-1",
            artifacts=copied,
            parameters={},
        )

        # Check for Zillow attribution
        assert len(manifest["sources"]) > 0
        zillow_sources = [s for s in manifest["sources"] if "Zillow" in s.get("name", "")]
        assert len(zillow_sources) == 1
