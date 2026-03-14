"""Curated layout compliance tests.

Validates that the curated_policy module correctly detects:
- Non-canonical filenames in artifact subdirectories
- Nested directory violations under flat artifact folders
- Unknown subdirectories under data/curated/
- Correct acceptance of canonical filenames and ignored files

All tests use tmp_path with synthetic directory structures.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from coclab.curated_policy import (
    CANONICAL_PATTERNS,
    CURATED_SUBDIRS,
    validate_curated_layout,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _touch(path: Path) -> Path:
    """Create a file and its parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
    return path


def _build_clean_curated(base: Path) -> None:
    """Create a minimal clean curated layout with one canonical file per subdir."""
    canonical_examples = {
        "coc_boundaries": "coc__B2025.parquet",
        "tiger": "tracts__T2023.parquet",
        "xwalks": "xwalk__B2025xT2023.parquet",
        "acs": "acs5_tracts__A2023xT2023.parquet",
        "measures": "measures__A2023@B2025xT2023.parquet",
        "zori": "zori__A2023@B2025xC2023__wrenter.parquet",
        "pep": "pep_county__v2024.parquet",
        "pit": "pit_vintage__P2024.parquet",
        "panel": "panel__Y2015-2024@B2025.parquet",
        "metro": "metro_definitions__glynn_fox_v1.parquet",
        "maps": "CO-500__2025.html",
    }
    for subdir, filename in canonical_examples.items():
        _touch(base / subdir / filename)


# ---------------------------------------------------------------------------
# Core validation tests
# ---------------------------------------------------------------------------


class TestCleanLayout:
    """A fully canonical layout should produce zero violations."""

    def test_clean_layout_no_violations(self, tmp_path: Path) -> None:
        curated = tmp_path / "curated"
        _build_clean_curated(curated)
        violations = validate_curated_layout(curated)
        assert violations == [], f"Expected no violations, got: {violations}"

    def test_missing_dir_returns_empty(self, tmp_path: Path) -> None:
        violations = validate_curated_layout(tmp_path / "nonexistent")
        assert violations == []


class TestNonCanonicalFilenames:
    """Non-canonical filenames must be detected."""

    @pytest.mark.parametrize(
        "subdir, bad_name",
        [
            ("coc_boundaries", "boundaries_2025.parquet"),
            ("tiger", "tracts_2023.parquet"),
            ("xwalks", "crosswalk_B2025_T2023.parquet"),
            ("acs", "acs_population_2023.parquet"),
            ("measures", "my_measures.parquet"),
            ("zori", "zori_data.parquet"),
            ("pep", "population_estimates.parquet"),
            ("pit", "pit_counts.parquet"),
            ("panel", "panel_data.parquet"),
            ("maps", "report.pdf"),
        ],
    )
    def test_non_canonical_filename_detected(
        self, tmp_path: Path, subdir: str, bad_name: str
    ) -> None:
        curated = tmp_path / "curated"
        _touch(curated / subdir / bad_name)
        violations = validate_curated_layout(curated)
        non_canonical = [v for v in violations if v.category == "non_canonical"]
        assert len(non_canonical) == 1
        assert bad_name in non_canonical[0].message

    def test_legacy_boundary_name_accepted(self, tmp_path: Path) -> None:
        """Legacy boundaries__B{year}.parquet should be accepted."""
        curated = tmp_path / "curated"
        _touch(curated / "coc_boundaries" / "boundaries__B2024.parquet")
        violations = validate_curated_layout(curated)
        assert violations == []


class TestCanonicalFilenamesAccepted:
    """Verify that all canonical patterns are properly accepted."""

    @pytest.mark.parametrize(
        "subdir, good_name",
        [
            ("coc_boundaries", "coc__B2025.parquet"),
            ("coc_boundaries", "boundaries__B2024.parquet"),
            ("tiger", "tracts__T2023.parquet"),
            ("tiger", "counties__C2023.parquet"),
            ("tiger", "tract_relationship__T2010xT2020.parquet"),
            ("xwalks", "xwalk__B2025xT2023.parquet"),
            ("xwalks", "xwalk__B2025xC2023.parquet"),
            ("acs", "acs5_tracts__A2023xT2023.parquet"),
            ("acs", "county_weights__A2023__wrenter.parquet"),
            ("measures", "measures__A2023@B2025xT2023.parquet"),
            ("measures", "measures__A2023@B2025.parquet"),
            ("measures", "measures__A2015(2013)@B2013xT2010.parquet"),
            ("zori", "zori__A2023@B2025xC2023__wrenter.parquet"),
            ("zori", "zori_yearly__A2023@B2025xC2023__wrenter__mpit_january.parquet"),
            ("zori", "zori__county__Z2026.parquet"),
            ("pep", "pep_county__v2024.parquet"),
            ("pep", "pep_county__combined.parquet"),
            ("pep", "pep_county__intercensal_2010_2020.parquet"),
            ("pep", "coc_pep__B2024xC2024__warea_share__2010_2024.parquet"),
            ("pit", "pit__P2024.parquet"),
            ("pit", "pit__P2024@B2024.parquet"),
            ("pit", "pit_vintage__P2024.parquet"),
            ("pit", "pit_vintage_registry.parquet"),
            ("pit", "pit_registry.parquet"),
            ("panel", "panel__Y2015-2024@B2025.parquet"),
            ("panel", "panel__metro__Y2011-2016@Dglynnfoxv1.parquet"),
            ("panel", "panel__Y2015-2024@B2025.manifest.json"),
            ("panel", "panel__metro__Y2011-2016@Dglynnfoxv1.manifest.json"),
            ("panel", "panel__metro__Y2011-2016@Dglynnfoxv1__diagnostics.json"),
            ("measures", "measures__metro__A2023@Dglynnfoxv1xT2020.parquet"),
            ("pit", "pit__metro__P2024@Dglynnfoxv1.parquet"),
            ("pep", "pep_county__v2020__y2011-2016.parquet"),
            ("pep", "pep__metro__Dglynnfoxv1xC2020__wpop__2011_2016.parquet"),
            ("zori", "zori__metro__A2023@Dglynnfoxv1xC2023__wrenter.parquet"),
            ("metro", "metro_definitions__glynn_fox_v1.parquet"),
            ("metro", "metro_coc_membership__glynn_fox_v1.parquet"),
            ("metro", "metro_county_membership__glynn_fox_v1.parquet"),
            ("maps", "CO-500__2025.html"),
        ],
    )
    def test_canonical_name_accepted(
        self, tmp_path: Path, subdir: str, good_name: str
    ) -> None:
        curated = tmp_path / "curated"
        _touch(curated / subdir / good_name)
        violations = validate_curated_layout(curated)
        assert violations == [], f"Unexpected violation for {subdir}/{good_name}: {violations}"


class TestNestedPaths:
    """Nested directories under artifact folders must be flagged."""

    def test_nested_data_dir_detected(self, tmp_path: Path) -> None:
        curated = tmp_path / "curated"
        _touch(curated / "tiger" / "data" / "raw" / "some_file.shp")
        violations = validate_curated_layout(curated)
        nested = [v for v in violations if v.category == "nested_path"]
        assert len(nested) >= 1
        assert any("data" in v.message for v in nested)

    def test_nested_subdir_detected(self, tmp_path: Path) -> None:
        curated = tmp_path / "curated"
        _touch(curated / "acs" / "backup" / "old_file.parquet")
        violations = validate_curated_layout(curated)
        nested = [v for v in violations if v.category == "nested_path"]
        assert len(nested) >= 1
        assert any("backup" in v.message for v in nested)


class TestUnknownSubdirs:
    """Unknown subdirectories under curated/ must be flagged."""

    def test_unknown_subdir_detected(self, tmp_path: Path) -> None:
        curated = tmp_path / "curated"
        (curated / "scratch").mkdir(parents=True)
        violations = validate_curated_layout(curated)
        unknown = [v for v in violations if v.category == "unknown_subdir"]
        assert len(unknown) == 1
        assert "scratch" in unknown[0].message

    def test_known_subdirs_accepted(self, tmp_path: Path) -> None:
        curated = tmp_path / "curated"
        for name in CURATED_SUBDIRS:
            (curated / name).mkdir(parents=True, exist_ok=True)
        violations = validate_curated_layout(curated)
        assert violations == []


class TestIgnoredFiles:
    """.DS_Store and registry files should be ignored."""

    def test_ds_store_ignored(self, tmp_path: Path) -> None:
        curated = tmp_path / "curated"
        _touch(curated / "tiger" / ".DS_Store")
        _touch(curated / ".DS_Store")
        violations = validate_curated_layout(curated)
        assert violations == []

    def test_root_registry_ignored(self, tmp_path: Path) -> None:
        curated = tmp_path / "curated"
        _touch(curated / "boundary_registry.parquet")
        _touch(curated / "source_registry.parquet")
        violations = validate_curated_layout(curated)
        assert violations == []

    def test_unexpected_root_file_flagged(self, tmp_path: Path) -> None:
        curated = tmp_path / "curated"
        _touch(curated / "notes.txt")
        violations = validate_curated_layout(curated)
        assert len(violations) == 1
        assert violations[0].category == "non_canonical"
        assert "notes.txt" in violations[0].message


class TestDotDirsIgnored:
    """Hidden directories (starting with .) at curated root should be skipped."""

    def test_hidden_dir_ignored(self, tmp_path: Path) -> None:
        curated = tmp_path / "curated"
        _touch(curated / ".git" / "config")
        violations = validate_curated_layout(curated)
        assert violations == []


# ---------------------------------------------------------------------------
# CLI integration tests
# ---------------------------------------------------------------------------


class TestCLI:
    """Test the validate curated-layout CLI command."""

    def test_clean_layout_exit_0(self, tmp_path: Path) -> None:
        from coclab.cli.main import app

        curated = tmp_path / "curated"
        _build_clean_curated(curated)

        runner = CliRunner()
        result = runner.invoke(app, ["validate", "curated-layout", "--dir", str(curated)])
        assert result.exit_code == 0, f"Exit {result.exit_code}:\n{result.output}"
        assert "no violations" in result.output.lower()

    def test_violations_exit_1(self, tmp_path: Path) -> None:
        from coclab.cli.main import app

        curated = tmp_path / "curated"
        _touch(curated / "tiger" / "bad_file.parquet")

        runner = CliRunner()
        result = runner.invoke(app, ["validate", "curated-layout", "--dir", str(curated)])
        assert result.exit_code == 1, f"Exit {result.exit_code}:\n{result.output}"
        assert "bad_file.parquet" in result.output
        assert "Total violations:" in result.output


# ---------------------------------------------------------------------------
# Pattern coverage sanity check
# ---------------------------------------------------------------------------


class TestPatternCoverage:
    """Ensure every known curated subdir has at least one canonical pattern."""

    def test_all_subdirs_have_patterns(self) -> None:
        missing = CURATED_SUBDIRS - set(CANONICAL_PATTERNS.keys())
        assert not missing, f"Subdirs without canonical patterns: {missing}"


# ---------------------------------------------------------------------------
# Migration utility tests
# ---------------------------------------------------------------------------


class TestCuratedMigration:
    """Tests for the curated data migration utility."""

    def test_empty_dir_returns_empty_plan(self, tmp_path: Path) -> None:
        from coclab.curated_migrate import scan_curated_for_migration

        plan = scan_curated_for_migration(tmp_path)
        assert plan.renames == []
        assert plan.duplicates == []
        assert plan.unknown == []

    def test_canonical_files_not_flagged(self, tmp_path: Path) -> None:
        from coclab.curated_migrate import scan_curated_for_migration

        (tmp_path / "coc_boundaries").mkdir()
        (tmp_path / "coc_boundaries" / "coc__B2025.parquet").touch()
        plan = scan_curated_for_migration(tmp_path)
        assert plan.renames == []
        assert plan.unknown == []

    def test_legacy_boundary_rename_proposed(self, tmp_path: Path) -> None:
        from coclab.curated_migrate import scan_curated_for_migration

        (tmp_path / "coc_boundaries").mkdir()
        (tmp_path / "coc_boundaries" / "boundaries__B2025.parquet").touch()
        plan = scan_curated_for_migration(tmp_path)
        # boundaries__B{year} is recognized as canonical by policy, so no rename
        assert plan.renames == []

    def test_legacy_measures_rename_proposed(self, tmp_path: Path) -> None:
        from coclab.curated_migrate import scan_curated_for_migration

        (tmp_path / "measures").mkdir()
        (tmp_path / "measures" / "coc_measures__2025__2023.parquet").touch()
        plan = scan_curated_for_migration(tmp_path)
        assert len(plan.renames) == 1
        assert plan.renames[0].action == "rename"
        assert "measures__A2023@B2025" in str(plan.renames[0].target)

    def test_duplicate_detected(self, tmp_path: Path) -> None:
        from coclab.curated_migrate import scan_curated_for_migration

        (tmp_path / "measures").mkdir()
        # Both legacy and canonical exist
        (tmp_path / "measures" / "coc_measures__2025__2023.parquet").touch()
        (tmp_path / "measures" / "measures__A2023@B2025.parquet").touch()
        plan = scan_curated_for_migration(tmp_path)
        assert len(plan.duplicates) == 1
        assert plan.duplicates[0].action == "duplicate"

    def test_apply_dry_run_does_not_rename(self, tmp_path: Path) -> None:
        from coclab.curated_migrate import apply_migration, scan_curated_for_migration

        (tmp_path / "measures").mkdir()
        src = tmp_path / "measures" / "coc_measures__2025__2023.parquet"
        src.touch()
        plan = scan_curated_for_migration(tmp_path)
        log = apply_migration(plan, dry_run=True)
        assert src.exists()  # Not renamed
        assert len(log) >= 1
        assert "[DRY-RUN]" in log[0]

    def test_apply_executes_rename(self, tmp_path: Path) -> None:
        from coclab.curated_migrate import apply_migration, scan_curated_for_migration

        (tmp_path / "measures").mkdir()
        src = tmp_path / "measures" / "coc_measures__2025__2023.parquet"
        src.touch()
        plan = scan_curated_for_migration(tmp_path)
        log = apply_migration(plan, dry_run=False)
        assert not src.exists()
        assert (tmp_path / "measures" / "measures__A2023@B2025.parquet").exists()
        assert "[DRY-RUN]" not in log[0]


# ---------------------------------------------------------------------------
# PEP naming helper tests
# ---------------------------------------------------------------------------


class TestPepNaming:
    """Tests for the PEP canonical naming helper."""

    def test_coc_pep_filename(self) -> None:
        from coclab.naming import coc_pep_filename

        result = coc_pep_filename(2024, 2024, "area_share", 2020, 2024)
        assert result == "coc_pep__B2024xC2024__warea_share__2020_2024.parquet"

    def test_coc_pep_filename_string_vintages(self) -> None:
        from coclab.naming import coc_pep_filename

        result = coc_pep_filename("2025", "2020", "pop", 2018, 2024)
        assert result == "coc_pep__B2025xC2020__wpop__2018_2024.parquet"
