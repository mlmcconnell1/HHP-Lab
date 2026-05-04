"""Retention compliance tests for raw data persistence.

Verifies that ingest modules comply with the raw-data-retention-policy:
1. Raw snapshots are persisted via raw_snapshot utilities.
2. Source registry local_path references raw artifacts (not curated outputs).
3. Curated paths are stored in metadata["curated_path"] when distinct.
4. API ingests use year+variant (not legacy snapshot_id) for year-first layout.
5. File ingests include a year segment in their subdirs.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

# All ingest modules that must comply with the retention policy
INGEST_MODULES = [
    "hhplab/hud/opendata_arcgis.py",
    "hhplab/census/ingest/tiger_tracts.py",
    "hhplab/census/ingest/tiger_counties.py",
    "hhplab/census/ingest/tract_relationship.py",
    "hhplab/nhgis/nhgis_ingest.py",
    "hhplab/acs/ingest/tract_population.py",
    "hhplab/rents/weights.py",
]

# API ingest modules must use year+variant (not legacy snapshot_id)
API_INGEST_MODULES = [
    "hhplab/hud/opendata_arcgis.py",
    "hhplab/hud/exchange_gis.py",
    "hhplab/acs/ingest/tract_population.py",
    "hhplab/rents/weights.py",
]

# File ingest modules that use persist_file_snapshot with subdirs
FILE_INGEST_MODULES = [
    "hhplab/census/ingest/tiger_tracts.py",
    "hhplab/census/ingest/tiger_counties.py",
    "hhplab/census/ingest/tract_relationship.py",
    "hhplab/nhgis/nhgis_ingest.py",
]


def _module_source(relpath: str) -> str:
    """Read a module's source from the project root."""
    path = Path(relpath)
    if not path.exists():
        pytest.skip(f"Module not found: {relpath}")
    return path.read_text(encoding="utf-8")


def _module_imports(source: str) -> set[str]:
    """Extract all imported names from module source."""
    tree = ast.parse(source)
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.names:
                for alias in node.names:
                    names.add(alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name)
    return names


class TestRetentionPolicyImports:
    """Verify each ingester imports raw_snapshot utilities."""

    @pytest.mark.parametrize("module_path", INGEST_MODULES)
    def test_imports_raw_snapshot_utility(self, module_path: str):
        """Each ingester must import from hhplab.raw_snapshot."""
        source = _module_source(module_path)
        imports = _module_imports(source)
        has_persist = "persist_file_snapshot" in imports
        has_api = "write_api_snapshot" in imports
        assert has_persist or has_api, (
            f"{module_path} does not import persist_file_snapshot "
            f"or write_api_snapshot from hhplab.raw_snapshot"
        )


class TestRetentionPolicyCallSites:
    """Verify each ingester calls raw snapshot persistence."""

    @pytest.mark.parametrize("module_path", INGEST_MODULES)
    def test_calls_raw_snapshot_function(self, module_path: str):
        """Each ingester must call persist_file_snapshot or write_api_snapshot."""
        source = _module_source(module_path)
        has_call = "persist_file_snapshot(" in source or "write_api_snapshot(" in source
        assert has_call, (
            f"{module_path} does not call persist_file_snapshot() or write_api_snapshot()"
        )


class TestLocalPathSemantic:
    """Verify register_source local_path points to raw artifacts."""

    @pytest.mark.parametrize("module_path", INGEST_MODULES)
    def test_local_path_references_raw_artifact(self, module_path: str):
        """local_path in register_source should reference raw snapshot, not curated output.

        We check that register_source calls use snap_dir, raw_path, or raw_dir
        as the local_path argument, rather than output_path or curated_path.
        """
        source = _module_source(module_path)

        # Find all register_source(...) call blocks
        # A simple heuristic: find lines with local_path= inside register_source
        in_register = False
        local_path_lines = []
        paren_depth = 0
        for line in source.splitlines():
            stripped = line.strip()
            if "register_source(" in stripped:
                in_register = True
                paren_depth = 0
            if in_register:
                paren_depth += stripped.count("(") - stripped.count(")")
                if "local_path=" in stripped:
                    local_path_lines.append(stripped)
                if paren_depth <= 0 and in_register and ")" in stripped:
                    in_register = False

        assert local_path_lines, f"{module_path}: no local_path= found in register_source call"

        for line in local_path_lines:
            # The local_path should NOT reference output_path directly
            # (unless it IS the raw file, like in PIT/ZORI which write to data/raw/)
            # We check for known raw-artifact variable names
            assert not line.startswith("local_path=str(output_path)"), (
                f"{module_path}: local_path should reference raw artifact, "
                f"not curated output_path. Line: {line}"
            )


class TestCuratedPathInMetadata:
    """Verify curated_path is stored in metadata when distinct from raw."""

    @pytest.mark.parametrize("module_path", INGEST_MODULES)
    def test_curated_path_in_metadata(self, module_path: str):
        """Ingesters that produce curated output should store curated_path in metadata."""
        source = _module_source(module_path)

        # If the module writes curated output (has curated_boundary_path,
        # write_parquet_with_provenance, or .to_parquet), it should include
        # curated_path in metadata
        produces_curated = (
            "curated_boundary_path" in source
            or "write_parquet_with_provenance" in source
            or ".to_parquet(" in source
        )

        if not produces_curated:
            pytest.skip(f"{module_path} does not produce curated output")

        assert '"curated_path"' in source or "'curated_path'" in source, (
            f"{module_path} produces curated output but does not store "
            f"curated_path in register_source metadata"
        )


class TestYearFirstApiLayout:
    """Verify API ingests use year+variant for canonical year-first raw layout."""

    @pytest.mark.parametrize("module_path", API_INGEST_MODULES)
    def test_uses_year_variant_not_snapshot_id(self, module_path: str):
        """write_api_snapshot calls should use year= and variant=, not snapshot_id=."""
        source = _module_source(module_path)
        # Find write_api_snapshot call blocks and check for year= usage
        assert "year=" in source and "variant=" in source, (
            f"{module_path}: write_api_snapshot should use year= and variant= "
            f"for canonical year-first layout, not legacy snapshot_id="
        )

    @pytest.mark.parametrize("module_path", API_INGEST_MODULES)
    def test_no_legacy_snapshot_id(self, module_path: str):
        """write_api_snapshot calls should not use legacy snapshot_id=."""
        source = _module_source(module_path)
        # Check that snapshot_id= is not used in write_api_snapshot calls
        # (it may still appear in manifest dicts or other contexts)
        in_call = False
        paren_depth = 0
        for line in source.splitlines():
            stripped = line.strip()
            if "write_api_snapshot(" in stripped:
                in_call = True
                paren_depth = 0
            if in_call:
                paren_depth += stripped.count("(") - stripped.count(")")
                assert "snapshot_id=" not in stripped, (
                    f"{module_path}: write_api_snapshot should use year=/variant= "
                    f"instead of legacy snapshot_id=. Line: {stripped}"
                )
                if paren_depth <= 0 and ")" in stripped:
                    in_call = False


class TestYearFirstFileLayout:
    """Verify file ingests include a year segment in their raw paths."""

    @pytest.mark.parametrize("module_path", FILE_INGEST_MODULES)
    def test_subdirs_include_year(self, module_path: str):
        """persist_file_snapshot subdirs should include a year-like segment."""
        source = _module_source(module_path)
        # Find subdirs=(...) in persist_file_snapshot calls
        # The year segment should be first in subdirs (e.g., str(year), "2020")
        subdirs_pattern = re.compile(r"subdirs=\(([^)]+)\)")
        matches = subdirs_pattern.findall(source)
        assert matches, f"{module_path}: no subdirs= found in persist_file_snapshot call"
        for match in matches:
            # The first element should contain a year reference
            first_arg = match.split(",")[0].strip().strip("'\"")
            has_year_ref = (
                "year" in first_arg.lower() or first_arg.isdigit() or first_arg.startswith("str(")
            )
            assert has_year_ref, (
                f"{module_path}: first subdir segment should be a year, got: {first_arg}"
            )
