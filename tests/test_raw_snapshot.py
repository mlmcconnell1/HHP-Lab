"""Tests for raw snapshot utilities.

Covers:
- persist_file_snapshot: file-based raw snapshot persistence
- write_api_snapshot: API-based raw snapshot persistence (NDJSON + manifest)
- hash_file: single-file SHA-256 hashing
- hash_directory: deterministic directory hashing
"""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
from pathlib import Path

import pytest

from coclab.raw_snapshot import (
    hash_directory,
    hash_file,
    hash_zip_contents,
    persist_file_snapshot,
    raw_dir,
    raw_path,
    write_api_snapshot,
)

# ---------------------------------------------------------------------------
# raw_dir / raw_path
# ---------------------------------------------------------------------------


class TestRawDir:
    """Tests for raw_dir path builder."""

    def test_without_variant(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        p = raw_dir("zori", 2024)
        assert p == tmp_path / "data" / "raw" / "zori" / "2024"

    def test_with_variant(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        p = raw_dir("acs5_tract", 2023, "full")
        assert p == tmp_path / "data" / "raw" / "acs5_tract" / "2023" / "full"

    def test_year_as_string(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        p = raw_dir("tiger", "2020", "tracts")
        assert p == tmp_path / "data" / "raw" / "tiger" / "2020" / "tracts"

    def test_custom_raw_root(self, tmp_path: Path):
        p = raw_dir("pep", 2024, raw_root=tmp_path)
        assert p == tmp_path / "pep" / "2024"


class TestRawPath:
    """Tests for raw_path file path builder."""

    def test_without_variant(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        p = raw_path("zori", 2026, "zori__county__2026-02-07.csv")
        assert p == tmp_path / "data" / "raw" / "zori" / "2026" / "zori__county__2026-02-07.csv"

    def test_with_variant(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        p = raw_path("tiger", 2020, "tab20_tract20_tract10_natl.txt", "tract_relationship")
        expected = (
            tmp_path / "data" / "raw" / "tiger" / "2020"
            / "tract_relationship" / "tab20_tract20_tract10_natl.txt"
        )
        assert p == expected

    def test_custom_raw_root(self, tmp_path: Path):
        p = raw_path("pep", 2024, "pep.csv", raw_root=tmp_path)
        assert p == tmp_path / "pep" / "2024" / "pep.csv"


# ---------------------------------------------------------------------------
# persist_file_snapshot
# ---------------------------------------------------------------------------


class TestPersistFileSnapshot:
    """Tests for persist_file_snapshot function."""

    def test_writes_file_to_correct_path(self, tmp_path: Path):
        """File lands at <raw_root>/<source_type>/<filename>."""
        content = b"hello raw data"
        path, _, _ = persist_file_snapshot(
            content, "census", "tract.shp", raw_root=tmp_path,
        )

        assert path == tmp_path / "census" / "tract.shp"
        assert path.exists()
        assert path.read_bytes() == content

    def test_writes_file_with_subdirs(self, tmp_path: Path):
        """File lands at <raw_root>/<source_type>/<subdirs...>/<filename>."""
        content = b"some bytes"
        path, _, _ = persist_file_snapshot(
            content,
            "census",
            "tl_2023_06_tract.csv",
            subdirs=("2023", "tracts"),
            raw_root=tmp_path,
        )

        assert path == tmp_path / "census" / "2023" / "tracts" / "tl_2023_06_tract.csv"
        assert path.exists()
        assert path.read_bytes() == content

    def test_returns_correct_tuple(self, tmp_path: Path):
        """Return value is (path, sha256, size)."""
        content = b"deterministic content"
        path, sha256_hex, size = persist_file_snapshot(
            content, "hud", "data.csv", raw_root=tmp_path,
        )

        assert isinstance(path, Path)
        assert isinstance(sha256_hex, str)
        assert isinstance(size, int)

    def test_sha256_matches_hashlib_non_zip(self, tmp_path: Path):
        """Returned SHA-256 matches hashlib.sha256(content) for non-ZIP files."""
        content = b"verify this hash please"
        _, sha256_hex, _ = persist_file_snapshot(
            content, "census", "test.csv", raw_root=tmp_path,
        )

        expected = hashlib.sha256(content).hexdigest()
        assert sha256_hex == expected

    def test_sha256_uses_zip_content_hash_for_zips(self, tmp_path: Path):
        """Returned SHA-256 for .zip files hashes extracted contents, not container."""
        import io
        import zipfile

        # Create two ZIPs with identical contents but different compression
        def make_zip(compress_type: int) -> bytes:
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", compression=compress_type) as zf:
                zf.writestr("data.txt", "hello world")
            return buf.getvalue()

        zip_stored = make_zip(zipfile.ZIP_STORED)
        zip_deflated = make_zip(zipfile.ZIP_DEFLATED)

        # Raw bytes differ
        assert zip_stored != zip_deflated

        _, hash1, _ = persist_file_snapshot(
            zip_stored, "census", "test.zip", raw_root=tmp_path,
        )
        _, hash2, _ = persist_file_snapshot(
            zip_deflated, "census", "test2.zip", raw_root=tmp_path,
        )

        # Content-stable hashes match
        assert hash1 == hash2

    def test_size_matches_content_length(self, tmp_path: Path):
        """Returned size matches len(content)."""
        content = b"twelve bytes"
        _, _, size = persist_file_snapshot(
            content, "census", "test.csv", raw_root=tmp_path,
        )

        assert size == len(content)

    def test_creates_parent_directories(self, tmp_path: Path):
        """Parent directories are created even when deeply nested."""
        nested_root = tmp_path / "deep" / "nested" / "raw"
        content = b"nested"
        path, _, _ = persist_file_snapshot(
            content, "census", "f.bin", subdirs=("a", "b"), raw_root=nested_root,
        )

        assert path.exists()
        assert path.parent == nested_root / "census" / "a" / "b"

    def test_uses_default_raw_root_when_not_specified(self, tmp_path: Path, monkeypatch):
        """When raw_root is None, resolves from storage config defaults."""
        monkeypatch.chdir(tmp_path)

        content = b"default root"
        path, _, _ = persist_file_snapshot(content, "src", "file.csv")

        assert path == tmp_path / "data" / "raw" / "src" / "file.csv"
        assert path.exists()

    def test_empty_content(self, tmp_path: Path):
        """Empty byte string is persisted correctly."""
        content = b""
        path, sha256_hex, size = persist_file_snapshot(
            content, "census", "empty.csv", raw_root=tmp_path,
        )

        assert path.read_bytes() == b""
        assert size == 0
        assert sha256_hex == hashlib.sha256(b"").hexdigest()


# ---------------------------------------------------------------------------
# write_api_snapshot
# ---------------------------------------------------------------------------


class TestWriteApiSnapshot:
    """Tests for write_api_snapshot function."""

    def _make_payload(self, obj: dict) -> bytes:
        """Encode a dict as JSON bytes (simulating an HTTP response body)."""
        return json.dumps(obj).encode("utf-8")

    def test_creates_response_ndjson(self, tmp_path: Path):
        """response.ndjson is created in the snapshot directory."""
        payloads = [self._make_payload({"a": 1})]
        snap_dir, _, _ = write_api_snapshot(
            payloads, "hud_opendata", snapshot_id="2026-02-07", raw_root=tmp_path,
        )

        ndjson_path = snap_dir / "response.ndjson"
        assert ndjson_path.exists()

    def test_ndjson_deterministic_sorted_keys(self, tmp_path: Path):
        """response.ndjson uses sorted keys for deterministic serialisation."""
        payloads = [self._make_payload({"z": 1, "a": 2, "m": 3})]
        snap_dir, _, _ = write_api_snapshot(
            payloads, "hud_opendata", snapshot_id="snap1", raw_root=tmp_path,
        )

        ndjson_path = snap_dir / "response.ndjson"
        line = ndjson_path.read_text(encoding="utf-8").strip()
        parsed_keys = list(json.loads(line).keys())
        assert parsed_keys == ["a", "m", "z"]

    def test_creates_request_json_when_metadata_provided(self, tmp_path: Path):
        """request.json is created when request_metadata is given."""
        payloads = [self._make_payload({"data": 1})]
        metadata = {"url": "https://example.com/api", "params": {"key": "val"}}

        snap_dir, _, _ = write_api_snapshot(
            payloads,
            "hud_opendata",
            snapshot_id="snap1",
            request_metadata=metadata,
            raw_root=tmp_path,
        )

        request_path = snap_dir / "request.json"
        assert request_path.exists()

        loaded = json.loads(request_path.read_text(encoding="utf-8"))
        assert loaded["url"] == "https://example.com/api"
        assert loaded["params"] == {"key": "val"}

    def test_no_request_json_without_metadata(self, tmp_path: Path):
        """request.json is NOT created when request_metadata is None."""
        payloads = [self._make_payload({"data": 1})]
        snap_dir, _, _ = write_api_snapshot(
            payloads, "hud_opendata", snapshot_id="snap1", raw_root=tmp_path,
        )

        assert not (snap_dir / "request.json").exists()

    def test_creates_manifest_json(self, tmp_path: Path):
        """manifest.json is created with correct fields."""
        payloads = [
            self._make_payload({"row": 1}),
            self._make_payload({"row": 2}),
        ]
        snap_dir, sha256_hex, ndjson_size = write_api_snapshot(
            payloads,
            "hud_opendata",
            snapshot_id="snap1",
            record_count=42,
            raw_root=tmp_path,
        )

        manifest_path = snap_dir / "manifest.json"
        assert manifest_path.exists()

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert manifest["snapshot_id"] == "snap1"
        assert manifest["source_type"] == "hud_opendata"
        assert manifest["page_count"] == 2
        assert manifest["record_count"] == 42
        assert manifest["ndjson_sha256"] == sha256_hex
        assert manifest["ndjson_bytes"] == ndjson_size
        assert "retrieved_at" in manifest

    def test_sha256_computed_from_persisted_ndjson(self, tmp_path: Path):
        """SHA-256 matches hash of the persisted response.ndjson bytes."""
        payloads = [self._make_payload({"x": 1})]
        snap_dir, sha256_hex, _ = write_api_snapshot(
            payloads, "hud_opendata", snapshot_id="snap1", raw_root=tmp_path,
        )

        ndjson_content = (snap_dir / "response.ndjson").read_bytes()
        expected = hashlib.sha256(ndjson_content).hexdigest()
        assert sha256_hex == expected

    def test_returns_correct_tuple(self, tmp_path: Path):
        """Return value is (snap_dir, sha256, ndjson_size)."""
        payloads = [self._make_payload({"v": 1})]
        snap_dir, sha256_hex, ndjson_size = write_api_snapshot(
            payloads, "hud_opendata", snapshot_id="snap1", raw_root=tmp_path,
        )

        assert isinstance(snap_dir, Path)
        assert snap_dir == tmp_path / "hud_opendata" / "snap1"
        assert isinstance(sha256_hex, str)
        assert len(sha256_hex) == 64
        assert isinstance(ndjson_size, int)
        assert ndjson_size > 0

    def test_custom_raw_root(self, tmp_path: Path):
        """Snapshot is written under the custom raw_root."""
        custom_root = tmp_path / "custom" / "raw"
        payloads = [self._make_payload({"ok": True})]
        snap_dir, _, _ = write_api_snapshot(
            payloads, "src", snapshot_id="s1", raw_root=custom_root,
        )

        assert snap_dir == custom_root / "src" / "s1"
        assert snap_dir.exists()

    def test_empty_response_list(self, tmp_path: Path):
        """Empty response list produces empty response.ndjson."""
        snap_dir, sha256_hex, ndjson_size = write_api_snapshot(
            [], "hud_opendata", snapshot_id="empty", raw_root=tmp_path,
        )

        ndjson_path = snap_dir / "response.ndjson"
        assert ndjson_path.exists()
        assert ndjson_path.read_bytes() == b""
        assert ndjson_size == 0
        assert sha256_hex == hashlib.sha256(b"").hexdigest()

        manifest = json.loads((snap_dir / "manifest.json").read_text(encoding="utf-8"))
        assert manifest["page_count"] == 0

    def test_deterministic_same_input_same_hash(self, tmp_path: Path):
        """Same input produces identical hash across two calls."""
        payloads = [self._make_payload({"z": 3, "a": 1})]

        _, hash1, size1 = write_api_snapshot(
            payloads, "src", snapshot_id="run1", raw_root=tmp_path,
        )
        _, hash2, size2 = write_api_snapshot(
            payloads, "src", snapshot_id="run2", raw_root=tmp_path,
        )

        assert hash1 == hash2
        assert size1 == size2

    def test_multiple_pages_produce_multiline_ndjson(self, tmp_path: Path):
        """Multiple payloads produce one NDJSON line each."""
        payloads = [
            self._make_payload({"page": 1}),
            self._make_payload({"page": 2}),
            self._make_payload({"page": 3}),
        ]
        snap_dir, _, _ = write_api_snapshot(
            payloads, "src", snapshot_id="multi", raw_root=tmp_path,
        )

        ndjson_text = (snap_dir / "response.ndjson").read_text(encoding="utf-8")
        # Trailing newline means split produces an extra empty string
        lines = [ln for ln in ndjson_text.split("\n") if ln]
        assert len(lines) == 3

    def test_record_count_none_in_manifest(self, tmp_path: Path):
        """record_count=None is faithfully stored in manifest."""
        payloads = [self._make_payload({"x": 1})]
        snap_dir, _, _ = write_api_snapshot(
            payloads, "src", snapshot_id="s1", raw_root=tmp_path,
        )

        manifest = json.loads((snap_dir / "manifest.json").read_text(encoding="utf-8"))
        assert manifest["record_count"] is None

    # -- year+variant mode --------------------------------------------------

    def test_year_variant_creates_year_first_dir(self, tmp_path: Path):
        """year+variant produces <root>/<source>/<year>/<variant>/."""
        payloads = [self._make_payload({"ok": True})]
        snap_dir, _, _ = write_api_snapshot(
            payloads, "acs5_tract", year=2023, variant="full", raw_root=tmp_path,
        )

        assert snap_dir == tmp_path / "acs5_tract" / "2023" / "full"
        assert (snap_dir / "response.ndjson").exists()

    def test_year_variant_manifest_snapshot_id(self, tmp_path: Path):
        """manifest.snapshot_id reflects year/variant."""
        payloads = [self._make_payload({"v": 1})]
        snap_dir, _, _ = write_api_snapshot(
            payloads, "hud_exchange", year=2025, variant="2026-02-07",
            raw_root=tmp_path,
        )

        manifest = json.loads((snap_dir / "manifest.json").read_text(encoding="utf-8"))
        assert manifest["snapshot_id"] == "2025/2026-02-07"

    def test_raises_when_both_year_and_snapshot_id(self, tmp_path: Path):
        """Cannot provide both year and snapshot_id."""
        with pytest.raises(ValueError, match="not both"):
            write_api_snapshot(
                [self._make_payload({"x": 1})], "src",
                year=2023, variant="full", snapshot_id="legacy",
                raw_root=tmp_path,
            )

    def test_raises_when_neither_year_nor_snapshot_id(self, tmp_path: Path):
        """Must provide either year or snapshot_id."""
        with pytest.raises(ValueError, match="not both"):
            write_api_snapshot(
                [self._make_payload({"x": 1})], "src", raw_root=tmp_path,
            )

    def test_raises_when_year_without_variant(self, tmp_path: Path):
        """year without variant is an error."""
        with pytest.raises(ValueError, match="variant is required"):
            write_api_snapshot(
                [self._make_payload({"x": 1})], "src",
                year=2023, raw_root=tmp_path,
            )


# ---------------------------------------------------------------------------
# hash_zip_contents
# ---------------------------------------------------------------------------


class TestHashZipContents:
    """Tests for hash_zip_contents function."""

    def _make_zip(
        self,
        files: dict[str, bytes],
        compress_type: int = zipfile.ZIP_DEFLATED,
    ) -> bytes:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=compress_type) as zf:
            for name, data in files.items():
                zf.writestr(name, data)
        return buf.getvalue()

    def test_stable_across_compression_types(self):
        """Same files produce the same hash regardless of compression."""
        files = {"a.txt": b"alpha", "b.txt": b"beta"}
        zip_stored = self._make_zip(files, zipfile.ZIP_STORED)
        zip_deflated = self._make_zip(files, zipfile.ZIP_DEFLATED)

        assert zip_stored != zip_deflated
        assert hash_zip_contents(zip_stored) == hash_zip_contents(zip_deflated)

    def test_detects_content_change(self):
        """Different file contents produce different hashes."""
        zip1 = self._make_zip({"a.txt": b"version1"})
        zip2 = self._make_zip({"a.txt": b"version2"})

        assert hash_zip_contents(zip1) != hash_zip_contents(zip2)

    def test_detects_filename_change(self):
        """Renaming a file changes the hash (same content, different name)."""
        zip1 = self._make_zip({"old.txt": b"data"})
        zip2 = self._make_zip({"new.txt": b"data"})

        assert hash_zip_contents(zip1) != hash_zip_contents(zip2)

    def test_ignores_directories(self):
        """Directory entries in the ZIP don't affect the hash."""
        files = {"data.txt": b"hello"}
        zip_flat = self._make_zip(files)

        # Create a ZIP with an explicit directory entry
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("somedir/", "")  # directory entry
            zf.writestr("data.txt", b"hello")
        zip_with_dir = buf.getvalue()

        assert hash_zip_contents(zip_flat) == hash_zip_contents(zip_with_dir)

    def test_sorted_order_is_deterministic(self):
        """File insertion order doesn't matter — entries are sorted."""
        zip1 = self._make_zip({"b.txt": b"B", "a.txt": b"A"})
        # Reverse insertion order
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("a.txt", b"A")
            zf.writestr("b.txt", b"B")
        zip2 = buf.getvalue()

        assert hash_zip_contents(zip1) == hash_zip_contents(zip2)


# ---------------------------------------------------------------------------
# hash_file
# ---------------------------------------------------------------------------


class TestHashFile:
    """Tests for hash_file helper."""

    def test_returns_correct_sha256_and_size(self, tmp_path: Path):
        """SHA-256 and size match independently computed values."""
        content = b"known content for hashing"
        f = tmp_path / "file.bin"
        f.write_bytes(content)

        sha256_hex, size = hash_file(f)

        assert sha256_hex == hashlib.sha256(content).hexdigest()
        assert size == len(content)

    def test_empty_file(self, tmp_path: Path):
        """Empty file produces the empty-bytes SHA-256."""
        f = tmp_path / "empty.bin"
        f.write_bytes(b"")

        sha256_hex, size = hash_file(f)

        assert sha256_hex == hashlib.sha256(b"").hexdigest()
        assert size == 0

    def test_hash_is_lowercase_hex(self, tmp_path: Path):
        """Returned hash is lowercase hexadecimal, 64 chars."""
        f = tmp_path / "f.bin"
        f.write_bytes(b"abc")

        sha256_hex, _ = hash_file(f)

        assert len(sha256_hex) == 64
        assert sha256_hex == sha256_hex.lower()
        assert all(c in "0123456789abcdef" for c in sha256_hex)

    def test_raises_for_missing_file(self, tmp_path: Path):
        """FileNotFoundError when file does not exist."""
        with pytest.raises(FileNotFoundError):
            hash_file(tmp_path / "nonexistent.bin")


# ---------------------------------------------------------------------------
# hash_directory
# ---------------------------------------------------------------------------


class TestHashDirectory:
    """Tests for hash_directory helper."""

    def test_hashes_files_in_sorted_order(self, tmp_path: Path):
        """Combined hash equals hashing file contents in sorted name order."""
        (tmp_path / "b.txt").write_bytes(b"beta")
        (tmp_path / "a.txt").write_bytes(b"alpha")

        combined_hex, total_size = hash_directory(tmp_path)

        # Manually reproduce: sorted order is a.txt, b.txt
        hasher = hashlib.sha256()
        hasher.update(b"alpha")
        hasher.update(b"beta")
        expected = hasher.hexdigest()

        assert combined_hex == expected
        assert total_size == len(b"alpha") + len(b"beta")

    def test_deterministic_across_calls(self, tmp_path: Path):
        """Same directory contents produce same hash on repeated calls."""
        (tmp_path / "x.txt").write_bytes(b"x-data")
        (tmp_path / "y.txt").write_bytes(b"y-data")

        hash1, size1 = hash_directory(tmp_path)
        hash2, size2 = hash_directory(tmp_path)

        assert hash1 == hash2
        assert size1 == size2

    def test_includes_files_in_subdirectories(self, tmp_path: Path):
        """Files in subdirectories are included (rglob)."""
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "nested.txt").write_bytes(b"nested content")
        (tmp_path / "top.txt").write_bytes(b"top content")

        _, total_size = hash_directory(tmp_path)

        assert total_size == len(b"nested content") + len(b"top content")

    def test_empty_directory(self, tmp_path: Path):
        """Empty directory produces the initial SHA-256 (no updates)."""
        empty = tmp_path / "empty_dir"
        empty.mkdir()

        combined_hex, total_size = hash_directory(empty)

        assert combined_hex == hashlib.sha256().hexdigest()
        assert total_size == 0

    def test_order_matters_for_hash(self, tmp_path: Path):
        """Renaming files changes the hash because sort order changes."""
        dir1 = tmp_path / "dir1"
        dir1.mkdir()
        (dir1 / "a.txt").write_bytes(b"first")
        (dir1 / "b.txt").write_bytes(b"second")

        dir2 = tmp_path / "dir2"
        dir2.mkdir()
        # Same content but swapped names
        (dir2 / "a.txt").write_bytes(b"second")
        (dir2 / "b.txt").write_bytes(b"first")

        hash1, _ = hash_directory(dir1)
        hash2, _ = hash_directory(dir2)

        assert hash1 != hash2
