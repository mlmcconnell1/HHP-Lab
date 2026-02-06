# Bundle Layout and MANIFEST.json

The `coclab build export` command creates self-contained, analysis-ready bundles for downstream repositories. This section documents the bundle structure and manifest schema.

## Bundle Directory Structure

```
export-{n}/
  MANIFEST.json
  README.md
  data/
    panels/
    inputs/
      boundaries/
      xwalks/
      pit/
      rents/
      acs/
  diagnostics/
  codebook/
    schema.md
    variables.csv
```

**Core Files:**

| Path | Description |
|------|-------------|
| `MANIFEST.json` | Machine-readable manifest with file hashes and provenance |
| `README.md` | Human-readable description of bundle contents and usage |
| `data/panels/` | Primary analysis panel parquet file(s) |

**Optional Directories (controlled by `--include`):**

| Path | Include Flag | Description |
|------|--------------|-------------|
| `data/inputs/boundaries/` | `inputs` | CoC boundary GeoParquet files |
| `data/inputs/xwalks/` | `inputs` | CoC-tract and CoC-county crosswalks |
| `data/inputs/pit/` | `inputs` | PIT count parquet files |
| `data/inputs/rents/` | `inputs` | ZORI rent data |
| `data/inputs/acs/` | `inputs` | ACS demographic measures |
| `diagnostics/` | `diagnostics` | Diagnostic outputs and quality reports |
| `codebook/` | `codebook` | Variable documentation |

## MANIFEST.json Schema

The manifest provides machine-readable provenance and file integrity information:

```json
{
  "bundle_name": "gbc_replication",
  "export_id": "export-7",
  "created_at_utc": "2026-01-07T21:15:03Z",
  "coclab": {
    "version": "0.9.3",
    "git_commit": "abc1234",
    "python": "3.11.6"
  },
  "parameters": {
    "boundary_vintage": "2025",
    "tract_vintage": "2023",
    "county_vintage": "2023",
    "acs_vintage": "2019-2023",
    "years": "2011-2024",
    "copy_mode": "copy"
  },
  "artifacts": [...],
  "sources": [...],
  "notes": ""
}
```

**Top-Level Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `bundle_name` | string | Logical name provided via `--name` |
| `export_id` | string | Folder name (e.g., `export-7`) |
| `created_at_utc` | string | ISO 8601 timestamp |
| `coclab.version` | string | CoC Lab package version |
| `coclab.git_commit` | string | Git commit hash (if available) |
| `coclab.python` | string | Python version |
| `parameters` | object | Export configuration parameters |
| `artifacts` | array | List of included files with metadata |
| `sources` | array | External data source attributions |
| `notes` | string | Optional user-provided notes |

**Artifact Entry Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `role` | string | `panel`, `input`, `derived`, `diagnostic`, or `codebook` |
| `path` | string | Relative path within bundle |
| `sha256` | string | SHA-256 hash of file contents |
| `bytes` | integer | File size in bytes |
| `rows` | integer | Row count (for parquet/csv files) |
| `columns` | integer | Column count |
| `key_columns` | array | List of important column names |
| `provenance` | object | Optional provenance metadata from source file |

> **Implementation Notes:**
> - `sha256` is computed on the **copied bundle file bytes**, not the source file. This ensures the hash matches what downstream consumers will read from the bundle, regardless of copy mode (copy, hardlink, or symlink).
> - `path` values are **bundle-relative** (e.g., `data/panels/coc_panel__2018_2024.parquet`), not absolute paths or paths relative to the source repository. All artifact paths should be resolvable from the bundle root directory.

## Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `2` | Validation failure (missing panel, incompatible vintages, unreadable files) |
| `3` | Filesystem failure (cannot create export directory, copy failure) |
| `4` | Manifest failure (hashing/metadata extraction failure) |

## Using Bundles in Analysis Repositories

Export bundles are designed for integration with version-controlled analysis repositories. Recommended workflow with DVC:

```bash
# In your analysis repository
dvc init
dvc add data/bundles/export-3
git add data/bundles/export-3.dvc .gitignore
git commit -m "Pin CoC Lab export bundle export-3"
```

All analysis code should reference paths within the bundle. The `MANIFEST.json` file pins exact file hashes for reproducibility.

---

**Previous:** [[11-Methodology-Panel-Assembly]] | **Next:** [[13-Module-Reference]]
