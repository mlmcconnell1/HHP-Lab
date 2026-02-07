# Legacy Specs (Consolidated)

This file consolidates older design specs that were previously split across:

- `coclab_zori_spec.md`
- `coclab_pep_county_spec.md`
- `coclab_export_bundle_spec.md`

Those standalone files were removed to avoid drift and duplication.

## Current Command Mapping

- ZORI ingest: `coclab ingest zori`
- ZORI aggregation (current code): `coclab build zori`
- PEP ingest: `coclab ingest pep`
- PEP aggregation (current code): `coclab build pep`
- Export bundle (current code): `coclab build export`

Deprecated passthrough forms exist for compatibility, but docs should use the grouped forms above.

## Build/Aggregate Spec (Implemented)

The spec in `coclab_build_aggregate_spec.md` is now implemented. The preferred commands are:

- `coclab build create --name <build> --years <spec>` (creates scaffold, pins base assets, writes manifest)
- `coclab aggregate acs --build <build> [--align ...]`
- `coclab aggregate zori --build <build> [--align ...]`
- `coclab aggregate pep --build <build> [--align ...]`
- `coclab aggregate pit --build <build> [--align ...]`

The old `coclab build measures/zori/pep` commands remain as deprecated passthroughs.

## Dataset-Level Notes Preserved From Legacy Specs

- ZORI: keep monthly-native outputs distinct from PIT-aligned yearly collapse modes.
- PEP: preserve explicit timing semantics (PEP is as-of July 1).
- Export: keep bundle manifests hash-pinned and reproducible.

Detailed operational behavior belongs in the manual and code, not this legacy summary.
