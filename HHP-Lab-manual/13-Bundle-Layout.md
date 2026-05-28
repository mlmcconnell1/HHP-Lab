# Bundle Layout and MANIFEST.json

`hhplab build recipe-export` creates export bundles from recipe outputs for downstream analysis repositories.

## Command Contract

```bash
hhplab build recipe-export --manifest <manifest_path> --destination /tmp/bundle
```

The recipe manifest sidecar (`.manifest.json`) tracks all consumed assets,
enabling reproducible exports.

Current bundle layout:

- `manifest.json`: exported recipe manifest
- `asset_store/`: copied reusable assets from `asset_store_root`
- `output/`: copied downstream outputs referenced from `output_root`
- `assets/`: legacy project-relative assets from older manifests

Recipe-native MSA-CoC coverage outputs are exported like other recipe outputs:
the Parquet artifact referenced by `msa_coc_coverage_path` is copied under
`output/`, and its sidecar manifest records consumed CoC, county, MSA
membership, ranking population, tract geometry, and ACS5 denominator assets.
When the recipe requested `csv_sidecar: true`, the CSV is an adjacent
convenience file; the Parquet file and `.manifest.json` remain the authoritative
artifact and lineage record.

`recipe-export` also accepts `--asset-store-root` and `--output-root` so
root-aware manifests can be exported correctly when the local storage layout is
not using the built-in defaults.

---

**Previous:** [[12-Methodology-Panel-Assembly]] | **Next:** [[14-Module-Reference]]
