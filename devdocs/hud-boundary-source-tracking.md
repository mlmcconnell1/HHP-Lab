# HUD Boundary Source Tracking

## 2019 Source-Change Warning Investigation

During the top-100 MSA/CoC coverage build, ingesting HUD Exchange CoC
boundaries for `boundary_vintage=2019` emitted an upstream source-change warning
against the current HUD ArcGIS FeatureServer URL. The ingest succeeded, but the
registry comparison was not meaningful for a historical vintage.

Finding:

- The default HUD Exchange ingest path used the current ArcGIS FeatureServer for
  any requested vintage when no explicit URL was supplied.
- The ArcGIS FeatureServer is a current boundary layer, not a historical
  year-specific source.
- Source registry change detection is keyed by `source_type` and `source_url`.
  Because the ArcGIS URL was the same for all requested vintages, a 2019 ingest
  could be compared against a previous current-year ingest.
- The raw snapshot path was not the root cause; snapshots used timestamped run
  ids and the 2019 run had a distinct raw directory.

Resolution:

- Four-digit historical vintages before 2024 now use the legacy HUD
  vintage-specific download path by default.
- Current vintages from 2024 onward continue to use the ArcGIS FeatureServer by
  default.
- Non-year tags still use ArcGIS unless the caller supplies `--use-legacy-source`
  or an explicit URL.

Operational guidance:

- Use the default ingest path for current boundaries.
- Use explicit legacy source options when reproducing older boundary vintages.
- Treat a source-change warning for a vintage-specific URL as meaningful.
  Treat cross-vintage comparisons against a shared current-layer URL as a
  source-selection bug, not evidence that historical HUD boundaries changed.
