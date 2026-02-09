# Appendix

## CoC ID Format

CoC identifiers follow the pattern: `{STATE}-{NUMBER}`

- `STATE` - Two-letter state abbreviation
- `NUMBER` - Three-digit number (zero-padded)

Examples: `CO-500`, `NY-600`, `CA-500`

**Cross-State CoCs:**

Some CoCs span multiple states. In HUD PIT data files, these may appear with a letter suffix (e.g., `MO-604a`) indicating combined territory data. CoC Lab normalizes these to the canonical format:

| Raw ID | Normalized | Notes |
|--------|------------|-------|
| `MO-604a` | `MO-604` | Kansas City metro (MO + KS) |

The original ID and mapping are preserved in Parquet provenance metadata for traceability.

## Coordinate Reference System

All geometries are stored in **EPSG:4326** (WGS84):
- Latitude: -90 to 90
- Longitude: -180 to 180

## Geometry Hash Algorithm

1. Extract WKB from geometry
2. Round coordinates to 6 decimal places (~11cm precision)
3. Compute SHA-256 hash
4. Store as hex string

This enables efficient change detection between vintages.

## Temporal Concepts Reference

For comprehensive documentation of vintage notation, temporal alignment, and mismatch terminology, see [[08-Temporal-Terminology|Temporal Terminology]].

---

**Previous:** [[15-Development]] | **Back to:** [[CoC-Lab-Manual]]

---

*Generated for CoC Lab v0*
