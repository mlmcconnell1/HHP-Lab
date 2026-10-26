# Right-to-shelter mixed-geography weights

Research note for `coclab-e0ezj`, prepared 2026-10-26. This note evaluates
weights for describing how much of an MSA or CoC lies in jurisdictions with
each coded legal status. It does not estimate which residents meet legal
eligibility rules.

## Scope and when weights are needed

The linked shelter-capacity recipe has a 2010–2024 analysis window and builds
both CoC and metro panels (`recipes/examples/shelter-capacity-results.yaml`).
Right-to-shelter outputs are generated for an explicitly requested range and
geography membership/boundary vintage. Until the reviewed baseline and dated
exceptions are complete, the actual mixed-status geography-years cannot be
enumerated: doing so now would confuse unreviewed/unknown jurisdiction status
with a known absence of a right.

For a requested geography-year, if every member county has the same coded
status, the flag and its covered share are exactly 0 or 1. That row needs no
population source. Weights are needed only for mixed-status geography-years,
and then only if a fractional covered share or a population-weighted
classification is requested. Validate complete county-by-year support over
the counties used in each such denominator; never drop missing counties and
renormalize silently.

## Native sources found

The package registry and local curated files provide these options:

| Source | Package support and local coverage | What it can support | Limits for this use |
|---|---|---|---|
| Census PEP county population | Native `hhplab ingest pep` pipeline. Local curated files include `pep_county__v2020.parquet` (2010–2020), `pep_county__v2024.parquet` (2020–2024), and `pep_county__v2025__y2020-2025.parquet`. PEP values are county resident-population estimates referenced to July 1. | A common county resident-population weight for all three legal flags; estimates geographic coverage among residents. | Not counts of legally eligible single adults, families, or currently pregnant people. Published vintage series can be revised; preserve the exact file/content identity and year/vintage selection. July 1 is not the January PIT reference date. |
| ACS5 total population | `ACS5_COVARIATE_REGISTRY`: B01003 `total_population`; tract-derived output. Existing county aggregation can use the package ACS5 pipeline and tract/county membership. | A common total-resident-population geography weight, with ACS five-year survey estimates. | Still not eligibility counts. ACS5 vintages overlap in years; choose the ACS vintage explicitly. It is less temporally specific than annual PEP. |
| ACS5 adult population | Registry: B01001 `adult_population`, derived from age cells; tract-derived output. | Could describe the share of adults geographically covered by a legal status if clearly labeled. | Adults are not synonymous with single adults eligible for a particular shelter duty; does not measure household or residency rules. Not a defensible single-adult eligibility denominator. |
| ACS household composition / fertility | B11001 family/nonfamily-household counts and B13016 women who had a birth in the previous 12 months exist in Census ACS tables, but neither is in the current native package registry. | Could motivate later separately designed measures. | B11001 counts households, not people eligible under a family-with-children rule; converting household counts to residents requires additional assumptions and still misses household eligibility details. B13016 measures recent births, not current pregnancy, and is not a pregnancy-population denominator. Neither should be introduced as an implicit proxy. |

`hhplab list acs-variables --json` confirms the ACS5 registry currently exposes
total population and adult population for this purpose but no B11001 household
type or B13016 fertility measure. The ACS1 county registry does not expose
B01001, B11001, or B13016. Do not infer support from tables available at the
Census API; a new measure would need registry and pipeline support first.

PEP is the only currently native annual county-level source among these
candidates. Existing files cover the linked 2010–2024 panel, but are split
across vintages at 2020. If using PEP, record that break explicitly (v2020 for
2010–2019; one selected series for 2020–2024), or choose and ingest a reviewed
consistent vintage/source plan. PEP's July 1 reference and the RTS annual
January 31 reference are not identical; disclose the timing approximation.
The local v2020 file has 3,143 county FIPS per year; v2024 and v2025 have
3,144. A build must validate the exact county-FIPS support required by each
mixed geography-year against the selected PEP vintage and its corresponding
membership vintage, rather than presuming the annual county universe is
identical across vintages.

## County-FIPS reconciliation is required

The local TIGER county files confirm that the 2020 and 2023 county geometries
do not use the same Connecticut IDs:

| Geometry | Connecticut county-equivalent FIPS |
|---|---|
| C2020 | Legacy counties `09001`, `09003`, `09005`, `09007`, `09009`, `09011`, `09013`, `09015` |
| C2023 | Planning regions `09110`, `09120`, `09130`, `09140`, `09150`, `09160`, `09170`, `09180`, `09190` |

These are the exact FIPS set differences between the local geometries. C2020
also includes `51515`, which C2023 does not. The local `census_msa_2010`
membership has one `51515` row, and none of the local PEP county vintages has
that FIPS. A build using that historical MSA definition therefore cannot
claim complete county support unless it introduces and documents a valid
historical concordance; otherwise it must fail for mixed rows that require
that county.

The PEP transition matches the Connecticut geometry transition: v2020 has the
eight legacy county IDs through 2020; v2024/v2025 have the nine planning
region IDs from 2020 onward. `load_pep_county()` resolves duplicate
county/year keys to the latest vintage, but cannot deduplicate legacy and
planning-region IDs because they are different keys. In the current local
directory it returns both eight legacy and nine planning-region CT rows for
2020; the legacy eight remain for 2010–2019. For a C2023 / 2023 MSA output,
the reviewed bridge therefore needs to:

1. For 2010–2019, allocate each legacy-county population estimate among the
   nine planning regions using the existing CT geometry overlay and the
   crosswalk's `legacy_share` (intersection area / legacy-county area).
2. For 2020–2024, use the selected latest-vintage planning-region estimates
   and exclude the legacy 2020 rows so Connecticut is not double counted.
3. Compare the resulting five-digit county FIPS against the full required
   county membership for every year; reject missing or duplicate support.

The package provides
`hhplab.geographies.coc.ct_planning_regions.build_ct_county_planning_region_crosswalk(2020, 2023)`.
For counts being allocated from legacy source county to planning-region target,
use `mapping.legacy_share`; the existing ZORI legacy-to-planning helper uses
`planning_share` to redistribute an intensive value and is not the right
weight for population counts. This population redistribution assumes uniform
residence density within each legacy county. Preserve and propagate that
approximation alongside the CoC county-fragment area-allocation caveat.

After that CT conversion, a read-only coverage audit of every county in the
local `census_msa_2023` membership found **66 unmatched FIPS in every year
2010–2024**. All 66 begin with state FIPS `72` (Puerto Rico municipios); none
of the locally curated PEP county files contains them. The 2023 MSA membership
has 1,252 distinct county FIPS; the selected PEP support after CT conversion
covers 1,186 of them. Puerto Rico is a single jurisdiction for this coding, so
these rows should be homogeneous once the reviewed legal baseline includes
Puerto Rico. In that case no PEP weights are required. If local/substate
status makes any Puerto Rico MSA mixed, weights cannot be supplied by the
current PEP county input and the build must fail rather than impute.

An executable read-only completeness check for MSA county membership is:

```python
from hhplab.geographies.msa.msa_io import read_msa_county_membership
from hhplab.sources.census.pep.pep_aggregate import load_pep_county

pep = load_pep_county()  # newest vintage wins only for identical FIPS/year keys
for definition in ("census_msa_2010", "census_msa_2023"):
    member = read_msa_county_membership(definition)
    required = set(member["county_fips"].astype(str).str.zfill(5))
    for year in range(2010, 2025):
        available = set(
            pep.loc[pep["year"].eq(year), "county_fips"].astype(str).str.zfill(5)
        )
        missing = sorted(required - available)
        if missing:
            print(definition, year, missing)
```

The full-membership check above is an availability audit, not the operative
denominator rule. Once the legal table is built, use only the county-year
pairs belonging to mixed-status MSA/CoC rows whose requested shares or
classification need weights. First restrict county membership to those mixed
geographies; then run the same set-difference validation against the complete
county-year weight input. For C2023 CoC/membership, do the Connecticut
conversion above before the check. The 2010 MSA definition's `51515` mismatch
is a structural support gap, not an estimate of zero.

## Native PEP loading and content identity

The reusable read API is
`hhplab.sources.census.pep.pep_aggregate.load_pep_county(pep_dir=...)`. Its
result retains row-level `vintage` and `raw_sha256`. Curated parquet provenance
is available through
`hhplab.storage.provenance.read_provenance(path).extra`, including PEP
`dataset`, `vintage`, `download_url`, `raw_sha256`, row count and year range.
For reproducibility, provenance should enumerate the exact selected curated
parquet paths and embedded raw SHA-256 values, and the county conversion plan.
If identity must pin the curated Parquet bytes rather than the Census download,
also hash the selected Parquet file bytes; an embedded raw-file hash alone does
not identify a later rewrite of the normalized artifact.

## Recommendation for review

If fractional mixed-geography shares are needed, the simplest presently
supportable option is one common **resident population** weight from county
PEP, used for each of the three legal flags. Name the resulting quantities
“resident-population geographic coverage shares” (or equally explicit wording).
They answer: “What share of residents in this mixed MSA/CoC live in counties
assigned to jurisdictions with this legal status?” They do not answer: “What
share of legally eligible people are covered?”

This is an explicit methodological simplification and requires Matt's review;
it is not approved by this research note. If it is not approved, keep the
flags and exact shares for homogeneous geography-years, and leave mixed-status
shares/classifications unbuilt or fail with a direct diagnostic until another
definition is reviewed. Do not silently substitute adult, family, pregnancy,
migrant, or HIC bed-count weights. The package's `any` geography rule can
produce a Boolean classification without weights, but must not be presented as
a quantitative covered-share estimate.

If Matt accepts the common PEP basis, the current implementation input schema
(`county_fips`, `year`, `population_single_adults`, `population_families`,
`population_pregnant_people`) could repeat one PEP value three times, but that
shape obscures the shared simplification. Prefer a single `resident_population`
count per county-year plus explicit weight-definition provenance, expanded to
the three share calculations in code. If retaining the current three-column
schema for compatibility with the in-flight source implementation, require
the three columns to be equal under this mode and record that all use the same
PEP resident-population measure.

For any PEP-based output, provenance should record: PEP series/vintage and
input parquet identity/hash; included years and vintage-break rule; July 1
source reference versus January 31 RTS coding date; resident-population
universe; common-weight simplification; county-FIPS support validation; and
the CoC county-area allocation approximation wherever a CoC only partly
intersects a county. Keep legal status fields independent of weight values.

## Concrete commands and current implementation boundary

The package's native PEP command can source missing vintages when needed:

```sh
uv run hhplab ingest pep --series postcensal --vintage 2020 --start 2010 --end 2020
uv run hhplab ingest pep --series postcensal --vintage 2024 --start 2020 --end 2024
```

Existing local files mean no external fetch is necessary to assess or prototype
the candidate coverage. The reworked `hhplab build right-to-shelter` interface
accepts optional county-population CSV or Parquet inputs with `county_fips`,
`year`, and `resident_population`, or the three population-specific weight
columns. Supplying weights also requires an explicit `--weight-definition`
describing the basis and vintage. Homogeneous geography-years require no
population rows even when weights are supplied for other, mixed geography-years.
Native PEP outputs name their count `population`; adopting them requires an
explicit resident-population mapping and vintage policy rather than silently
treating them as population-specific legal-eligibility counts. Do not run the
build against a placeholder or incomplete mixed-county input. Use of a common
PEP basis for real outputs remains pending Matt's methodological decision.

The requested 2010–2024 CoC/metro outputs should be rebuilt after the
baseline/exception coverage has been reviewed. At that point report the actual
mixed-status rows (geography ID, year, affected legal flag, member counties)
along with exact homogeneous row counts. Do not treat the candidate NYC,
rest-of-NY, Massachusetts, and DC exceptions as a complete national
classification.

## Choices for Matt

1. Approve or reject a common PEP resident-population weight for mixed
   geography-years, explicitly labeling shares as geographic resident
   coverage rather than legal-eligibility coverage.
2. If approving it, choose one source vintage policy across 2010–2024 (including
   treatment of the overlapping 2020 estimates) and confirm whether July 1 PEP
   values are acceptable for January 31 status-year outputs.
3. Confirm the mixed-geography classification rule/threshold independently of
   this weight definition. The weight can supply a share, but the threshold
   changes the flag and remains a research choice.

Without these decisions, independently executable work is limited to
homogeneous exact 0/1 outputs, complete-support validation design, and
recording provenance/caveat requirements. No exact population-specific legal
eligibility weights are currently supported.

## Primary source references

- [Census Bureau PEP FAQ](https://www.census.gov/programs-surveys/popest/about/faq.html): county estimates use July 1 reference dates and are revised in later vintages.
- [Census Bureau ACS5 B11001 table](https://api.census.gov/data/2022/acs/acs5/groups/B11001.html): household-type table, with households as the universe.
- [Census Bureau ACS5 B13016 table](https://api.census.gov/data/2020/acs/acs5/groups/B13016.html): women aged 15–50 who had a birth in the past 12 months.
- [Census Bureau ACS B01001 table](https://api.census.gov/data/2024/acs/acs1/groups/B01001.html): sex-by-age detail, including age categories from which adult counts can be derived.
