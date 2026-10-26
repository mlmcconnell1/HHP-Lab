# Right-to-shelter mixed-geography weights

Research note for `coclab-e0ezj`, prepared 2026-10-26. This note evaluates
weights for describing how much of an MSA or CoC lies in jurisdictions with
each coded legal status. It does not estimate which residents meet legal
eligibility rules.

Matt approved the recommendations on 2026-10-26: use common county resident
population for all three legal populations, PEP v2020 for 2010–2020 and v2024
for 2021–2024, accepting the July 1 versus January 31 timing approximation.
The main exclusion uses any qualifying year-round geographic coverage in any
of the three legal populations (`coding_rule=broad`, weather excluded,
`geography_rule=any`). Retain separate population flags and covered shares.
Majority coverage (share >= 0.5) and inclusion of weather-only protections
are separate sensitivity checks. These choices govern the intended research
run; reusable CLI/API defaults are unchanged, so builds must pass them
explicitly.

## Scope and when weights are needed

The linked shelter-capacity recipe has a 2010–2024 analysis window and builds
both CoC and metro panels (`recipes/examples/shelter-capacity-results.yaml`).
Right-to-shelter outputs are generated for an explicitly requested range and
geography membership/boundary vintage. Until the reviewed baseline and dated
exceptions are complete, the full set of mixed-status geography-years cannot
be enumerated: Matt has accepted the versioned provisional none-elsewhere
baseline, but unresolved listed-jurisdiction gaps remain unknown rather than
known absences of rights.

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
candidates. The approved source selection is v2020 for
2010–2020 and v2024 for 2021–2024. This deliberately uses v2020 for the 2020
overlap; the frozen Census MSA/CoC population-overlap geography rule is a
separate decision and says nothing about which PEP vintage to choose. Preserve
the approved source selection and exact identities. PEP's July 1 reference
and the RTS annual Jan. 31 reference are not identical, so disclose the timing
approximation. The local v2020 file has 3,143 county FIPS per year; v2024 and
v2025 have 3,144. A build must validate exact county-FIPS support for each
mixed geography-year against the selected PEP vintage and matching membership
vintage, rather than assuming the county universe is identical across
vintages.

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
2020; the legacy eight remain for 2010–2019. Under the approved
selection (v2020 through 2020, v2024 for 2021–2024), a C2023 / 2023 MSA output
would need to:

1. For 2010–2020, allocate each legacy-county population estimate among the
   nine planning regions using the existing CT geometry overlay and the
   crosswalk's `legacy_share` (intersection area / legacy-county area).
2. For 2021–2024, use the selected v2024 planning-region estimates. If the
   overlap-year source choice later uses the v2024 estimate for 2020 instead,
   use those planning-region rows and exclude the v2020 legacy rows in that
   year; never include both representations in one denominator.
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
PEP basis for real outputs was approved by Matt on 2026-10-26.

The requested 2010–2024 CoC/metro outputs should be rebuilt after the
baseline/exception coverage has been reviewed. At that point report the actual
mixed-status rows (geography ID, year, affected legal flag, member counties)
along with exact homogeneous row counts. Do not treat the candidate NYC,
rest-of-NY, Massachusetts, and DC exceptions as a complete national
classification.

## Approved research choices

The common resident basis, explicit vintage partition and timing approximation
are approved. The main exclusion uses any year-round coverage in any legal
population. Majority coverage and weather inclusion remain sensitivity checks,
as recorded at the beginning of this note. No exact population-specific legal
eligibility weights are currently supported or inferred from this decision.

## Primary source references

- [Census Bureau PEP FAQ](https://www.census.gov/programs-surveys/popest/about/faq.html): county estimates use July 1 reference dates and are revised in later vintages.
- [Census Bureau ACS5 B11001 table](https://api.census.gov/data/2022/acs/acs5/groups/B11001.html): household-type table, with households as the universe.
- [Census Bureau ACS5 B13016 table](https://api.census.gov/data/2020/acs/acs5/groups/B13016.html): women aged 15–50 who had a birth in the past 12 months.
- [Census Bureau ACS B01001 table](https://api.census.gov/data/2024/acs/acs1/groups/B01001.html): sex-by-age detail, including age categories from which adult counts can be derived.

## Follow-up audit and normalization plan (2026-10-26)

The current local curated PEP directory contains v2020, v2024, and v2025
files. `load_pep_county()` discovers all vintage files, sorts them newest-first,
and lets the newest file win for duplicate county/year keys. A read-only check
therefore found v2025 selected for 2020–2024 by default. The candidate policy
below explicitly selects v2020 through 2020 and v2024 after 2020, as approved
by the user. The reproducible normalization names the chosen two
files and partitions years explicitly, so later default discovery cannot
silently change this artifact.

The targeted Census MSA support audit used the plausible mixed-status
areas raised in the current research: New York-Newark-Jersey City (`35620`),
Minneapolis-St. Paul-Bloomington (`33460`), Boston (`14460`), Providence-Warwick
(`39300`), and Washington-Arlington-Alexandria (`47900`). All county FIPS in
those five MSA memberships have records in the proposed v2020/v2024 inputs for
each applicable study year. Their county sets include respectively 22, 15, 7,
6, and 23 counties. The exact local input is
`data/curated/msa/msa_county_membership__census_msa_2023.parquet` (SHA-256
`08d4614b045460d72840de9319e4f224ebff72795307d075f403e31e47f03578`). The
five county lists are recorded below so the support audit can be reproduced.
The New York MSA membership does not include Connecticut; Hennepin's MSA, the
two identified Massachusetts-border MSAs, and the DC MSA also have complete
PEP county-row presence. This establishes row availability, not that any
specific status/coding choice makes a given geography-year mixed, nor that the
values form a valid positive denominator.

| MSA ID | County FIPS in audited membership | County/year rows present under v2020 (2010–2020) and v2024 (2021–2024) |
|---|---|---:|
| 35620 | `34003, 34013, 34017, 34019, 34023, 34025, 34027, 34029, 34031, 34035, 34037, 34039, 36005, 36047, 36059, 36061, 36079, 36081, 36085, 36087, 36103, 36119` | 22/22 each year |
| 33460 | `27003, 27019, 27025, 27037, 27053, 27059, 27079, 27095, 27123, 27139, 27141, 27163, 27171, 55093, 55109` | 15/15 each year |
| 14460 | `25009, 25017, 25021, 25023, 25025, 33015, 33017` | 7/7 each year |
| 39300 | `25005, 44001, 44003, 44005, 44007, 44009` | 6/6 each year |
| 47900 | `11001, 24017, 24021, 24031, 24033, 51013, 51043, 51047, 51059, 51061, 51107, 51153, 51157, 51177, 51179, 51187, 51510, 51600, 51610, 51630, 51683, 51685, 54037` | 23/23 each year |

For contrast, a full-membership audit remains broader than the likely mixed
denominators: the 2023 MSA membership includes seven Connecticut planning-region
FIPS that are absent from v2020 in 2010–2020 and 66 Puerto Rico municipios
absent from all local PEP vintages. A historical 2010 MSA definition also
contains FIPS `51515`, absent from PEP. These are not missing resident counts
to replace with zero. They require a relevant county concordance, a known
homogeneous status that avoids weights, or a fail-closed diagnostic if such a
county enters a mixed denominator. Puerto Rico's provisional none-elsewhere
status is a research assumption; it does not make a weight value available.
The targeted MSA audit does not certify CoC county-fragment mappings. CoC
shares use the existing intersection-area-over-county-area approximation and
need a provenance caveat wherever a partial county intersection is used.

The approved canonical normalized input is:

```text
data/curated/right_to_shelter/right_to_shelter__county_weights__Y2010-2024__pep_v2020_v2024.parquet
```

It contains exactly `county_fips` (five-character string), `year`
(integer), and `resident_population` (nonnegative Census resident count), with
one unique row per county/year. Its provenance identifies the population
universe as all residents, the approved selection rule (v2020 for 2010–2020;
v2024 for 2021–2024), the July 1 PEP reference versus Jan. 31 RTS coding date,
both curated source paths and their file-byte SHA-256 values, and the raw
Census SHA-256 values retained in the native PEP `raw_sha256` column. The
existing local files have no embedded provenance block, so do not assume
`read_provenance()` returns one. The normalized provenance also
states that this is a common geographic population denominator, not an
eligibility estimate. The later MSA/CoC mapping provenance should separately
record county-FIPS completeness for the actual mixed cells and the CoC area
allocation approximation. The curated-layout allowlist now recognizes this
canonical filename pattern.

The existing native reader is
`hhplab.sources.census.pep.pep_aggregate.load_pep_county(pep_path=...)`, which
reads one curated PEP artifact and retains its vintage/raw identity columns.
The normalization below calls that reader for two explicit curated
paths. It does not use default discovery or fetch source data. It validates
integral years, five-digit string FIPS, unique county/year keys, and finite,
nonnegative integral populations before writing provenance. The selected
normalized rows must still be checked against the actual mixed county/year
denominator pairs before mapping. Complete row presence alone is not enough:
each mixed denominator after any county allocation must be positive. The
mapping code rejects missing or nonpositive mixed denominators.

```python
from hashlib import sha256
from pathlib import Path

import numpy as np
import pandas as pd

from hhplab.sources.census.pep.pep_aggregate import load_pep_county
from hhplab.storage.provenance import (
    ProvenanceBlock,
    write_parquet_with_provenance,
)

source_specs = (
    (Path("data/curated/pep/pep_county__v2020.parquet"), 2010, 2020),
    (Path("data/curated/pep/pep_county__v2024.parquet"), 2021, 2024),
)
pieces = []
source_identities = []
for path, first_year, last_year in source_specs:
    pep = load_pep_county(pep_path=path)
    years = pd.to_numeric(pep["year"], errors="coerce")
    if years.isna().any() or not years.eq(years.round()).all():
        raise ValueError(f"Non-integral PEP year in {path}")
    pep["year"] = years.astype("int64")

    fips = pep["county_fips"].astype("string")
    if fips.isna().any() or not fips.str.fullmatch(r"[0-9]{5}").all():
        raise ValueError(f"County FIPS must be five-digit strings in {path}")
    pep["county_fips"] = fips

    population = pd.to_numeric(pep["population"], errors="coerce")
    if (population.isna().any() or not np.isfinite(population).all()
            or population.lt(0).any() or not population.eq(population.round()).all()):
        raise ValueError(f"PEP population must be finite, nonnegative integers in {path}")
    pep["resident_population"] = population.astype("int64")

    selected = pep.loc[
        pep["year"].between(first_year, last_year),
        ["county_fips", "year", "resident_population"],
    ].copy()
    if selected.empty or selected["year"].min() != first_year or selected["year"].max() != last_year:
        raise ValueError(f"Selected PEP years do not cover {first_year}-{last_year}: {path}")
    if set(selected["year"].unique()) != set(range(first_year, last_year + 1)):
        raise ValueError(f"Missing a selected year in {first_year}-{last_year}: {path}")
    pieces.append(selected)

    raw_hashes = pep["raw_sha256"].dropna().astype(str).unique().tolist()
    if not raw_hashes or any(len(value) != 64 for value in raw_hashes):
        raise ValueError(f"Missing native raw Census SHA-256 identity in {path}")
    source_identities.append({
        "path": str(path),
        "selected_years": [first_year, last_year],
        "parquet_sha256": sha256(path.read_bytes()).hexdigest(),
        "raw_sha256": sorted(raw_hashes),
        "vintages": sorted(int(value) for value in pep["vintage"].unique()),
    })

weights = pd.concat(pieces, ignore_index=True)
if weights.duplicated(["county_fips", "year"]).any():
    raise ValueError("Duplicate county/year PEP weight rows")
weights = weights.sort_values(["county_fips", "year"]).reset_index(drop=True)

output_path = Path(
    "data/curated/right_to_shelter/"
    "right_to_shelter__county_weights__Y2010-2024__pep_v2020_v2024.parquet"
)
if output_path.exists():
    raise FileExistsError(f"Refusing to overwrite existing artifact: {output_path}")
provenance = ProvenanceBlock(
    geo_type="county",
    weighting="resident_population",
    extra={
        "dataset_type": "right_to_shelter_county_resident_weights",
        "population_universe": "all county residents; not legal eligibility",
        "source_reference_date": "July 1",
        "policy_reference_date": "January 31",
        "approved_vintage_policy": "v2020: 2010-2020; v2024: 2021-2024",
        "researcher_approval_date": "2026-10-26",
        "sources": source_identities,
    },
)
write_parquet_with_provenance(weights, output_path, provenance)
```

Audited input identities at the time of this note:

| Curated file | Rows and years | SHA-256 of Parquet bytes |
|---|---:|---|
| `pep_county__v2020.parquet` | 34,573 rows; 2010–2020; 3,143 counties/year | `2bec7aaf86625b6e3e01213acf5d6c9a9284454ddcaf2c0ce2211cf543797469` |
| `pep_county__v2024.parquet` | 15,720 rows; 2020–2024; 3,144 counties/year | `bd14769463e9e156c74be430b803d50ff5652c427937cf7421bd3d1e50ebccdf` |

The approved common basis estimates geographic coverage, without introducing
a population-specific eligibility proxy. The frozen Census MSA/CoC overlap
geography selection is independent of this PEP vintage choice. The artifact
is external curated data; exact source identities above and the executable
normalization preserve its construction in this repository.

## Executed normalization and build check (2026-10-26)

The normalization produced 47,149 rows for 2010–2024, with unique county/year
keys, all requested years present, and positive resident counts throughout.
The three-column schema and embedded approved vintage/source provenance were
read back and validated. Output Parquet SHA-256:
`01166afdd6464189e99d346edef705a1f30c8d426d5a05e18b202de37fdfdd50`.

The full national native build was attempted with this input, broad coding,
weather excluded, and geography rule `any`. It correctly refused unresolved
single-adult scopes in Hennepin, NJ, NY's early years, MA and DC before writing
national outputs. The weight decision does not resolve those legal gaps.
The example recipe records the approved choices but retains its unresolved
reference marker until real annual outputs can be joined.

The persisted [membership and cell audit](right-to-shelter-weight-audit.json)
uses the native Census MSA 2023 membership and native CoC 2024 / county 2023
overlay. It records all membership allocations, source content identities,
known mixed cells with denominators/shares and both classification flags,
and unresolved cells by population and jurisdiction. It is an audit, not a
completed all-population reference table.

| Audit scope, 2010–2024 | MSA | CoC |
|---|---:|---:|
| Geographies | 393 | 387 |
| Known homogeneous population cells | 16,908 | 15,957 |
| Known mixed population cells, complete positive denominators | 60 | 0 |
| Unresolved population cells | 717 | 1,458 |
| Distinct geography-years with any unresolved population | 309 | 596 |
| Unresolved geography-years missing county weight support | 0 | 0 |

Cells here are geography × year × legal population. The 60 known mixed cells
are family and pregnancy coverage in Boston (`14460`) and Providence (`39300`)
for all 15 years. Boston's resident coverage ranges from 90.84% to 91.00%;
Providence's ranges from 34.26% to 34.90%. Both receive flag 1 under the
approved any-coverage rule. Providence receives flag 0 under the majority
sensitivity, a difference in 30 population cells. Both MSAs still have
unresolved single-adult coding; these audited shares do not bypass the native
all-population builder's failure on that gap.

Every county/year in each unresolved geography-year was also checked against
the approved input; all are present with positive counts. Thus the current
listed-jurisdiction gaps introduce no additional missing-weight dependency.
Homogeneous rows avoid weights, including areas where nationwide membership
and PEP county universes differ. A changed legal inventory or geography
vintage requires rerunning support checks; this audit does not certify new
mixed denominators outside its recorded inputs. No known mixed CoC cell is
asserted while unresolved cells remain; zero in the table means zero among
the legally resolved cells only.

Audit JSON SHA-256:
`b31701fb3eca610c1c19e28744da05568b2632f2a605c65faf1514155f767972`.
