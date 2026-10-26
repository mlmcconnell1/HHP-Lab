# Right-to-shelter legal evidence and coverage plan

Status: research evidence for `coclab-gl6pz`; candidate coding only. This is
not a completed national legal inventory. Matt accepted a provisional
none-elsewhere baseline on 2026-10-26, subject to revision after feedback;
population/coding choices and listed-jurisdiction gaps remain separate.

## Study frame and coding conventions

The example study in `recipes/examples/shelter-capacity-results.yaml` covers
PIT/HIC years 2010–2024. Its metro unemployment input is narrower (2015–2023),
but that does not narrow the legal source window unless the research design
does so. The right-to-shelter implementation uses January 31 as the annual
reference date. An interval affects a year only when it is in effect on that
year's January 31. Thus changes after January 31, 2024 first affect the 2025
annual row, outside this example's window.

Keep four concepts independent in maintained evidence: legal force/strength,
population and eligibility scope, weather-triggered authority, and actual
weather activation. The Jan 31 legal indicator does not assert that a weather
threshold was met that night. Observed HUD HIC beds are a separate capacity
measure, not evidence of a legal right. A geographic coverage share is not an
estimate of the share of residents legally eligible when the law uses
household, income, residency, disability, migration, or other individual
criteria.

The current source contract is a reviewed baseline plus dated exception
intervals. A jurisdiction may be coded as baseline-none only for years that
have actually been reviewed; an absent exception outside that scope is
unknown, not no-right. The candidate jurisdictions below do not establish
a nationwide legal finding. Matt has now authorized treating unlisted
jurisdictions as none provisionally for analysis, provided that the assumption
travels with the results. This decision does not resolve explicit gaps inside
listed jurisdictions. The canonical candidate files below still represent
the earlier evidence-only coverage; full national inputs and joins remain to
be built under the accepted assumption.

## Candidate jurisdictions

| Jurisdiction | Supported primary evidence and dates | Population and restriction evidence | Candidate interpretation; unresolved points |
|---|---|---|---|
| New York City (substate) | The New York Court of Appeals says the Callahan consent judgment was dated August 26, 1981 and quotes its covered class: homeless men meeting the public-assistance need standard or needing temporary shelter because of physical, mental, or social dysfunction. It identifies the extension to homeless women in *Eldredge v. Koch* (1983). [*Callahan v. Carey*, 12 N.Y.3d 496 (2009)](https://www.nycourts.gov/Reporter/3dseries/2009/2009_04302.htm). A later court identifies family relief under *McCain v. Koch* (1983). [*Herkert v. State*, 81 Misc. 3d 1 (Sup. Ct. 2023)](https://www.nycourts.gov/reporter/files/bv/81Misc3d.pdf), pp. 442–443. The *Boston v. City of New York* final judgment requires emergency shelter for eligible homeless families with children who lack alternate housing. The judgment's condition says it becomes effective after judicial approval and entry; the Appellate Division later cites entry on December 12, 2008. [Final judgment](https://www.nyc.gov/html/law/downloads/pdf/BOSTON%20FINAL%20JUDGMENT%209%2017%2008%20%20FINAL%20AT%20CITY%20HALL.pdf); [*Zheng v. City of New York*, 2012 NY Slip Op 02033](https://www.nycourts.gov/reporter/3dseries/2012/2012_02033.htm). | The actions have differentiated classes. Pregnant women are included as a subgroup of the homeless women covered under *Eldredge*, subject to that decree's terms; there is no separate pregnancy rule in the cited decision. Boston's 2008 order explicitly concerns families with children, so adult-family status is not silently assumed. | Candidate year-round enforceable protection for the covered classes, subject to decree eligibility. The court entered the March 15, 2024 settlement order that day, and the order states that its provisions are added to the existing Callahan agreement. [Court transcript/order, March 15, 2024](https://www.coalitionforthehomeless.org/wp-content/uploads/2024/03/3152024Callahan.pdf). It provides that adult new arrivals initially receive 30 days (60 days for those under 23) and sets reapplication limits and exceptions. The City says the agreement applies only to adults and does not affect families with children. [NYC Mayor's Office, March 15, 2024](https://www.nyc.gov/mayors-office/news/2024/03/mayor-adams-agreement-the-legal-aid-society-callahan-right-shelter). This post-Jan31 change must not alter the 2024 PIT-reference row. The exception file records the single-adult post-change scope as unknown because one strength value cannot summarize both the new-arrival and long-term resident groups. |
| New York State outside NYC | NY Social Services Law § 131 makes local social-services officials responsible for assistance and care to people residing or found in the district who need help, “insofar as funds are available”; it does not itself guarantee a particular shelter placement. [NYS Open Legislation, § 131](https://www.nysenate.gov/legislation/laws/SOS/131). § 131-v authorizes temporary emergency shelter for eligible homeless households in qualifying units, with monthly review and a six-month default limit (longer only under stated district determination); it is authorization for a particular program, not an unqualified bed entitlement. [NYS Open Legislation, § 131-v](https://www.nysenate.gov/legislation/laws/SOS/131-V). OTDA's [16-ADM-11, Temporary Housing Assistance: Consolidation and Clarification of Policy](https://otda.ny.gov/policy/directives/2016/ADM/16-ADM-11.pdf), is dated November 3, 2016 and describes locally administered assistance for individuals and families. New York's statewide cold-weather regulation, [18 NYCRR § 304.1](https://govt.westlaw.com/nycrr/Document/If0386b55f29f11e6848bebc42a960167?bhcp=1&contextData=%28sc.Default%29&transitionType=Default), was adopted effective March 15, 2017; it defines inclement winter weather as air temperature at or below 32°F including wind chill, and requires each district to take specified protective steps, including offering/directing people to appropriate sheltered facilities. The formal regulation's 2017 effective date does not establish whether identical local or statewide policies applied in earlier years. | Temporary housing aid is conditional and locally administered. The directive describes individuals and families, but this review has not established a distinct pregnant-person category. The general § 131 duty concerns public assistance and care, not expressly shelter. Code Blue is weather-only and covers people experiencing homelessness, but local forecasts, activation notices, facility availability, and local procedures are distinct from the policy's existence. OTDA's [22-ADM-09](https://otda.ny.gov/policy/directives/2022/ADM/22-ADM-09.pdf) gives district budgeting instructions for the 2022 directive. | Candidate conditional temporary-housing assistance and weather-only cold-weather protection from March 15, 2017, not a strict statewide year-round shelter entitlement. The artifact starts the evidenced assistance interval on 2016-11-03, which first affects the Jan. 31, 2017 reference date; the statewide Code Blue rule begins 2017-03-15, so it first affects Jan. 31, 2018. Earlier years remain an explicit unknown gap. Later directives/amendments, local variation, and subgroup eligibility need reconstruction. NYC must remain a separate substate jurisdiction. |
| Massachusetts | Mass. Gen. Laws ch. 23B, § 30 provides, subject to appropriation, emergency housing assistance to needy families with children and a pregnant woman with no other children, including temporary shelter when the family has no feasible alternative housing. [Official current statute](https://malegislature.gov/Laws/GeneralLaws/PartI/TitleII/Chapter23b/Section30). The Oct. 31, 2023 capacity declaration under 760 CMR 67.10 was effective Nov. 1, 2023 through Feb. 28, 2024; the Commonwealth's program page confirms the declaration and that the guidance began Nov. 1, 2023. [Declaration](https://www.mass.gov/doc/declaration-emergency-assistance-family-shelter-1/download), [program page](https://www.mass.gov/orgs/emergency-assistance-ea-family-shelter). The declaration was later extended, but the period relevant to the 2024-01-31 PIT reference is the original declaration. | This is a family/pregnancy program, not a single-adult entitlement. Statutory and regulatory eligibility includes income/household and homelessness/alternative-housing tests. The October 2023 capacity rules prioritized eligible families and allowed a waitlist after capacity; that weakens any claim of guaranteed immediate placement for all otherwise eligible households. | Candidate conditional year-round family/pregnancy protection before Nov. 1, 2023 and conditional/capacity-limited status from Nov. 1, 2023. Exact 2010–2023 historical regulations, annual appropriations, eligibility changes, and enforceability must be reviewed before populating all years. The 2023 declaration is dated precisely, so it does not change Jan. 31, 2023 status; it does affect Jan. 31, 2024. The subsequent 2024 extension and later stay-limit law do not affect the 2024-01-31 reference, but would matter if the data window is extended. |
| District of Columbia | D.C. Law 18-367, the Homeless Services Reform Amendment Act of 2010, became effective April 8, 2011 and amended D.C. Code § 4-753.01(c). Its text requires appropriate space for a homeless District resident who cannot access other housing when actual or forecast temperature (including wind chill) is below 32°F or temperature/heat index exceeds 95°F. It permits, but does not require, space for nonresidents and gives residents priority; low-barrier/severe-weather low-barrier shelters need not verify or prioritize residency. [Official enrolled law](https://code.dccouncil.gov/us/dc/council/laws/docs/18-367.pdf), [law history/effective date](https://code.dccouncil.gov/us/dc/council/laws/18-367), [current codification](https://code.dccouncil.gov/us/dc/council/code/sections/4-753.01). | This is a severe-weather, resident-prioritized duty, not a year-round general shelter entitlement. “Cannot access other housing arrangements” is statutorily constrained to living somewhere not intended as a residence or in a dangerous situation. Specific low-barrier exceptions and the resident definition matter. | Candidate weather-only status from Apr. 8, 2011; it is not in force on Jan. 31, 2011 under this amendment, and the prior 2005–2011 rule must be examined before assigning 2010 or 2011. Weather authority must not imply actual activation or count coverage as an all-resident right. No separately verified year-round family or single-adult entitlement is established by this provision. Check subsequent permanent amendments and historical code versions before finalizing 2012–2024 intervals. |

### Additional candidates reviewed 2026-10-26

Matt supplied these additional leads and proposed categories. The table below
records the evidence and disposition; it does not assign an entire CoC or MSA
the status of a city within it. Primary-source links are citations, not copied
source documents; redistribution permission for their full contents has not
been established.

| Jurisdiction / rough CoC anchor | Category and dated evidence | Reference-table disposition |
|---|---|---|
| Hennepin County, Minnesota / MN-500 | **Policy-based family protection.** County [board action request 24-0119](https://hennepinmn.legistar.com/ViewReport.ashx?GID=953&GUID=E36B49B1-294B-43B8-8587-489BEEAFEBA6&ID=5676091&M=R&N=Text&Title=Legislation+Text) describes shelter-all for families with children since 2005 and overflow expansion in 2022–23. The county's [housing strategy](https://www.hennepincounty.gov/government/projects-initiatives/housing-and-homelessness/housing-stability-strategy) describes eligibility and children under 18. Its [January 14, 2022 bulletin](https://content.govdelivery.com/accounts/MNHENNE/bulletins/3017e5f) dates elimination of family self-pay to **2022-01-01**, explicitly leaving shelter-all unchanged. | Candidate `families_strength=conditional`, distinguished from statutory/court-enforced protection by administrative authority. Do not infer single-adult or pregnancy-alone eligibility. Adoption is evidenced to year precision only; retain 2005 without inventing a day. Not yet in executable inputs: reconstruct historical eligibility and use explicit Hennepin county membership (27053), not all Minnesota or the Minneapolis MSA. Fee removal is a separate access change, not the beginning of shelter-all. |
| New Jersey / NJ-* | **Statutory weather planning/warming-center duty.** [P.L. 2017, c.68](https://pub.njleg.gov/bills/2016/PL17/68_.PDF) took effect **2017-05-11**. Original triggers were at most 25°F without precipitation, at most 32°F with precipitation, or wind chill at most 0°F for two hours. [P.L. 2019, c.427](https://pub.njleg.state.nj.us/Bills/2018/AL19/427_.HTM) took effect **2020-01-21**, changing the temperature trigger to at most 32°F regardless of precipitation. | Added two dated canonical weather-policy intervals; first affects the 2018 Jan31 row, threshold amendment affects 2020. The law requires county plans, coordination and warming centers for at-risk people, not a verified individual overnight bed guarantee. Earlier weather coverage and year-round rights remain unknown, so all-population RTS flag derivation still fails closed. County implementation/compliance is a separate unmeasured dimension; the supplied ACLU compliance lead remains unverified. |
| Philadelphia / PA-500 | **Administrative cold-weather service.** A [dated city announcement](https://www.phila.gov/2024-01-18-mayor-parker-city-officials-announce-preparations-for-winter-storm/) documents activation on **2024-01-14**, enhanced outreach, warming space, and shelter access without ID during the alert. | Evidence of a specific activation, not the policy's adoption date or proof of an individual entitlement. Retain as an optional administrative-weather candidate pending historical policy and population review; no full-panel interval or RTS flag added. Ordinary shelter-first/low-barrier practice alone is excluded. |
| Baltimore City / MD-501 | **Administrative winter shelter/Code Blue.** The city's [FY2025 winter shelter plan](https://homeless.baltimorecity.gov/sites/default/files/MOHS%20FY25_Winter%20Shelter%20%26%20Code%20Purple%20Plan%2011.7.2024.pdf) search extract distinguishes winter shelter declarations from health-department Code Blue alerts and retrospectively reports both for 2023–24. Full PDF retrieval failed in this review. | Research lead only. Do not equate a health alert with an enforceable bed guarantee or backfill the 2010–24 panel from a later plan. Retrieve historical plans, triggers and coverage before creating executable intervals. |
| Delaware / DE-500 | **Administrative/nonprofit Code Purple**, rather than Code Blue. A [2022-01-10 state DHSS release](https://dhss.delaware.gov/newsroom/with-arrival-of-cold-snap-nonprofits-and-volunteers-are-opening-code-purple-sites-across-delaware/) describes nonprofit/volunteer activations varying by county/site and limited capacity. | Optional weather-service candidate; no statewide individual right established. Historical dates, site coverage and admission criteria remain unresolved. Do not turn this activation report into an all-population statewide weather entitlement. |
| Los Angeles **city** / within CA-600 | **Capacity obligation.** The city's [May 31, 2024 CAO report](https://cityclerk.lacity.org/onlinedocs/2023/23-1022-s3_rpt_cao_5-31-24.pdf) identifies the **2022-06-14** Alliance settlement and a June 14, 2027 target for 12,915 housing/intervention units, based on 60% of the specified unsheltered population. It distinguishes targets from open/occupiable interventions. | Separate capacity-mandate candidate, beginning with the 2023 Jan31 reference row; do not set `rts_*` from a supply target. Retrieve the controlling settlement and amendments before final annual control coding. City coverage does not cover all CA-600 or the Los Angeles MSA, and the separate county settlement must not be conflated with the city's agreement. |
| Sacramento **city** / within CA-503 | **Capacity/enforcement condition.** The [city's Measure O page](https://www.cityofsacramento.gov/city-manager/projects-and-programs/measureo) gives an effective date of **2022-12-23** and describes a limited, budget-conditioned city obligation. The notice-and-demand process begins **2023-06-21**, a distinct date. | Separate capacity/enforcement candidate, first affecting 2023 Jan31; not an individual shelter entitlement. Verify the ordinance and historical amendments before producing annual controls. The city is only part of CA-503 and the MSA; do not label either entire geography as covered. |

The proposed reporting groups are broad year-round protection (NYC, with its
existing eligibility caveats), family/pregnancy protection (Massachusetts),
policy-based family protection (Hennepin), statutory weather duties (DC, NY,
NJ), optional administrative weather services, and capacity/enforcement
obligations. These are overlapping dimensions: NY, for example, has both
conditional year-round assistance and weather rules. Preserve the existing
population, strength, weather, authority and qualification fields rather than
replace them with one mutually exclusive category. Administrative practices
must remain distinguishable from statutory weather duties even if a future
broad sensitivity analysis includes both.

### Enforcement precedent excluded from shelter-right flags

The case name is **Martin v. City of Boise**, not Martinez. The Ninth Circuit's
[2018 opinion](https://cdn.ca9.uscourts.gov/datastore/opinions/2018/09/04/15-35845.pdf)
was filed **2018-09-04**; the Supreme Court decided
[*City of Grants Pass v. Johnson*](https://www.supremecourt.gov/opinions/23pdf/23-175_19m2.pdf)
on **2024-06-28**, rejecting the relevant Eighth Amendment restriction on
generally applicable camping penalties. Neither is entered as an individual
shelter right. If separately modeled as an enforcement-regime control, the
Jan31 convention would first capture the 2018 Martin decision in **2019** and
still capture the pre-Grants-Pass regime in **2024**; this is not a 2018–2023
annual dummy. Review injunctions, prior precedent and mandate dates before
claiming these decisions alone define the complete regional enforcement regime.

### Accepted provisional baseline and output footnote

On 2026-10-26 Matt accepted “none everywhere else” provisionally and will
revisit that assumption if additional feedback identifies other policies.
This supersedes the requirement to complete a nationwide negative legal
inventory before using a closed-list baseline. It does not turn that assumption
into a verified legal finding or resolve missing dates, population scope, or
mapping for the listed candidates. Those gaps remain explicit.

The example recipe now declares the versioned annotation
`right-to-shelter-provisional-baseline#r1`, scoped to `right_to_shelter` on both
panels, with an explicit `public_text` for public outputs. It explains the
none-elsewhere assumption, decision date, possibility of revision, continued
listed-jurisdiction qualifications, and distinction from capacity mandates.
The existing results path carries applicable annotations into
`annotations.json`, report data notes/footnotes and the bundle README,
including analyses whose sample exclusion uses the annotated column. Base
analyses that do not use that column need not display the footnote. Revision
of the classification should increment the annotation revision and preserve
the prior recipe/run identity.

This recipe annotation is an immediate declaration of the research choice,
not a completed national data build. When generating national source outputs,
record the assumption in baseline provenance and attach an equivalent
data-origin annotation to the generated RTS columns, so other recipes inherit
it as well. Preserve it through the final classification derive/join alongside
the existing subgroup, geography and weighting annotations.

### Strength coding is a research decision

The source should retain categorical dimensions rather than decide an analysis
flag implicitly. For review, a defensible candidate strict rule is to count
only population-specific, enforceable year-round protection. A broad rule
could additionally count conditional year-round programs; weather-only policy
should remain separately selectable, not silently folded into either. These
are candidate robustness rules, not final definitions. In particular:

- NYC has enforceable orders but defined classes and later subgroup-specific
  modifications.
- NY outside NYC has statutory/regulatory and administrative assistance with
  conditions; Code Blue is a separate weather dimension.
- Massachusetts' eligible family/pregnancy program is subject to statutory
  eligibility, appropriation, and a capacity declaration beginning
  2023-11-01.
- DC's cited entitlement is weather-triggered and resident-prioritized.

Population columns must not imply one jurisdiction has equivalent rights for
single adults, families, and pregnant people. If a subgroup is not fully
resolved, mark scope unknown or use a conservative, explicitly caveated
summary; do not represent the summary as subgroup-level legal certainty.

## Baseline completeness and geographic assignment

No evidence reviewed here establishes nationwide absence of other rights for
2010–2024. A national MSA/CoC build may instead use Matt's accepted provisional
none-elsewhere assumption, explicitly versioned and annotated as described
above. Enumerate its geography/time coverage and retain unresolved gaps in
listed jurisdictions as `unknown_gaps`. Distinguish a documented legal review
finding no applicable right from a negative classification made under the
provisional research assumption.

NYC is a substate exception within New York. For MSA/CoC assignment use dated
county membership and explicit five-borough county membership, not CoC-ID
prefixes or the label “New York MSA.” The New York MSA includes counties
outside NYC, so it cannot inherit NYC's status wholesale. DC is a state-equivalent
jurisdiction within the Washington MSA, which also crosses into Maryland and
Virginia; do not label the full metro uniformly. County fragments and
multi-jurisdiction geographies need a documented classification rule and
population weights where the rule requires them. Covered geographic share is
not eligible-person share.

Matt's 2026-10-26 geography decision sets Census MSA/CoC population-overlap
sites as the default metro research approach; Glynn/Fox is restricted to
analysis directly dealing with Glynn/Fox. For the shelter-capacity recipe,
join RTS classifications and MSA-native covariates by the same Census MSA
ID/year to the frozen overlap-site panel. Whole-CoC outcome footprints can
differ from MSA legal-policy/covariate footprints; preserve that caveat rather
than relabeling the selected CoC union as exactly the MSA.

The maintained source should follow the agreed compact contract:

- `right_to_shelter__baseline.json`: versioned annual reference date,
  explicitly reviewed jurisdiction/year intervals with citations, and explicit
  unknown gaps. Omission outside enumerated reviewed coverage must fail.
- `right_to_shelter__exceptions.csv`: dated inclusive policy intervals with
  separate strength and weather-only fields for single adults, families, and
  pregnant people; citation source type/redistribution; and explicit
  `qualification_scope` plus a summary for subgroup or unknown scope.
- Generate annual geography rows using Jan 31 status. Keep strict/broad and
  weather inclusion as documented derivation choices. Do not generate historic
  negatives until the reviewed baseline supports them.

The machine-readable proposal shape is:

```json
{
  "schema_version": "1.0",
  "baseline_version": "<review date or stable tag>",
  "annual_reference_date": "01-31",
  "reviewed_coverage": [
    {
      "jurisdiction_id": "<explicit state/DC code or NYC identifier>",
      "jurisdiction_type": "state|sub_state",
      "first_year": 2010,
      "last_year": 2024,
      "evidence": [
        {
          "citation": "<primary legal source URL and title>",
          "citation_source_type": "statute|regulation|court_order|administrative_directive",
          "citation_redistribution": "public_domain|public|permission_required|unknown|not_applicable"
        }
      ]
    }
  ],
  "unknown_gaps": [
    {
      "jurisdiction_id": "<explicit jurisdiction>",
      "jurisdiction_type": "state|sub_state",
      "first_year": 2010,
      "last_year": 2024,
      "reason": "<specific missing primary-source or population-scope research>"
    }
  ]
}
```

```csv
exception_id,jurisdiction_id,jurisdiction_type,effective_date,ended_date,single_adults_strength,single_adults_weather_only,families_strength,families_weather_only,pregnant_people_strength,pregnant_people_weather_only,single_adults_qualification_scope,single_adults_qualification_summary,families_qualification_scope,families_qualification_summary,pregnant_people_qualification_scope,pregnant_people_qualification_summary,citation,citation_source_type,citation_redistribution
```

These shapes match the runtime contract. The canonical candidate inputs and
their exact content are captured below. `unknown_gaps` accepts an optional
`populations` list; omission means all three populations are unknown for that
jurisdiction-year. Any exception with `qualification_scope=unknown` must fail
closed until the uncertainty is resolved.

Potential future legal research additions include the exact NYC stipulation
and operative *Eldredge*/*McCain* orders, NYS historical statutory/regulatory
and Code Blue intervals, Massachusetts historical regulations and all
relevant 2023–24 capacity extensions, and D.C. § 4-753.01 versions before/after
the 2011 change. This note records those gaps rather than treating the
candidate list as complete.

## Legal status is separate from shelter capacity

The example's HIC inputs are year-round emergency shelter, transitional
housing, and safe haven beds. Preserve these observed capacity measures as
their own variables (and consider aligned per-capita or population-specific
measures only where definitions and data support them). Do not make shelter
bed capacity part of a legal right flag or use it automatically as a
right-to-shelter weight. Whether capacity is a confounder, mechanism, or
outcome is a model-design question. Weather/seasonal beds also need a distinct
definition from year-round HIC capacity.

## Primary source list

- NYC Court of Appeals / official state court decisions: [*Callahan v. Carey*, 12 N.Y.3d 496](https://www.nycourts.gov/Reporter/3dseries/2009/2009_04302.htm); [*Herkert v. State*, 81 Misc. 3d 1](https://www.nycourts.gov/reporter/files/bv/81Misc3d.pdf); [March 15, 2024 court transcript/order](https://www.coalitionforthehomeless.org/wp-content/uploads/2024/03/3152024Callahan.pdf).
- NYC government: [March 15, 2024 Callahan stipulation announcement](https://www.nyc.gov/mayors-office/news/2024/03/mayor-adams-agreement-the-legal-aid-society-callahan-right-shelter).
- New York: [Social Services Law §§ 131](https://www.nysenate.gov/legislation/laws/SOS/131) and [131-v](https://www.nysenate.gov/legislation/laws/SOS/131-V); OTDA [16-ADM-11 (Nov. 3, 2016)](https://otda.ny.gov/policy/directives/2016/ADM/16-ADM-11.pdf), [22-ADM-09](https://otda.ny.gov/policy/directives/2022/ADM/22-ADM-09.pdf), [Temporary Assistance program](https://otda.ny.gov/programs/temporary-assistance/); [18 NYCRR § 304.1 regulatory history/current text](https://govt.westlaw.com/nycrr/Document/If0386b55f29f11e6848bebc42a960167?bhcp=1&contextData=%28sc.Default%29&transitionType=Default).
- Massachusetts Legislature and administration: [M.G.L. ch. 23B, § 30](https://malegislature.gov/Laws/GeneralLaws/PartI/TitleII/Chapter23b/Section30), [2023 capacity declaration](https://www.mass.gov/doc/declaration-emergency-assistance-family-shelter-1/download), [EA program page](https://www.mass.gov/orgs/emergency-assistance-ea-family-shelter), [760 CMR 67.00](https://www.mass.gov/regulations/760-CMR-6700-eligibility-for-emergency-assistance-ea).
- District of Columbia: [D.C. Law 18-367](https://code.dccouncil.gov/us/dc/council/laws/18-367), [enrolled law text](https://code.dccouncil.gov/us/dc/council/laws/docs/18-367.pdf), [current § 4-753.01](https://code.dccouncil.gov/us/dc/council/code/sections/4-753.01).

## Canonical candidate input snapshot

These exact file contents are recorded here because `data` is an external
symlink and the files may not travel with a code checkout. Both are provisional
and deliberately fail flag derivation where the source contract identifies
unknown legal scope. They do not support a complete or national map.

### `right_to_shelter__baseline.json`

```json
{
  "schema_version": "1.0",
  "baseline_version": "candidate-legal-review-2026-10-26-expanded",
  "annual_reference_date": "01-31",
  "reviewed_coverage": [
    {
      "jurisdiction_id": "34",
      "jurisdiction_type": "state",
      "first_year": 2010,
      "last_year": 2024,
      "evidence": [
        {
          "citation": "https://pub.njleg.gov/bills/2016/PL17/68_.PDF — P.L. 2017, c.68, effective 2017-05-11",
          "citation_source_type": "statute",
          "citation_redistribution": "public"
        },
        {
          "citation": "https://pub.njleg.state.nj.us/Bills/2018/AL19/427_.HTM — P.L. 2019, c.427, effective 2020-01-21",
          "citation_source_type": "statute",
          "citation_redistribution": "public"
        }
      ]
    },
    {
      "jurisdiction_id": "NYC",
      "jurisdiction_type": "sub_state",
      "first_year": 2010,
      "last_year": 2024,
      "evidence": [
        {
          "citation": "https://www.nycourts.gov/Reporter/3dseries/2009/2009_04302.htm — Callahan v. Carey, 12 N.Y.3d 496 (2009)",
          "citation_source_type": "court_order",
          "citation_redistribution": "public_domain"
        },
        {
          "citation": "https://www.nyc.gov/html/law/downloads/pdf/BOSTON%20FINAL%20JUDGMENT%209%2017%2008%20%20FINAL%20AT%20CITY%20HALL.pdf — Boston v. City of New York final judgment (2008)",
          "citation_source_type": "court_order",
          "citation_redistribution": "public_domain"
        },
        {
          "citation": "https://www.nycourts.gov/reporter/files/bv/81Misc3d.pdf — Herkert v. State, 81 Misc. 3d 1 (Sup. Ct. 2023)",
          "citation_source_type": "court_order",
          "citation_redistribution": "public_domain"
        }
      ]
    },
    {
      "jurisdiction_id": "36",
      "jurisdiction_type": "state",
      "first_year": 2010,
      "last_year": 2024,
      "evidence": [
        {
          "citation": "https://www.nysenate.gov/legislation/laws/SOS/131 — New York Social Services Law § 131",
          "citation_source_type": "statute",
          "citation_redistribution": "public_domain"
        },
        {
          "citation": "https://otda.ny.gov/policy/directives/2016/ADM/16-ADM-11.pdf — OTDA 16-ADM-11, Temporary Housing Assistance: Consolidation and Clarification of Policy (2016-11-03)",
          "citation_source_type": "administrative_directive",
          "citation_redistribution": "public"
        },
        {
          "citation": "https://govt.westlaw.com/nyreg/Document/I3d652b8519cd11e7be658bd87eb0c0aa?contextData=%28sc.Default%29&originationContext=documenttoc&transitionType=CategoryPageItem&viewType=FullText — 18 NYCRR Part 304 adoption (effective 2017-03-15)",
          "citation_source_type": "regulation",
          "citation_redistribution": "public"
        }
      ]
    },
    {
      "jurisdiction_id": "25",
      "jurisdiction_type": "state",
      "first_year": 2010,
      "last_year": 2024,
      "evidence": [
        {
          "citation": "https://archives.lib.state.ma.us/server/api/core/bitstreams/5af96675-c3b5-4131-895b-edb07e7904f5/content — Massachusetts Acts of 1983, Chapter 450, approved 1983-10-27",
          "citation_source_type": "statute",
          "citation_redistribution": "public_domain"
        },
        {
          "citation": "https://malegislature.gov/Laws/GeneralLaws/PartI/TitleII/Chapter23b/Section30 — Massachusetts General Laws Chapter 23B, § 30",
          "citation_source_type": "statute",
          "citation_redistribution": "public_domain"
        },
        {
          "citation": "https://www.mass.gov/doc/declaration-emergency-assistance-family-shelter-1/download — EA Family Shelter capacity declaration effective 2023-11-01 through 2024-02-28",
          "citation_source_type": "regulation",
          "citation_redistribution": "public"
        }
      ]
    },
    {
      "jurisdiction_id": "11",
      "jurisdiction_type": "state",
      "first_year": 2010,
      "last_year": 2024,
      "evidence": [
        {
          "citation": "https://code.dccouncil.gov/us/dc/council/laws/18-367 — D.C. Law 18-367, effective 2011-04-08",
          "citation_source_type": "statute",
          "citation_redistribution": "public_domain"
        },
        {
          "citation": "https://code.dccouncil.gov/us/dc/council/code/sections/4-753.01 — D.C. Code § 4-753.01",
          "citation_source_type": "statute",
          "citation_redistribution": "public_domain"
        }
      ]
    }
  ],
  "unknown_gaps": [
    {
      "jurisdiction_id": "34",
      "jurisdiction_type": "state",
      "first_year": 2010,
      "last_year": 2024,
      "reason": "New Jersey Code Blue establishes a county planning and warming-center duty from 2017-05-11, amended 2020-01-21. Earlier weather protections and year-round rights for each population remain unreviewed; weather-policy presence does not establish an individual bed entitlement or county compliance."
    },
    {
      "jurisdiction_id": "36",
      "jurisdiction_type": "state",
      "first_year": 2010,
      "last_year": 2016,
      "reason": "The cited statewide temporary-housing directive is dated 2016-11-03 and Code Blue regulation is effective 2017-03-15; earlier statewide status for this study window has not been reconstructed."
    },
    {
      "jurisdiction_id": "25",
      "jurisdiction_type": "state",
      "first_year": 2010,
      "last_year": 2024,
      "populations": ["single_adults"],
      "reason": "M.G.L. c. 23B, § 30 establishes EA shelter for eligible families with children and pregnant women with no other children. This does not establish that single adults have no other applicable shelter protection; single-adult status remains unknown."
    },
    {
      "jurisdiction_id": "11",
      "jurisdiction_type": "state",
      "first_year": 2010,
      "last_year": 2024,
      "reason": "The reviewed D.C. provision establishes a weather-triggered shelter duty from 2011-04-08, but the broader year-round legal status for each population and the prior 2010–2011 law have not been fully reviewed."
    }
  ]
}
```

### `right_to_shelter__exceptions.csv`

```csv
exception_id,jurisdiction_id,jurisdiction_type,effective_date,ended_date,single_adults_strength,single_adults_weather_only,families_strength,families_weather_only,pregnant_people_strength,pregnant_people_weather_only,single_adults_qualification_scope,single_adults_qualification_summary,families_qualification_scope,families_qualification_summary,pregnant_people_qualification_scope,pregnant_people_qualification_summary,citation,citation_source_type,citation_redistribution
nj-code-blue-2017-05-11-to-2020-01-20,34,state,2017-05-11,2020-01-20,none,true,none,true,none,true,subgroup,"At-risk people exposed outdoors or in poorly insulated settings; county alert plans and warming centers do not establish an individual bed entitlement. Year-round status is unknown.",subgroup,"At-risk family members may use weather services; no separate family entitlement is inferred.",subgroup,"At-risk pregnant people may use weather services; no separate pregnancy entitlement is inferred.","https://pub.njleg.gov/bills/2016/PL17/68_.PDF",statute,public
nj-code-blue-threshold-amendment-2020-01-21,34,state,2020-01-21,,none,true,none,true,none,true,subgroup,"Code Blue threshold amended to 32 F or lower regardless of precipitation; the separate wind-chill trigger remains. County compliance and actual activation are not measured; year-round status is unknown.",subgroup,"At-risk family members may use weather services; no separate family entitlement is inferred.",subgroup,"At-risk pregnant people may use weather services; no separate pregnancy entitlement is inferred.","https://pub.njleg.gov/bills/2016/PL17/68_.PDF ; https://pub.njleg.state.nj.us/Bills/2018/AL19/427_.HTM",statute,public
nyc-court-orders-through-2024-03-14,NYC,sub_state,2008-12-12,2024-03-14,enforceable,false,enforceable,false,enforceable,false,subgroup,"Callahan and Eldredge cover homeless men and women under the decree's class and eligibility terms; not an unconditional right for every individual.",subgroup,"Boston final judgment applies to eligible homeless families with children lacking alternate housing; adult families are not established by this source.",subgroup,"Pregnant women fall within Eldredge's covered class of homeless women subject to the decree's eligibility terms; the decision does not define a separate pregnancy rule or expand protection to every pregnant person regardless of class.","https://www.nycourts.gov/Reporter/3dseries/2009/2009_04302.htm ; https://www.nycourts.gov/courts/AD1/calendar/appsmots/2012/March/2012_03_20_dec.pdf ; https://www.nyc.gov/html/law/downloads/pdf/BOSTON%20FINAL%20JUDGMENT%209%2017%2008%20%20FINAL%20AT%20CITY%20HALL.pdf ; https://www.nycourts.gov/reporter/files/bv/81Misc3d.pdf",court_order,public_domain
nyc-adult-new-arrival-modification-from-2024-03-15,NYC,sub_state,2024-03-15,,none,false,enforceable,false,none,false,unknown,"Court-entered Callahan stipulation on 2024-03-15 sets 30-day initial shelter (60 days for new arrivals under 23) and limits reapplication with exceptions. This affects a subset of single adults, while long-term residents retain Callahan protections; a single categorical strength does not describe both groups safely.",subgroup,"March 2024 stipulation applies only to adults seeking shelter and expressly does not affect families with children.",unknown,"The stipulation applies to adults; its effect on pregnant new arrivals cannot be represented separately without population-specific scope. Pregnancy-only classification remains uncertain.","https://www.coalitionforthehomeless.org/wp-content/uploads/2024/03/3152024Callahan.pdf ; https://www.nyc.gov/mayors-office/news/2024/03/mayor-adams-agreement-the-legal-aid-society-callahan-right-shelter",court_order,public_domain
ny-state-temporary-housing-assistance-2016-11-03-to-2017-03-14,36,state,2016-11-03,2017-03-14,conditional,false,conditional,false,conditional,false,subgroup,"OTDA temporary housing assistance is locally administered and eligibility-based; this is a program-level candidate, not an unconditional bed entitlement.",subgroup,"Temporary housing assistance applies to qualifying individuals and families under assistance eligibility and local district rules.",subgroup,"Pregnant applicants are covered only if eligible under the individual/family temporary-assistance rules; no separate pregnancy entitlement is inferred.","https://otda.ny.gov/policy/directives/2016/ADM/16-ADM-11.pdf ; https://www.nysenate.gov/legislation/laws/SOS/131",administrative_directive,public
ny-state-tha-and-code-blue-from-2017-03-15,36,state,2017-03-15,,conditional,true,conditional,true,conditional,true,subgroup,"OTDA temporary housing assistance is locally administered and eligibility-based; this is a program-level candidate, not an unconditional bed entitlement.",subgroup,"Temporary housing assistance applies to qualifying individuals and families under assistance eligibility and local district rules.",subgroup,"Pregnant applicants may qualify under the individual/family temporary-assistance rules; Code Blue weather measures apply to homeless people generally, subject to the regulation's trigger.","https://otda.ny.gov/policy/directives/2016/ADM/16-ADM-11.pdf ; https://govt.westlaw.com/nyreg/Document/I3d652b8519cd11e7be658bd87eb0c0aa?contextData=%28sc.Default%29&originationContext=documenttoc&transitionType=CategoryPageItem&viewType=FullText ; https://govt.westlaw.com/nycrr/Document/If0386b55f29f11e6848bebc42a960167?bhcp=1&contextData=%28sc.Default%29&transitionType=Default",regulation,public
ma-emergency-assistance-family-pregnancy-1983-10-27,25,state,1983-10-27,2023-10-31,none,false,conditional,false,conditional,false,unknown,"EA law covers families and pregnant women without other children; no conclusion that single adults lack other shelter protections is made. The baseline marks this population unknown.",subgroup,"M.G.L. c. 23B, § 30 and 760 CMR 67 limit EA to needy families with children meeting eligibility and no-feasible-alternative-housing conditions; subject to appropriation.",subgroup,"The EA statute includes a pregnant woman with no other children, subject to eligibility, no-feasible-alternative-housing, and appropriation conditions.","https://archives.lib.state.ma.us/server/api/core/bitstreams/5af96675-c3b5-4131-895b-edb07e7904f5/content ; https://malegislature.gov/Laws/GeneralLaws/PartI/TitleII/Chapter23b/Section30 ; https://www.mass.gov/regulations/760-CMR-6700-eligibility-for-emergency-assistance-ea",statute,public_domain
ma-ea-capacity-declaration-2023-11-01-to-2024-02-28,25,state,2023-11-01,2024-02-28,none,false,conditional,false,conditional,false,unknown,"The EA statute covers eligible family/pregnancy populations; single-adult shelter status remains unknown and is not coded none.",subgroup,"Eligible families with children remained covered by EA, but the capacity declaration created prioritization and a waitlist when the system reached its 7,500-family capacity; this is not guaranteed immediate placement.",subgroup,"Eligible pregnant women without other children remained within EA, subject to the capacity declaration, prioritization, and waitlist.","https://www.mass.gov/doc/declaration-emergency-assistance-family-shelter-1/download ; https://www.mass.gov/orgs/emergency-assistance-ea-family-shelter ; https://malegislature.gov/Laws/GeneralLaws/PartI/TitleII/Chapter23b/Section30",statute,public_domain
dc-severe-weather-duty-from-2011-04-08,11,state,2011-04-08,,none,true,none,true,none,true,all,"D.C. Law 18-367 requires appropriate space for a homeless resident unable to access other housing during specified cold or heat conditions; this field represents weather-policy existence, not actual activation. Year-round single-adult status remains unknown in the baseline.",all,"The statute covers homeless resident families unable to access other housing in severe weather; nonresident service is discretionary and low-barrier exceptions apply. Year-round family status remains unknown in the baseline.",all,"Weather-only duty may apply to a pregnant resident as an individual or family member if statutory criteria are met; no separate year-round pregnancy right is established.","https://code.dccouncil.gov/us/dc/council/laws/18-367 ; https://code.dccouncil.gov/us/dc/council/laws/docs/18-367.pdf ; https://code.dccouncil.gov/us/dc/council/code/sections/4-753.01",statute,public_domain
```
