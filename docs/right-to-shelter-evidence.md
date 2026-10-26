# Right-to-shelter legal evidence and coverage plan

Status: research evidence and provisional coding for `coclab-gl6pz`. This is
not an exhaustive national legal inventory. Matt accepted the provisional
negative baseline and its extension to uncovered populations within listed
jurisdictions on 2026-10-26, subject to revision after feedback. Main coding
is broad year-round protection, weather excluded, any geographic coverage
in any of the three legal populations; retain majority and weather sensitivities.

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
travels with the results. Uncertainty about an identified policy still fails
closed; no identified qualifying protection is provisionally none, including
uncovered populations within listed jurisdictions. The canonical baseline enumerates the
50 states, DC and five territories for 2010–2024, with provisional-none basis
outside identified policy coverage. NYC and Hennepin have explicit county
scopes. Historical corrections below resolve the previous study-window gaps;
the separate post-March-2024 NYC subgroup uncertainty is retained outside
the January-reference 2010–2024 window.

## Original jurisdiction research leads

This table records initial evidence and uncertainties. The finalized interval
updates and canonical snapshot below supersede its pending-work language.

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

This recipe annotation declares the research choice. The source now also
supports an explicit `provisional_baseline` annotation and per-coverage
`basis: provisional_none`; affected mapped RTS flags and covered shares inherit
that data-origin caveat, so other recipes can retain it without redeclaration.
Active exception rows do not inherit a default-absence basis. Preserve these
contracts through the final classification derive/join alongside the existing
subgroup, geography and weighting annotations. This is not a completed
national data build while listed-jurisdiction gaps remain.

### Finalized study-window intervals (2026-10-26)

NY's [94 ADM-20, updated in 1996](https://otda.ny.gov/policy/directives/1994/ADM/94_ADM-20_updated96.pdf)
and [96 ADM-20](https://otda.ny.gov/policy/directives/1996/ADM/96_ADM-20.pdf)
support conditional THA before the study start. The [2015 directive](https://otda.ny.gov/policy/directives/2015/ADM/15-ADM-06-T.pdf)
has an effective date of December 15, 2015; the [OAH transmittal](https://otda.ny.gov/hearings/transmittals/2015/15-02.pdf)
records its predecessor and delay. [16-ADM-11](https://otda.ny.gov/policy/directives/2016/ADM/16-ADM-11.pdf)
replaces the 2015 directives from November 3, 2016. Eligibility revisions do
not change the conditional category or establish individual eligibility.
[EO151](https://dos.ny.gov/january-27-2016vol-xxxviii-issue-4-book-1-2)
adds statewide cold-weather protective steps from January 5, 2016, before
the March 15, 2017 regulation. NY therefore has conditional year-round flags
in all study years and a weather-policy flag from the 2016 annual observation.

DC's [original Act](https://code.dccouncil.gov/us/dc/council/laws/docs/16-35.pdf)
and [effective-date history](https://code.dccouncil.gov/us/dc/council/laws/16-35)
support severe-weather protection from October 22, 2005. The
[2011 amendment](https://code.dccouncil.gov/us/dc/council/laws/18-367)
changes mandatory access to residents, with nonresident discretion and
low-barrier exceptions, from April 8, 2011. The 2010 and 2011 annual rows
use the original scope; 2012–2024 use the revised scope. No identified
year-round protection is provisionally none under the approved inventory
rule, without asserting absence of every independent legal duty.

Hennepin's [official board record](https://hennepinmn.legistar.com/LegislationDetail.aspx?GUID=EE81BC82-470D-47B5-AA53-EAA2953C0BDA&ID=6570409&Options=&Search=)
dates family shelter-all to 2005. The interval clips it at January 1, 2010
for the study without inventing an adoption day. It is conditional county
policy for eligible families with children. [Self-pay removal](https://content.govdelivery.com/accounts/MNHENNE/bulletins/3017e5f)
from January 1, 2022 gets a separate interval without changing the flag.
Pregnancy-alone provider access does not establish a countywide guarantee;
it and single-adult countywide protection are provisionally none in this
inventory under the approved decision.

Massachusetts retains conditional EA coverage for eligible families and
pregnant women throughout the window. The [2012 renumbering notice](https://www.mass.gov/doc/housing-stabilization-notice-2012-13-cross-references-to-new-regulation-numbers/download)
documents program continuity across the regulation change with eligibility
revisions. The November 1, 2023 capacity declaration first affects the 2024
annual observation. June 2024 stay limits do not change January 31, 2024.
Singles are provisional none in the approved inventory. This classification
does not promise immediate placement or backdate detailed current eligibility.

These updates and the approved baseline extension resolve the former
study-window gaps. Unknown NYC subgroup scope after March 15, 2024 remains
explicit outside the January-reference 2010–2024 window.

### Strength coding is a research decision

The source should retain categorical dimensions rather than decide an analysis
flag implicitly. For review, a defensible candidate strict rule is to count
only population-specific, enforceable year-round protection. A broad rule
could additionally count conditional year-round programs; weather-only policy
should remain separately selectable, not silently folded into either. These
are available robustness rules; the main run now uses broad year-round coding
without weather. In particular:

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

## Materialized joins and report checks

The canonical 2010–2024 reference outputs contain 5,895 MSA/year and 5,805
CoC/year rows. Native preparation writes joined CoC outcomes plus RTS fields
and a native MSA reference panel under
`/home/matt/HHP-Output/shelter-capacity-rts-reference/20261026/run-04/`.
All reference flags and shares match the joined values exactly. The CoC
outcome panel also contains 393 rows for 75 historical IDs outside the B2024
reference; their RTS status remains null and is excluded when known status
is required, rather than provisionally coding an unmapped geography zero.

The frozen 0.99 overlap cohort produces 960 site/year rows for 64 MSAs. Every
site/year has matching RTS fields. The current evidence supports 2024 outcome
comparison; 56 sites have observed complete unsheltered counts that year.
Earlier outcome cells remain unknown pending historical compatibility review.
This is separate from complete annual legal coding. The native covariate-join
contract is implemented, but the full scientific run still requires the
separately tracked covariate/source coverage and numerical research inputs.

The private reference-check bundle is
`/home/matt/HHP-Output/right-to-shelter-reference-check/20261026/run-02/results/right-to-shelter-reference-check/ae8192d189b6437bb600ab2f40805d93/`.
It completes the inventory and 2024 outcome/exclusion checks and passes
portable integrity and numerical replay with zero differences. Its automatic
source notes include the provisional baseline, subgroup qualifications,
population weights and geographic approximation. The public counterpart under
`/home/matt/HHP-Output/right-to-shelter-reference-check/20261026/public/`
includes public note text in README/report output while retaining the existing
withholding of restricted or unreviewed source details and row-level inputs.
These descriptive checks introduce no equivalence margins or scientific fit
thresholds and do not substitute for a locked research run.

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

## Canonical provisional input snapshot

These exact file contents are recorded here because `data` is an external
symlink and the files may not travel with a code checkout. Both are provisional.
They support the requested 2010–2024 build; unknown identified-policy scope
outside that window remains explicit and fails closed.

### `right_to_shelter__baseline.json`

The companion `right_to_shelter__substate_counties.csv` is additive to the
built-in NYC five-borough assignment. Its exact contents are:

```text
jurisdiction_id,jurisdiction_type,county_fips
HENNEPIN,sub_state,27053
```

The county assignment limits Hennepin's conditional family policy to that
county; it does not create a statewide Minnesota shelter right.

```json
{
  "schema_version": "1.0",
  "baseline_version": "provisional-national-baseline-2026-10-26-r2",
  "annual_reference_date": "01-31",
  "provisional_baseline": {
    "id": "right-to-shelter-provisional-source-baseline",
    "revision": 2,
    "category": "caveat",
    "summary": "Provisional absence of unlisted shelter protections",
    "text": "No identified qualifying protection is provisionally coded none under the researcher decision of October 26, 2026, including uncovered populations within listed jurisdictions. This working inventory is not an exhaustive legal review and may be revised after feedback. Explicit uncertainty about an identified policy remains unknown. Covered shares describe geographic policy coverage, not individual eligibility; provider access and capacity mandates do not establish an individual entitlement.",
    "public_text": "None means no qualifying protection identified in this provisional inventory, including uncovered populations within listed jurisdictions, under the researcher decision of October 26, 2026. This is not an exhaustive legal review and may change after feedback. Explicit policy uncertainties remain unknown; geographic shares do not measure legal eligibility.",
    "evidence": [{"name": "Researcher baseline decision and source caveats", "ref": "docs/right-to-shelter-evidence.md"}],
    "scope": {"columns": ["rts_single_adults", "rts_families", "rts_pregnant_people", "rts_single_adults_population_share", "rts_families_population_share", "rts_pregnant_people_population_share"]}
  },
  "reviewed_coverage": [
    {
      "jurisdiction_id": "HENNEPIN",
      "jurisdiction_type": "sub_state",
      "basis": "provisional_none",
      "county_fips": ["27053"],
      "first_year": 2010,
      "last_year": 2024,
      "evidence": [{"citation": "Hennepin County board action request 24-0119: shelter-all family policy since 2005; docs/right-to-shelter-evidence.md", "citation_source_type": "administrative_directive", "citation_redistribution": "public"}]
    },
    {"jurisdiction_id": "01", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "02", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "04", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "05", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "06", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "08", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "09", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "10", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "12", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "13", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "15", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "16", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "17", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "18", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "19", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "20", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "21", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "22", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "23", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "24", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "26", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "27", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "28", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "29", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "30", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "31", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "32", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "33", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "35", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "37", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "38", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "39", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "40", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "41", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "42", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "44", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "45", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "46", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "47", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "48", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "49", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "50", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "51", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "53", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "54", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "55", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "56", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "60", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "66", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "69", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "72", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {"jurisdiction_id": "78", "jurisdiction_type": "state", "first_year": 2010, "last_year": 2024, "basis": "provisional_none", "evidence": [{"citation": "Researcher decision 2026-10-26: provisional none outside listed policy exceptions; docs/right-to-shelter-evidence.md", "citation_source_type": "secondary", "citation_redistribution": "public"}]},
    {
      "jurisdiction_id": "34",
      "jurisdiction_type": "state",
      "basis": "provisional_none",
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
      "county_fips": ["36005", "36047", "36061", "36081", "36085"],
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
      "basis": "provisional_none",
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
      "basis": "provisional_none",
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
      "basis": "provisional_none",
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
  "unknown_gaps": []
}
```

### `right_to_shelter__exceptions.csv`

```csv
exception_id,jurisdiction_id,jurisdiction_type,effective_date,ended_date,single_adults_strength,single_adults_weather_only,families_strength,families_weather_only,pregnant_people_strength,pregnant_people_weather_only,single_adults_qualification_scope,single_adults_qualification_summary,families_qualification_scope,families_qualification_summary,pregnant_people_qualification_scope,pregnant_people_qualification_summary,citation,citation_source_type,citation_redistribution
nj-code-blue-2017-05-11-to-2020-01-20,34,state,2017-05-11,2020-01-20,none,true,none,true,none,true,subgroup,"County alert plans and warming centers for at-risk people do not establish an individual bed entitlement. No identified year-round protection is provisionally none; weather flag denotes planning policy rather than actual activation or guaranteed placement.",subgroup,"At-risk family members may use weather services; no separate family entitlement is inferred.",subgroup,"At-risk pregnant people may use weather services; no separate pregnancy entitlement is inferred.","https://pub.njleg.gov/bills/2016/PL17/68_.PDF",statute,public
nj-code-blue-threshold-amendment-2020-01-21,34,state,2020-01-21,,none,true,none,true,none,true,subgroup,"Code Blue threshold amended to 32 F or lower regardless of precipitation; wind-chill trigger remains. County compliance and activation are not measured. No identified year-round protection is provisionally none.",subgroup,"At-risk family members may use weather services; no separate family entitlement is inferred.",subgroup,"At-risk pregnant people may use weather services; no separate pregnancy entitlement is inferred.","https://pub.njleg.gov/bills/2016/PL17/68_.PDF ; https://pub.njleg.state.nj.us/Bills/2018/AL19/427_.HTM",statute,public
nyc-court-orders-through-2024-03-14,NYC,sub_state,2008-12-12,2024-03-14,enforceable,false,enforceable,false,enforceable,false,subgroup,"Callahan and Eldredge cover homeless men and women under the decree's class and eligibility terms; not an unconditional right for every individual.",subgroup,"Boston final judgment applies to eligible homeless families with children lacking alternate housing; adult families are not established by this source.",subgroup,"Pregnant women fall within Eldredge's covered class of homeless women subject to the decree's eligibility terms; the decision does not define a separate pregnancy rule or expand protection to every pregnant person regardless of class.","https://www.nycourts.gov/Reporter/3dseries/2009/2009_04302.htm ; https://www.nycourts.gov/courts/AD1/calendar/appsmots/2012/March/2012_03_20_dec.pdf ; https://www.nyc.gov/html/law/downloads/pdf/BOSTON%20FINAL%20JUDGMENT%209%2017%2008%20%20FINAL%20AT%20CITY%20HALL.pdf ; https://www.nycourts.gov/reporter/files/bv/81Misc3d.pdf",court_order,public_domain
nyc-adult-new-arrival-modification-from-2024-03-15,NYC,sub_state,2024-03-15,,none,false,enforceable,false,none,false,unknown,"Court-entered Callahan stipulation on 2024-03-15 sets 30-day initial shelter (60 days for new arrivals under 23) and limits reapplication with exceptions. This affects a subset of single adults, while long-term residents retain Callahan protections; a single categorical strength does not describe both groups safely.",subgroup,"March 2024 stipulation applies only to adults seeking shelter and expressly does not affect families with children.",unknown,"The stipulation applies to adults; its effect on pregnant new arrivals cannot be represented separately without population-specific scope. Pregnancy-only classification remains uncertain.","https://www.coalitionforthehomeless.org/wp-content/uploads/2024/03/3152024Callahan.pdf ; https://www.nyc.gov/mayors-office/news/2024/03/mayor-adams-agreement-the-legal-aid-society-callahan-right-shelter",court_order,public_domain
ny-state-tha-before-code-blue,36,state,1996-12-27,2016-01-04,conditional,false,conditional,false,conditional,false,subgroup,"Eligibility-based Temporary Housing Assistance for individuals and families under 94/96 directives and subsequent 2015 revisions; requires lack of accessible alternative housing and compliance with applicable rules. Operational eligibility changes do not alter this conditional policy-presence classification.",subgroup,"Qualifying families are covered by locally administered THA; eligibility and alternative-housing tests apply.",subgroup,"Pregnant applicants may qualify through individual or family THA eligibility; no distinct pregnancy entitlement is inferred.","https://otda.ny.gov/policy/directives/1994/ADM/94_ADM-20_updated96.pdf ; https://otda.ny.gov/policy/directives/1996/ADM/96_ADM-20.pdf ; https://otda.ny.gov/policy/directives/2015/ADM/15-ADM-06-T.pdf ; https://otda.ny.gov/hearings/transmittals/2015/15-02.pdf",administrative_directive,public
ny-state-tha-and-eo151-to-2016-11-02,36,state,2016-01-05,2016-11-02,conditional,true,conditional,true,conditional,true,subgroup,"Conditional THA continues under 2015 directives. EO151 effective January 5 2016 separately requires statewide protective steps at 32 F or lower including wind chill; weather-policy presence is not observed activation.",subgroup,"Qualifying families retain conditional THA; EO151 protective steps are separate from year-round eligibility.",subgroup,"Pregnant applicants qualify only under individual/family THA eligibility; weather protective steps do not create a separate pregnancy entitlement.","https://otda.ny.gov/policy/directives/2015/ADM/15-ADM-06-T.pdf ; https://dos.ny.gov/january-27-2016vol-xxxviii-issue-4-book-1-2",administrative_directive,public
ny-state-tha-and-eo151-from-2016-11-03,36,state,2016-11-03,2017-03-14,conditional,true,conditional,true,conditional,true,subgroup,"16-ADM-11 consolidates conditional THA policy from November 3 2016; EO151 weather protective steps already apply from January 5 2016.",subgroup,"Temporary housing assistance applies to qualifying individuals and families under assistance eligibility and local district rules.",subgroup,"Pregnant applicants are covered only if eligible under the individual/family temporary-assistance rules; no separate pregnancy entitlement is inferred.","https://otda.ny.gov/policy/directives/2016/ADM/16-ADM-11.pdf ; https://dos.ny.gov/january-27-2016vol-xxxviii-issue-4-book-1-2",administrative_directive,public
ny-state-tha-and-code-blue-from-2017-03-15,36,state,2017-03-15,,conditional,true,conditional,true,conditional,true,subgroup,"OTDA temporary housing assistance is locally administered and eligibility-based; this is a program-level candidate, not an unconditional bed entitlement.",subgroup,"Temporary housing assistance applies to qualifying individuals and families under assistance eligibility and local district rules.",subgroup,"Pregnant applicants may qualify under the individual/family temporary-assistance rules; Code Blue weather measures apply to homeless people generally, subject to the regulation's trigger.","https://otda.ny.gov/policy/directives/2016/ADM/16-ADM-11.pdf ; https://govt.westlaw.com/nyreg/Document/I3d652b8519cd11e7be658bd87eb0c0aa?contextData=%28sc.Default%29&originationContext=documenttoc&transitionType=CategoryPageItem&viewType=FullText ; https://govt.westlaw.com/nycrr/Document/If0386b55f29f11e6848bebc42a960167?bhcp=1&contextData=%28sc.Default%29&transitionType=Default",regulation,public
ma-emergency-assistance-family-pregnancy-1983-10-27,25,state,1983-10-27,2023-10-31,none,false,conditional,false,conditional,false,all,"No single-adult protection is identified in this inventory; provisional none under the approved population-specific baseline.",subgroup,"M.G.L. c. 23B section 30 and EA rules cover qualifying families with dependent children, subject to appropriation and no feasible alternative housing. The 2012 regulation renumbering and eligibility revisions preserve this conditional classification.",subgroup,"EA includes a pregnant woman with no other dependent children, subject to eligibility, alternative-housing and appropriation conditions.","https://archives.lib.state.ma.us/server/api/core/bitstreams/5af96675-c3b5-4131-895b-edb07e7904f5/content ; https://malegislature.gov/Laws/GeneralLaws/PartI/TitleII/Chapter23b/Section30 ; https://www.mass.gov/regulations/760-CMR-6700-eligibility-for-emergency-assistance-ea ; https://www.mass.gov/doc/housing-stabilization-notice-2012-13-cross-references-to-new-regulation-numbers/download",statute,public_domain
ma-ea-capacity-declaration-2023-11-01-to-2024-02-28,25,state,2023-11-01,2024-02-28,none,false,conditional,false,conditional,false,all,"No single-adult protection is identified in this inventory; provisional none under the approved population-specific baseline.",subgroup,"Eligible families remain within EA but capacity declaration creates prioritization and a waitlist at the 7500-family cap; this is not guaranteed immediate placement.",subgroup,"Eligible pregnant women without other dependent children remain within EA subject to the capacity declaration and waitlist. June 2024 stay limits do not affect the January 31 2024 observation.","https://www.mass.gov/doc/declaration-emergency-assistance-family-shelter-1/download ; https://www.mass.gov/orgs/emergency-assistance-ea-family-shelter ; https://malegislature.gov/Laws/GeneralLaws/PartI/TitleII/Chapter23b/Section30",statute,public_domain
dc-severe-weather-duty-2005-to-2011,11,state,2005-10-22,2011-04-07,none,true,none,true,none,true,subgroup,"2005 Act requires severe-weather shelter for homeless people in DC unable to access other shelter: below 32 F including wind chill or above 95 F temperature/heat index. Policy presence is not actual activation. No year-round protection is identified in this inventory.",subgroup,"The weather duty covers homeless families unable to access other shelter; it does not establish year-round protection.",subgroup,"Pregnant people may qualify as individuals/family members under the weather duty; no separate year-round protection is identified.","https://code.dccouncil.gov/us/dc/council/laws/16-35 ; https://code.dccouncil.gov/us/dc/council/laws/docs/16-35.pdf",statute,public_domain
dc-severe-weather-duty-from-2011-04-08,11,state,2011-04-08,,none,true,none,true,none,true,subgroup,"2011 amendment requires appropriate space for homeless District residents unable to access other housing during severe cold or heat. Nonresident access is discretionary and low-barrier exceptions apply; presence is not actual activation. No year-round protection is identified in this inventory.",subgroup,"Resident families unable to access other housing are covered in severe weather; nonresident service is discretionary with low-barrier exceptions. No year-round protection is identified.",subgroup,"Pregnant residents may qualify under individual/family severe-weather criteria; no separate year-round protection is identified.","https://code.dccouncil.gov/us/dc/council/laws/18-367 ; https://code.dccouncil.gov/us/dc/council/laws/docs/18-367.pdf ; https://code.dccouncil.gov/us/dc/council/code/sections/4-753.01",statute,public_domain
hennepin-family-shelter-all-study-window,HENNEPIN,sub_state,2010-01-01,2021-12-31,none,false,conditional,false,none,false,all,"No qualifying countywide single-adult shelter-all protection is identified; provisional none.",subgroup,"County shelter-all policy for eligible families with children is evidenced since 2005. Start date clips that pre-existing policy to the study window and is not an adoption date; a county policy rather than a statutory or court entitlement.",all,"No countywide pregnancy-alone shelter-all guarantee is identified; provisional none. Provider-specific access for pregnant women is not coded as a county guarantee.","https://hennepinmn.legistar.com/LegislationDetail.aspx?GUID=EE81BC82-470D-47B5-AA53-EAA2953C0BDA&ID=6570409&Options=&Search= ; https://www.hennepin.us/government/projects-initiatives/housing-and-homelessness/housing-stability-strategy",administrative_directive,public
hennepin-family-shelter-all-self-pay-removed,HENNEPIN,sub_state,2022-01-01,,none,false,conditional,false,none,false,all,"No qualifying countywide single-adult shelter-all protection is identified; provisional none.",subgroup,"County shelter-all family policy continues; self-pay removed January 1 2022. Overflow expansion in 2022-2023 implements the existing family policy; no individual statutory entitlement is inferred.",all,"No countywide pregnancy-alone shelter-all guarantee is identified; provisional none. Provider-specific access for pregnant women is not coded as a county guarantee.","https://content.govdelivery.com/accounts/MNHENNE/bulletins/3017e5f ; https://hennepinmn.legistar.com/LegislationDetail.aspx?GUID=EE81BC82-470D-47B5-AA53-EAA2953C0BDA&ID=6570409&Options=&Search=",administrative_directive,public
```
