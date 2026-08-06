# Agent Instructions

This project uses `.beads/` for issue tracking. Two CLI tools can manage beads: **br** (beads_rust, preferred) and **bd** (beads Python). At session start, detect which is available:

```bash
command -v br && BR=br || { command -v bd && BR=bd || echo "No beads CLI found"; }
```

Use `$BR` (or just the resolved command) for all beads operations below. Prefer **br** when both are installed.

**Note:** `br` is non-invasive and never executes git commands. After `br sync --flush-only`, you must manually run `git add .beads/ && git commit`. The `bd sync` command commits and pushes automatically.

**Repository rename note:** the repository/runtime name is now HHP-Lab/`hhplab`. Historical references to CoC-Lab may still appear in the beads database and JSONL exports, along with historical files in `devdocs/`. Keep those historical artifacts as-is unless a task explicitly requires migration.

**Bead namespace note:** even though the project/runtime name is now HHP-Lab/`hhplab`, bead IDs intentionally remain in the historical `coclab-*` namespace. Do not migrate existing bead slugs and do not switch new issues to an `hhplab-*` prefix.

**Reproducibility:** the project/runtime will be used in the future for replication of any reported results.  It is acceptable to write one-off scripts to test ideas, but scripts that result in some kind of learning should be noted in a bead so that they are made accessible through the CLI and results can be easily checked and the code tracked by version control.

****

## Quick Reference

| Action | br (preferred) | bd (fallback) |
|--------|---------------|---------------|
| Onboard | `br onboard` | — |
| List issues | `br list` | `bd list --status=open` |
| Find ready work | `br ready` | `bd ready` |
| Show issue | `br show <id>` | `bd show <id>` |
| Create issue | `br create` | `bd create --title="..." --type=task --priority=2` |
| Claim work | `br update <id> --status in_progress` | `bd update <id> --status=in_progress` |
| Close issue | `br close <id>` | `bd close <id> --reason="Completed"` |
| Sync to disk | `br sync --flush-only` | `bd sync` |

## HHP-Lab Agent-Friendly CLI

Prefer these HHP-Lab runtime features when automating:

- Use machine-readable output whenever available (`--json`) to avoid parsing human text.
- Run non-interactively for CI/agents: pass `--non-interactive` or set `HHPLAB_NON_INTERACTIVE=1`.
- Use `hhplab agents` for built-in geography/year matching rules and operational guidance.
- Preflight environment and prerequisites with:
  - `hhplab status --json`
- Validate curated naming/layout policy before and after writes:
  - `hhplab validate curated-layout`
- For curated filename migrations, default to dry-run first:
  - `hhplab migrate curated-layout`
  - `hhplab migrate curated-layout --apply`
- For recipe workflows, validate/plan before execute:
  - `hhplab build recipe-preflight --recipe <file> --json`
  - `hhplab build recipe --recipe <file> --json`
  - Use `hhplab build recipe-plan --recipe <file> --json` when you need the resolved task graph while authoring/debugging a recipe

## Measure Discovery

Before calling external APIs or writing one-off scripts, determine whether the package already supports the measure:

1. Check the package registries first:
   - ACS5 tract-derived measures: `hhplab/sources/census/acs/variables.py` (`ACS5_COVARIATE_REGISTRY`), or `hhplab list acs-variables`.
   - ACS1 metro/county-native measures: `hhplab/sources/census/acs/variables_acs1.py` (`DERIVED_ACS1_MEASURES` and `ACS1_*_MEASURE_COLUMNS`), or `hhplab list acs-variables`.
   - External covariate sources: `hhplab list covariates`, backed by `hhplab/covariates/catalog.py`.
2. Treat curated files on disk as possibly stale. Absence of a column in an existing parquet does **not** mean the package lacks support; re-ingest with `--force` to refresh the schema before concluding support is absent.
3. If a measure exists in a registry, use the package ingest, aggregate, and analyze pipeline. Only fall back to external fetching when the measure is genuinely absent, and then file a bead to add registry support rather than leaving a one-off script.

## Code Style: Human and Agent Readable

All code generated for this project must be easily usable by both humans and AI agents. Apply these principles when writing code, tests, CLI commands, and data pipelines:

### Tests and fixtures
- **Declarative over implicit.** Define fixture data and expected outcomes as named constants, not as magic numbers buried in logic. An agent (or human) modifying a fixture value should not need to hand-recompute downstream expectations.
- **Parametrize over loops.** Use `@pytest.mark.parametrize` instead of for-loops inside test bodies. Failures should name the exact case in the test ID (e.g., `test_dtype[year-int64]`), not require reading assertion messages.
- **Truth tables over prose.** When a fixture has designed-in outcomes (e.g., which rows pass a threshold), document the full truth table in a visible location (module docstring or a constant), not scattered across individual test docstrings.
- **Allowlists and exceptions at module level.** If something is a known special case (e.g., columns with expected dtype normalization), declare it as a named module-level constant with a comment explaining why — not as a local variable inside a function.
- **Derive, don't duplicate.** Golden-value tests should compute expectations from the same constants that build the fixtures. Changing a fixture value should automatically update the expected outcome.

### CLI and output
- **Always provide `--json`.** Every CLI command that produces output should support a `--json` flag emitting structured, machine-parseable JSON. Agents should never need to scrape human-formatted tables or prose to extract results.
- **Actionable error messages.** Errors should state what went wrong AND what to do about it (e.g., "No ACS measures found — run `hhplab aggregate acs` first"). An agent that encounters an error should be able to act on it without searching the codebase.
- **Deterministic, parseable file names.** Output artifacts should use the canonical naming from `hhplab/naming.py` so agents can discover and reference them programmatically without globbing.

### Schemas and data contracts
- **Canonical column lists as code.** Output schemas (e.g., `PANEL_COLUMNS`, `ZORI_COLUMNS`) must be defined as module-level constants. When a schema changes, update the constant — never add columns silently.
- **Provenance in every artifact.** Parquet outputs must embed provenance metadata via `write_parquet_with_provenance` so downstream agents can inspect lineage without external tracking.

## Dataset Availability & Geometry Rules

When authoring recipes, selecting year ranges, or debugging missing data, use these constraints. A year outside a source's coverage window means the data **does not exist** — it is not a build failure.

### Temporal coverage

Use `hhplab list sources --json` for machine-readable temporal coverage of core panel sources (PIT, HIC, ACS5, ACS1, PEP, ZORI). Use `hhplab list covariates --json` for external covariate source coverage such as IRS SOI migration.

- **ACS lag rule:** ACS vintage for PIT year Y is Y−1 (released ~Dec of year Y−1).
- **PEP coverage:** Postcensal vintage 2020 covers 2010-2020; vintage 2024 covers 2020-2024. Combined/intercensal fills the full 2010-2020 range.

### Census tract geometry eras

Tract-based data (ACS, crosswalks) must reference the correct decennial tract vintage. Tracts are redefined each decennial census:

| Data years | Tract vintage | Example |
|-----------|---------------|---------|
| 2000–2009 | 2000 | ACS 2009 uses 2000-era tracts |
| 2010–2019 | 2010 | ACS 2018 uses 2010-era tracts |
| 2020–2029 | 2020 | ACS 2023 uses 2020-era tracts |

**Rule:** use the most recent decennial ≤ the data year. In recipes, this drives the `segments` section of `file_set` specs — each segment maps a year range to its tract vintage.

Cross-era analysis (e.g., a 2015-2024 panel) requires a tract relationship file (2010↔2020) and separate crosswalk builds per era.

### Measure columns by data source

Different data sources produce different demographic columns. Conformance checks use `PanelRequest.measure_columns` to validate the right set:

| Source | Measure columns in panel | Notes |
|--------|-------------------------|-------|
| ACS | `total_population`, `adult_population`, `population_below_poverty`, `median_household_income`, `median_gross_rent` | Tract-level, apportioned via crosswalk |
| PEP | `population` | County-level, aggregated to target geography |

When building non-ACS panels (e.g., PEP-based metro), set `measure_columns` on `PanelRequest` so conformance checks validate the correct columns instead of defaulting to the ACS set.

## Adding Beads (Problem Noticed)

If you identify a problem in the code, even incidentally while working on something else, add a bead to make sure it is addressed later.

<!-- bv-agent-instructions-v2 -->

---

## Beads Workflow Integration

This project uses [beads_rust](https://github.com/Dicklesworthstone/beads_rust) (`br`) for issue tracking and [beads_viewer](https://github.com/Dicklesworthstone/beads_viewer) (`bv`) for graph-aware triage. Issues are stored in `.beads/` and tracked in git.

### Using bv as an AI sidecar

bv is a graph-aware triage engine for Beads projects (.beads/beads.jsonl). Instead of parsing JSONL or hallucinating graph traversal, use robot flags for deterministic, dependency-aware outputs with precomputed metrics (PageRank, betweenness, critical path, cycles, HITS, eigenvector, k-core).

**Scope boundary:** bv handles *what to work on* (triage, priority, planning). `br` handles creating, modifying, and closing beads.

**CRITICAL: Use ONLY --robot-* flags. Bare bv launches an interactive TUI that blocks your session.**

#### The Workflow: Start With Triage

**`bv --robot-triage` is your single entry point.** It returns everything you need in one call:
- `quick_ref`: at-a-glance counts + top 3 picks
- `recommendations`: ranked actionable items with scores, reasons, unblock info
- `quick_wins`: low-effort high-impact items
- `blockers_to_clear`: items that unblock the most downstream work
- `project_health`: status/type/priority distributions, graph metrics
- `commands`: copy-paste shell commands for next steps

```bash
bv --robot-triage        # THE MEGA-COMMAND: start here
bv --robot-next          # Minimal: just the single top pick + claim command

# Token-optimized output (TOON) for lower LLM context usage:
bv --robot-triage --format toon
```

#### Other bv Commands

| Command | Returns |
|---------|---------|
| `--robot-plan` | Parallel execution tracks with unblocks lists |
| `--robot-priority` | Priority misalignment detection with confidence |
| `--robot-insights` | Full metrics: PageRank, betweenness, HITS, eigenvector, critical path, cycles, k-core |
| `--robot-alerts` | Stale issues, blocking cascades, priority mismatches |
| `--robot-suggest` | Hygiene: duplicates, missing deps, label suggestions, cycle breaks |
| `--robot-diff --diff-since <ref>` | Changes since ref: new/closed/modified issues |
| `--robot-graph [--graph-format=json\|dot\|mermaid]` | Dependency graph export |

#### Scoping & Filtering

```bash
bv --robot-plan --label backend              # Scope to label's subgraph
bv --robot-insights --as-of HEAD~30          # Historical point-in-time
bv --recipe actionable --robot-plan          # Pre-filter: ready to work (no blockers)
bv --recipe high-impact --robot-triage       # Pre-filter: top PageRank scores
```

### br Commands for Issue Management

```bash
br ready              # Show issues ready to work (no blockers)
br list --status=open # All open issues
br show <id>          # Full issue details with dependencies
br create --title="..." --type=task --priority=2
br update <id> --status=in_progress
br close <id> --reason="Completed"
br close <id1> <id2>  # Close multiple issues at once
br sync --flush-only  # Export DB to JSONL
```

### Workflow Pattern

1. **Triage**: Run `bv --robot-triage` to find the highest-impact actionable work
2. **Claim**: Use `br update <id> --status=in_progress`
3. **Work**: Implement the task
4. **Complete**: Use `br close <id>`
5. **Sync**: Always run `br sync --flush-only` at session end

### Key Concepts

- **Dependencies**: Issues can block other issues. `br ready` shows only unblocked work.
- **Priority**: P0=critical, P1=high, P2=medium, P3=low, P4=backlog (use numbers 0-4, not words)
- **Types**: task, bug, feature, epic, chore, docs, question
- **Blocking**: `br dep add <issue> <depends-on>` to add dependencies

### Session Protocol

```bash
git status              # Check what changed
git add <files>         # Stage code changes
br sync --flush-only    # Export beads changes to JSONL
git commit -m "..."     # Commit everything
git push                # Push to remote
```

<!-- end-bv-agent-instructions -->

<!-- goals-agent-instructions-v1 -->

---

## Goals Workflow Integration

This project uses goals-rust to preserve functional intent, append-only result
evidence, and evidence-backed fulfillment assessments across agent sessions.

`goals-rust` is the command-line provider. Agents should prefer its read-only
`--robot-*` facade, discovered with `goals-rust --robot-help`; use `--format json`
or `--format toon` as needed. Authoring and other ordinary commands use `--output
json`. Both interfaces preserve versioned response envelopes, stable operation
IDs, errors, and bounded collections.

**Scope boundary:** Goals owns durable Goals, positive and negative intent,
acceptance criteria, native Goal relations, evidence history, applicability, and
derived fulfillment assessment.

`br` owns work requests, priority, assignment, dependencies, readiness, and closure.
`elements-rust` owns repository architecture and exact implementation-change
provenance. Goals must not infer either system's facts from paths, Git state, names,
or completion labels.

Goal evidence may preserve typed `work_provenance` to identify Beads issues whose
execution produced observations or authored the evidence. This is a
non-authoritative evidence-to-work association: it never imports Bead status,
creates a Goal relation, or contributes to fulfillment.

### What goals-rust provides

`goals-rust` maintains a local SQLite authority under `.goals/` and exposes
deterministic progressive queries for understanding intended behavior, constraints,
prior approaches, evidence, relationships, and remaining fulfillment gaps. It keeps
declared intent, observed evidence, applicability, and derived assessment separate;
later success never erases an unsuccessful, stale, corrected, invalidated, or
superseded attempt.

A typical agent workflow is:

1. Discover the read-only agent surface with `goals-rust --robot-help` and broader
   provider capabilities with `goals-rust --output json capabilities`.
2. Orient with `--robot-summary` or task-focused `--robot-search`, then inspect a
   relevant Goal with `--robot-detail` and `--robot-gaps`.
3. Review its declared constraints, criteria, evidence ledger, prior approaches,
   and native relations before proposing or evaluating work.
4. Derive a current assessment with explicit context using `assess`; add
   `--persist` only when intentionally updating the rebuildable projection.
5. When authorized to record results, append canonical evidence with identified
   producer, exact applicable Element changes, and typed managed-work provenance
   when available. Use action-specific amendment commands instead of rewriting
   history.

Treat `.goals/goals.db` as the local authority. `.goals/goals.jsonl` is a canonical
interchange snapshot only after an explicit export. Do not hand-edit the database,
copy it without its live WAL state, overwrite interchange data without the required
explicit flag, or assume that a read command refreshes an external provider.

### Agent command quick reference

- Discover robot routes with `goals-rust --robot-help`; discover broader operations
  and schemas with `goals-rust --output json capabilities`, `schema list`, and
  `schema show <SCHEMA_ID>`.
- Orient progressively with `--robot-summary`, `--robot-search <TEXT>`,
  `--robot-detail <GOAL_ID>`, and `--robot-gaps <GOAL_ID>`. Robot output defaults
  to JSON and accepts `--format toon`.
- Inspect intent and history with `intent list`, `criterion list`, `evidence list`,
  `evidence show`, `query negative-intent`, and `query prior-approaches`.
- Inspect the native graph with `relation list`, `relation tree`, and `query
  relations`; Goals relations are not Bead dependencies or arbitrary cross-domain
  links.
- Derive fulfillment with `assess <GOAL_ID> --context-json <JSON>`. Assessment is
  read-only unless `--persist` is explicit, and omitted context remains unknown.
- Inspect recorded Elements contributions with `query element-changes`; request
  external observation only with the explicit `--resolve --project-id <PROJECT_ID>`
  pair or standalone `reference resolve`.
- Author Goals, intent, criteria, and relations with canonical JSON input and use
  `--dry-run` where supported before applying a mutation.
- Append observations with `evidence append`; preserve history with `evidence
  correct|invalidate|mark-stale|supersede` rather than changing an earlier record.
- Review interchange with `interchange validate` and dry-run `interchange import`.
  Export, `import --apply`, and rebuild are explicit writes and never invoke Git.
- Keep reads bounded with `--limit` and opaque `--continuation` cursors. Branch on
  `ok`, `operation`, `error.code`, and advertised capabilities, never human prose or
  the binary version.

<!-- end-goals-agent-instructions -->

<!-- elr-agent-instructions-v1 -->

---

## Elements Workflow Integration

`elr` is the installed command alias for `elements-rust`; both names invoke the same architecture engine. Examples below use `elements-rust` for portability.
Goals owns desired functionality, Elements owns reviewed architecture and structural facts, and Beads owns realization work.
Use `rg` or repository-native search to select an exact ID, path, or symbol before asking Elements for structure or impact:

```bash
elements-rust node <ID_OR_PATH>
elements-rust impact
elements-rust scaffold --repo .
```

Task-prose interpretation and cross-provider orientation belong to the agent or Rosetta; Elements answers deterministic structural questions about explicit anchors.
Rosetta reads Goals, Elements, and Beads without becoming their authority.
Treat `.elements/elements.scaffold.jsonl` as generated facts and `.elements/elements.registry.jsonl` as persistent identity; never hand-edit either file.

<!-- end-elr-agent-instructions -->

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
   - Full test suite command: `uv run --extra dev pytest` (requires dev extras such as `pytest-httpx`)
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   # If using br:
   br sync --flush-only
   git add .beads/
   git commit -m "sync beads"
   # If using bd:
   # bd sync              # (commits and pushes automatically)
   git push
   git status  # MUST show "up to date with origin"
  ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

## Key Concepts

- **Dependencies**: Issues can block other issues. `ready` shows only unblocked work.
- **Priority**: P0=critical, P1=high, P2=medium, P3=low, P4=backlog (use numbers, not words)
- **Types**: task, bug, feature, epic, question, docs
