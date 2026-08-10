# Report-source reproducibility

This note resolves bead `coclab-xfy3f`. The three files under `report_src/`
are report-specific assembly and audit entrypoints, not duplicate result
workflows. They should remain tracked because `report.html` embeds their
figures and the scripts provide a reproducible audit of the published
headline statistics.

## Inventory

| Script | Inputs | Tracked/created outputs | Decision |
| --- | --- | --- | --- |
| `report_src/report_stats.py` | `outputs/top50_msa_untangle_A2024.parquet`, `outputs/top50_msa_fd_2015_2025.parquet`, `outputs/top50_msa_longitudinal_2010_2025.parquet`, `outputs/tot_longdiff.parquet`, plus the long-difference inputs resolved by the sanctuary workflow | Labeled stdout only | Retain as a report QA/audit script; no package CLI replacement is equivalent. |
| `report_src/report_figs.py` | The top-50 cross-section, longitudinal, and long-difference artifacts above; `build_longdiff_inputs()` from the sanctuary workflow | `outputs/report_src/figs/fig1_capacity.svg` through `fig5_sanctuary.svg` | Retain as the report figure renderer; it is intentionally report-specific. |
| `report_src/report_map.py` | `data/curated/tiger/counties__C2023.parquet`, `data/curated/coc_boundaries/coc__B2024.parquet` | `outputs/report_src/figs/fig0_msa_coc_map.svg` | Retain as the static print-map renderer; the package interactive map is not an equivalent SVG artifact. |

`report_src/report.html` consumes `fig0_msa_coc_map.svg` and `fig1`–`fig5`
alongside the curated panel artifacts. The report README remains the canonical
runbook and requires execution from the repository root because the scripts
use repository-relative inputs and outputs.

## Reuse assessment

The scripts already reuse package analysis code where the result is a shared
scientific computation (`build_longdiff_inputs`). Their remaining logic is
presentation-specific: headline correlations, coefficient annotations,
fixed report labels, figure dimensions, colors, and the Los Angeles CoC map
extent. Generalizing those details into package APIs would add a second
parameter surface without a current consumer.

The package result CLI remains the replacement for producing the underlying
analysis artifacts, for example:

```bash
HHPLAB_NON_INTERACTIVE=1 uv run hhplab build result sanctuary-longdiff-robustness --json
```

That command does not replace the report scripts because it does not promise
the report's five SVGs, static map, or exact headline-audit printout. If a
future report needs reusable chart or map specifications, promote that
specific requirement into a package workflow first; do not silently delete
these entrypoints.

## Retention policy

Keep the three scripts, `report.html`, and this inventory through the report's
reproduction window. Revisit generalization or deprecation after 2026-12-31,
only after checking the report's cited outputs and any external replication
instructions. Generated SVGs and other `outputs/` files remain derived and
ignored; the scripts and HTML are the durable source artifacts.
