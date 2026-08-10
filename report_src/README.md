# Report Source

Tracked source for the HHP-Lab report in `report.html`.

Run from the repository root:

```bash
uv run python report_src/report_stats.py
uv run --extra analysis python report_src/report_figs.py
uv run --extra analysis python report_src/report_map.py
```

The figure scripts write generated SVGs to ignored output files under
`outputs/report_src/figs/`. The HTML uses relative links to those generated
figures so it can be opened directly from `report_src/report.html` after the
figure scripts run.

The inventory, input/output contract, and retention decision are documented in
[`devdocs/report-source-reproducibility.md`](../devdocs/report-source-reproducibility.md).
