"""Generate the CoC-to-MSA rollup illustration map (Los Angeles) as SVG.

Uses the package's own curated boundary artifacts (CoC boundaries, TIGER
county boundaries, MSA-county membership) -- the same geometries
hhplab/viz/map_folium.py's interactive renderer draws from -- but plots them
statically with geopandas/matplotlib for print embedding in the PDF report.
"""
# ruff: noqa: E402, I001
import os
import geopandas as gpd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

FIGDIR = os.path.join("outputs", "report_src", "figs")
os.makedirs(FIGDIR, exist_ok=True)

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
SECONDARY = "#52514e"
MUTED = "#898781"

LA_MSA_ID = "31080"
LA_COUNTIES = ["06037", "06059"]  # Los Angeles County, Orange County
# All allocation_share == 1.0 (fully contained) CoCs for MSA 31080 per
# data/curated/xwalks/msa_coc_xwalk__B2024xMcensus_msa_2023xC2023.parquet;
# a handful of additional rows in that file are boundary-touching slivers
# with allocation_share ~1e-10 and are not meaningfully part of the MSA.
LA_COC_IDS = ["CA-600", "CA-602", "CA-606", "CA-607", "CA-612"]
COC_COLORS = {
    "CA-600": "#2a78d6",  # Los Angeles City & County CoC
    "CA-602": "#e8823c",  # Santa Ana, Anaheim/Orange County CoC
    "CA-606": "#1baf7a",  # Long Beach CoC
    "CA-607": "#b56576",  # Pasadena CoC
    "CA-612": "#8d6ab8",  # Glendale CoC
}
COC_SHORT = {
    "CA-600": "Los Angeles City & County",
    "CA-602": "Santa Ana / Anaheim (Orange Co.)",
    "CA-606": "Long Beach",
    "CA-607": "Pasadena",
    "CA-612": "Glendale",
}

CRS_PROJECTED = "EPSG:3310"  # NAD83 California Albers, low distortion locally

county = gpd.read_parquet("data/curated/tiger/counties__C2023.parquet")
county = county[(county.geo_vintage == "2023") & (county.geoid.isin(LA_COUNTIES))]
county = county.to_crs(CRS_PROJECTED)

coc = gpd.read_parquet("data/curated/coc_boundaries/coc__B2024.parquet")
coc = coc[coc.coc_id.isin(LA_COC_IDS)].to_crs(CRS_PROJECTED)

msa_outline = county.dissolve()

fig, ax = plt.subplots(figsize=(6.4, 4.6))
msa_outline.boundary.plot(ax=ax, color=INK, linewidth=2.6, zorder=4)
county.boundary.plot(ax=ax, color=SECONDARY, linewidth=1.1, linestyle=(0, (5, 3)), zorder=3)
for _, row in coc.iterrows():
    gpd.GeoSeries([row.geometry], crs=CRS_PROJECTED).plot(
        ax=ax, color=COC_COLORS[row.coc_id], alpha=0.55,
        edgecolor=COC_COLORS[row.coc_id], linewidth=0.8, zorder=2,
    )

# Crop out Santa Catalina Island (part of LA County, ~20 miles offshore) so
# the mainland CoC mosaic -- the point of this figure -- isn't squeezed.
ax.set_xlim(96000, 241000)
ax.set_ylim(-460000, -352000)
ax.set_aspect("equal")
ax.set_axis_off()
ax.set_title(
    "One MSA, two counties, five CoCs: Los Angeles-Long Beach-Anaheim",
    fontsize=11.5, color=INK, pad=10,
)
ax.annotate(
    "Also includes Santa Catalina Island (LA Co.), off-map to the south",
    xy=(0.02, 0.02), xycoords="axes fraction", fontsize=7, color=MUTED,
)

coc_legend = [
    Patch(facecolor=COC_COLORS[cid], edgecolor=COC_COLORS[cid], alpha=0.55,
          label=f"{COC_SHORT[cid]} ({cid})")
    for cid in LA_COC_IDS
]
boundary_legend = [
    Line2D([0], [0], color=INK, lw=2.6, label="MSA boundary (LA + Orange Co.)"),
    Line2D([0], [0], color=SECONDARY, lw=1.1, linestyle=(0, (5, 3)),
           label="County line"),
]
leg = fig.legend(
    handles=coc_legend + boundary_legend, loc="lower center",
    fontsize=8, frameon=False, ncol=2, bbox_to_anchor=(0.5, -0.02),
)
for text in leg.get_texts():
    text.set_color(SECONDARY)

fig.tight_layout(rect=(0, 0.12, 1, 1))
fig.savefig(f"{FIGDIR}/fig0_msa_coc_map.svg", facecolor=SURFACE, bbox_inches="tight")
plt.close(fig)
print("map written to", f"{FIGDIR}/fig0_msa_coc_map.svg")
