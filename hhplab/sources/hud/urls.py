"""HUD data-product URLs owned by the HUD source package."""

from typing import Final

HUD_USER_PIT_BASE: Final = "https://www.huduser.gov/portal/sites/default/files/xls/"
HUD_USER_HIC_BASE: Final = HUD_USER_PIT_BASE
HUD_USER_HIC_COUNTS_BY_STATE_TEMPLATE: Final = (
    f"{HUD_USER_HIC_BASE}{{year}}-HIC-Counts-by-State.csv"
)

__all__ = [
    "HUD_USER_HIC_BASE",
    "HUD_USER_HIC_COUNTS_BY_STATE_TEMPLATE",
    "HUD_USER_PIT_BASE",
]
