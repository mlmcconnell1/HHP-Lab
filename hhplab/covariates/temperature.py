"""Pure temperature-derived covariate transformations."""

from __future__ import annotations

import pandas as pd

FREEZING_C = 0.0
EMERGENCY_SHELTER_ACTIVATION_C = 4.4


def derive_prism_temperature_basis(df: pd.DataFrame) -> pd.DataFrame:
    """Add policy-threshold PRISM tmin basis columns when ``tmin_c`` is present."""
    if "tmin_c" not in df.columns:
        return df
    result = df.copy()
    tmin = pd.to_numeric(result["tmin_c"], errors="coerce")
    result["tmin_below_freezing"] = tmin.clip(upper=FREEZING_C)
    result["tmin_code_blue_band"] = tmin.clip(
        lower=FREEZING_C,
        upper=EMERGENCY_SHELTER_ACTIVATION_C,
    )
    result["tmin_above_code_blue"] = (tmin - EMERGENCY_SHELTER_ACTIVATION_C).clip(lower=0.0)
    return result
