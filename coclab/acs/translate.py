"""Tract vintage translation for ACS data.

Translates ACS data from 2010 census tract geography to 2020 census tract
geography using the Census Bureau tract relationship file.

Background
----------
ACS 5-year estimates use tract geographies aligned to the most recent
decennial census at or before the ACS end year:
- ACS ending 2010-2019: Uses 2010 census tract geography
- ACS ending 2020-2029: Uses 2020 census tract geography

The tract relationship file provides the mapping between 2010 and 2020 tracts,
including area-based weights for proportional allocation when tracts split
or merge.

Interpolation Logic
-------------------
For a 2010 tract that splits into multiple 2020 tracts:
    pop_in_2020_tract = pop_2010 * area_2010_to_2020_weight

For multiple 2010 tracts merging into one 2020 tract:
    pop_in_2020_tract = sum(pop_2010_i * weight_i for each source tract)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

from coclab.census.ingest.tract_relationship import load_tract_relationship

logger = logging.getLogger(__name__)


def _parse_acs_end_year(acs_vintage: str) -> int:
    """Parse the ACS end year from a vintage string."""
    if "-" in acs_vintage:
        return int(acs_vintage.split("-")[1])
    return int(acs_vintage)


def default_tract_vintage_for_acs(acs_vintage: str) -> int:
    """Return the default tract vintage for an ACS vintage.

    The default is the most recent decennial census year at or before the ACS end year.
    """
    end_year = _parse_acs_end_year(acs_vintage)
    return end_year - (end_year % 10)


def get_source_tract_vintage(acs_vintage: str) -> int:
    """Determine the census tract geography used by an ACS vintage.

    Parameters
    ----------
    acs_vintage : str
        ACS vintage string like "2019-2023" or "2023".

    Returns
    -------
    int
        The census tract vintage year (decennial, e.g., 2010 or 2020).

    Examples
    --------
    >>> get_source_tract_vintage("2015-2019")
    2010
    >>> get_source_tract_vintage("2019-2023")
    2020
    >>> get_source_tract_vintage("2026-2030")
    2030
    >>> get_source_tract_vintage("2019")
    2010
    """
    return default_tract_vintage_for_acs(acs_vintage)


def needs_translation(acs_vintage: str, target_tract_vintage: int | str) -> bool:
    """Check if ACS data needs tract geography translation.

    Parameters
    ----------
    acs_vintage : str
        ACS vintage string like "2019-2023" or "2015-2019".
    target_tract_vintage : int or str
        Target tract vintage year (e.g., 2023 or "2023").

    Returns
    -------
    bool
        True if translation is needed from 2010 to 2020 geography.

    Examples
    --------
    >>> needs_translation("2015-2019", 2023)
    True
    >>> needs_translation("2019-2023", 2023)
    False
    """
    source_vintage = get_source_tract_vintage(acs_vintage)
    target_year = int(target_tract_vintage)

    # Target vintage 2020+ uses 2020 geography
    target_is_2020_based = target_year >= 2020

    # Need translation if source is 2010-based and target is 2020-based
    return source_vintage == 2010 and target_is_2020_based


@dataclass
class TranslationStats:
    """Statistics from a tract translation operation.

    Attributes
    ----------
    input_tracts : int
        Number of unique 2010 tracts in input data.
    output_tracts : int
        Number of unique 2020 tracts in output data.
    matched_tracts : int
        Number of 2010 tracts that matched the relationship file.
    unmatched_tracts : int
        Number of 2010 tracts with no mapping (dropped).
    match_rate : float
        Fraction of input tracts that matched (0.0 to 1.0).
    input_population : int
        Total population before translation.
    output_population : int
        Total population after translation (should be close to input).
    population_delta_pct : float
        Percentage change in total population.
    """

    input_tracts: int
    output_tracts: int
    matched_tracts: int
    unmatched_tracts: int
    match_rate: float
    input_population: int
    output_population: int
    population_delta_pct: float

    def __str__(self) -> str:
        return (
            f"TranslationStats(\n"
            f"  input_tracts={self.input_tracts:,}, "
            f"output_tracts={self.output_tracts:,}\n"
            f"  matched={self.matched_tracts:,} ({self.match_rate:.1%}), "
            f"unmatched={self.unmatched_tracts:,}\n"
            f"  pop_in={self.input_population:,}, "
            f"pop_out={self.output_population:,} "
            f"(delta={self.population_delta_pct:+.2f}%)\n"
            f")"
        )


def translate_tracts_2010_to_2020(
    df: pd.DataFrame,
    population_column: str = "total_population",
    geoid_column: str = "tract_geoid",
) -> tuple[pd.DataFrame, TranslationStats]:
    """Translate tract data from 2010 to 2020 census geography.

    Uses area-based interpolation weights from the Census Bureau tract
    relationship file to proportionally allocate data when tracts split
    or merge between census decades.

    Parameters
    ----------
    df : pd.DataFrame
        Input data with 2010 tract GEOIDs and population values.
    population_column : str
        Name of the column containing population counts (default: "total_population").
    geoid_column : str
        Name of the column containing tract GEOIDs (default: "tract_geoid").

    Returns
    -------
    tuple[pd.DataFrame, TranslationStats]
        Translated DataFrame with 2020 tract GEOIDs and translation statistics.

    Raises
    ------
    TractRelationshipNotFoundError
        If the tract relationship file has not been ingested.
    ValueError
        If required columns are missing from input DataFrame.

    Notes
    -----
    The translation preserves total population by distributing each 2010
    tract's population proportionally to 2020 tracts based on land area
    overlap.

    For margin of error columns (containing "moe_" prefix), the translation
    uses the square root of sum of squared weighted values to properly
    propagate uncertainty.

    Example
    -------
    >>> df = pd.DataFrame({
    ...     "tract_geoid": ["01001020100", "01001020200"],
    ...     "total_population": [1000, 2000]
    ... })
    >>> translated, stats = translate_tracts_2010_to_2020(df)
    >>> print(stats.match_rate)
    1.0
    """
    # Validate input
    if geoid_column not in df.columns:
        raise ValueError(f"Missing required column: {geoid_column}")
    if population_column not in df.columns:
        raise ValueError(f"Missing required column: {population_column}")

    # Capture input stats
    input_tracts = df[geoid_column].nunique()
    input_population = int(df[population_column].sum())

    logger.info(
        f"Translating {input_tracts:,} tracts from 2010 to 2020 geography "
        f"(population: {input_population:,})"
    )

    # Load relationship file
    rel = load_tract_relationship()

    # Join input data with relationship file on 2010 GEOID
    merged = df.merge(
        rel,
        left_on=geoid_column,
        right_on="tract_geoid_2010",
        how="left",
    )

    # Count matches
    matched_mask = merged["tract_geoid_2020"].notna()
    matched_tracts = df.loc[
        df[geoid_column].isin(merged.loc[matched_mask, geoid_column])
    ][geoid_column].nunique()
    unmatched_tracts = input_tracts - matched_tracts

    if unmatched_tracts > 0:
        # Get example unmatched GEOIDs for logging
        unmatched_geoids = df.loc[
            ~df[geoid_column].isin(merged.loc[matched_mask, geoid_column])
        ][geoid_column].unique()[:5]
        logger.warning(
            f"{unmatched_tracts:,} tracts ({unmatched_tracts / input_tracts:.1%}) "
            f"have no mapping in relationship file. Examples: {list(unmatched_geoids)}"
        )

    # Drop unmatched rows (they have no 2020 mapping)
    translated = merged[matched_mask].copy()

    # Apply area weights to population
    translated[population_column] = (
        translated[population_column] * translated["area_2010_to_2020_weight"]
    )

    # Handle margin of error columns with proper error propagation
    # For weighted sums: MOE = sqrt(sum(weight^2 * moe^2))
    moe_columns = [col for col in df.columns if col.startswith("moe_")]
    for moe_col in moe_columns:
        if moe_col in translated.columns:
            # Store weighted squared MOE for later aggregation
            translated[f"__{moe_col}_weighted_sq"] = (
                translated["area_2010_to_2020_weight"] ** 2
                * translated[moe_col].fillna(0) ** 2
            )

    # Aggregate by 2020 tract GEOID
    agg_funcs = {
        population_column: "sum",
    }

    # Add MOE aggregation (sum of squared weighted values, then sqrt)
    for moe_col in moe_columns:
        sq_col = f"__{moe_col}_weighted_sq"
        if sq_col in translated.columns:
            agg_funcs[sq_col] = "sum"

    # Preserve metadata columns (take first value since they're the same)
    metadata_cols = [
        col
        for col in df.columns
        if col not in [geoid_column, population_column] + moe_columns
    ]
    for col in metadata_cols:
        if col in translated.columns:
            agg_funcs[col] = "first"

    # Group by 2020 GEOID and aggregate
    result = translated.groupby("tract_geoid_2020", as_index=False).agg(agg_funcs)

    # Compute final MOE from squared sums
    for moe_col in moe_columns:
        sq_col = f"__{moe_col}_weighted_sq"
        if sq_col in result.columns:
            result[moe_col] = result[sq_col].pow(0.5)
            result = result.drop(columns=[sq_col])

    # Rename GEOID column to match input schema
    result = result.rename(columns={"tract_geoid_2020": geoid_column})

    # Reorder columns to match input (with new GEOIDs)
    output_cols = [col for col in df.columns if col in result.columns]
    result = result[output_cols]

    # Capture output stats
    output_tracts = result[geoid_column].nunique()
    output_population = int(result[population_column].sum())

    if input_population > 0:
        population_delta_pct = (
            (output_population - input_population) / input_population * 100
        )
    else:
        population_delta_pct = 0.0

    stats = TranslationStats(
        input_tracts=input_tracts,
        output_tracts=output_tracts,
        matched_tracts=matched_tracts,
        unmatched_tracts=unmatched_tracts,
        match_rate=matched_tracts / input_tracts if input_tracts > 0 else 0.0,
        input_population=input_population,
        output_population=output_population,
        population_delta_pct=population_delta_pct,
    )

    logger.info(
        f"Translation complete: {output_tracts:,} output tracts, "
        f"population delta: {population_delta_pct:+.2f}%"
    )

    return result, stats


def translate_acs_to_target_vintage(
    df: pd.DataFrame,
    acs_vintage: str,
    target_tract_vintage: int | str,
    population_column: str = "total_population",
    geoid_column: str = "tract_geoid",
) -> tuple[pd.DataFrame, TranslationStats | None]:
    """Translate ACS data to target tract vintage if needed.

    This is a convenience function that checks if translation is needed
    and performs it if so. If no translation is needed, returns the
    original DataFrame unchanged.

    Parameters
    ----------
    df : pd.DataFrame
        ACS tract population data.
    acs_vintage : str
        ACS vintage string like "2015-2019".
    target_tract_vintage : int or str
        Target tract vintage year (e.g., 2023).
    population_column : str
        Column containing population values.
    geoid_column : str
        Column containing tract GEOIDs.

    Returns
    -------
    tuple[pd.DataFrame, TranslationStats | None]
        Translated DataFrame and statistics. If no translation was needed,
        returns original DataFrame and None.

    Examples
    --------
    >>> # ACS 2015-2019 uses 2010 geography, needs translation to 2023
    >>> translated, stats = translate_acs_to_target_vintage(
    ...     df, "2015-2019", 2023
    ... )
    >>> if stats:
    ...     print(f"Translated {stats.input_tracts} -> {stats.output_tracts} tracts")

    >>> # ACS 2019-2023 uses 2020 geography, no translation needed
    >>> translated, stats = translate_acs_to_target_vintage(
    ...     df, "2019-2023", 2023
    ... )
    >>> assert stats is None  # No translation performed
    """
    if not needs_translation(acs_vintage, target_tract_vintage):
        source_vintage = get_source_tract_vintage(acs_vintage)
        logger.info(
            f"ACS {acs_vintage} uses {source_vintage} geography, "
            f"compatible with target vintage {target_tract_vintage}. "
            f"No translation needed."
        )
        return df, None

    source_vintage = get_source_tract_vintage(acs_vintage)
    logger.info(
        f"ACS {acs_vintage} uses {source_vintage} geography. "
        f"Translating to {target_tract_vintage} (2020-based) geography."
    )

    return translate_tracts_2010_to_2020(
        df,
        population_column=population_column,
        geoid_column=geoid_column,
    )
