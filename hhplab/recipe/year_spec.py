"""Year-spec parser for build creation.

Accepted forms:
- Single year: "2020"
- Range: "2018-2024"
- List: "2018,2019,2020"
- Mixed: "2018-2020,2022"
"""

from __future__ import annotations

MIN_YEAR = 2000
MAX_YEAR = 2099


def _parse_token(token: str) -> list[int]:
    """Parse a single token which may be a year or a range."""
    token = token.strip()
    if not token:
        raise ValueError("Empty token in year spec.")

    if "-" in token:
        parts = token.split("-")
        if len(parts) != 2:
            raise ValueError(f"Invalid range: '{token}'. Expected format 'YYYY-YYYY'.")

        left, right = parts[0].strip(), parts[1].strip()
        if not left or not right:
            raise ValueError(f"Incomplete range: '{token}'. Expected format 'YYYY-YYYY'.")

        if not left.isdigit() or not right.isdigit():
            raise ValueError(
                f"Non-numeric value in range: '{token}'. Years must be integers."
            )

        start, end = int(left), int(right)

        if start > end:
            raise ValueError(
                f"Backwards range: '{token}'. Start year must be <= end year."
            )

        if start < MIN_YEAR or end > MAX_YEAR:
            raise ValueError(
                f"Year out of bounds in range '{token}'. "
                f"Years must be between {MIN_YEAR} and {MAX_YEAR}."
            )

        return list(range(start, end + 1))

    if not token.isdigit():
        raise ValueError(
            f"Non-numeric year: '{token}'. Years must be integers."
        )

    year = int(token)
    if year < MIN_YEAR or year > MAX_YEAR:
        raise ValueError(
            f"Year out of bounds: {year}. "
            f"Years must be between {MIN_YEAR} and {MAX_YEAR}."
        )

    return [year]


def parse_year_spec(spec: str) -> list[int]:
    """Parse a year-spec string into a sorted, deduplicated list of years.

    Parameters
    ----------
    spec:
        A string containing years as singles, ranges, or a mix separated by
        commas. Examples: ``"2020"``, ``"2018-2024"``, ``"2018-2020,2022"``.

    Returns
    -------
    list[int]
        Sorted ascending, deduplicated list of integer years.

    Raises
    ------
    ValueError
        If *spec* is empty, contains invalid tokens, backwards ranges, or
        produces an empty result after normalization.
    """
    if not spec or not spec.strip():
        raise ValueError("Year spec must not be empty.")

    tokens = spec.split(",")
    years: set[int] = set()

    for token in tokens:
        years.update(_parse_token(token))

    if not years:
        raise ValueError("Year spec produced no years after parsing.")

    return sorted(years)
