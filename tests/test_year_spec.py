"""Tests for the year-spec parser."""

import pytest

from hhplab.recipe.year_spec import parse_year_spec


class TestParseYearSpec:
    """Tests for parse_year_spec."""

    # --- Success cases ---

    def test_single_year(self):
        assert parse_year_spec("2020") == [2020]

    def test_range(self):
        assert parse_year_spec("2018-2024") == [
            2018, 2019, 2020, 2021, 2022, 2023, 2024,
        ]

    def test_list(self):
        assert parse_year_spec("2018,2020,2022") == [2018, 2020, 2022]

    def test_mixed(self):
        assert parse_year_spec("2018-2020,2022,2024") == [
            2018, 2019, 2020, 2022, 2024,
        ]

    def test_deduplication(self):
        assert parse_year_spec("2020,2020") == [2020]

    def test_sort_order(self):
        assert parse_year_spec("2022,2018") == [2018, 2022]

    def test_range_single_year(self):
        """A range where start == end is valid and yields one year."""
        assert parse_year_spec("2020-2020") == [2020]

    def test_mixed_with_overlap(self):
        """Overlapping range and list items are deduplicated."""
        assert parse_year_spec("2018-2020,2019,2020,2021") == [
            2018, 2019, 2020, 2021,
        ]

    def test_whitespace_in_tokens(self):
        """Whitespace around tokens is tolerated."""
        assert parse_year_spec("2018 , 2020") == [2018, 2020]

    def test_whitespace_in_range(self):
        assert parse_year_spec("2018 - 2020") == [2018, 2019, 2020]

    # --- Error cases ---

    def test_empty_string(self):
        with pytest.raises(ValueError, match="must not be empty"):
            parse_year_spec("")

    def test_whitespace_only(self):
        with pytest.raises(ValueError, match="must not be empty"):
            parse_year_spec("   ")

    def test_non_numeric(self):
        with pytest.raises(ValueError, match="Non-numeric"):
            parse_year_spec("abc")

    def test_backwards_range(self):
        with pytest.raises(ValueError, match="Backwards range"):
            parse_year_spec("2024-2018")

    def test_incomplete_range_trailing_dash(self):
        with pytest.raises(ValueError, match="Incomplete range"):
            parse_year_spec("2018-")

    def test_incomplete_range_leading_dash(self):
        with pytest.raises(ValueError, match="Incomplete range"):
            parse_year_spec("-2018")

    def test_triple_dash_range(self):
        with pytest.raises(ValueError, match="Invalid range"):
            parse_year_spec("2018-2020-2022")

    def test_year_below_minimum(self):
        with pytest.raises(ValueError, match="out of bounds"):
            parse_year_spec("1999")

    def test_year_above_maximum(self):
        with pytest.raises(ValueError, match="out of bounds"):
            parse_year_spec("2100")

    def test_range_below_minimum(self):
        with pytest.raises(ValueError, match="out of bounds"):
            parse_year_spec("1998-2000")

    def test_range_above_maximum(self):
        with pytest.raises(ValueError, match="out of bounds"):
            parse_year_spec("2098-2100")

    def test_non_numeric_in_list(self):
        with pytest.raises(ValueError, match="Non-numeric"):
            parse_year_spec("2018,abc,2020")

    def test_empty_token_from_trailing_comma(self):
        with pytest.raises(ValueError, match="Empty token"):
            parse_year_spec("2018,")

    def test_empty_token_from_leading_comma(self):
        with pytest.raises(ValueError, match="Empty token"):
            parse_year_spec(",2018")
