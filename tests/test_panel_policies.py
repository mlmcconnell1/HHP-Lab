"""Tests for panel assembly policies."""

from __future__ import annotations

import json

import pytest

from coclab.panel.policies import (
    DEFAULT_POLICY,
    AlignmentPolicy,
    default_acs_vintage,
    default_boundary_vintage,
)


class TestDefaultBoundaryVintage:
    """Tests for default_boundary_vintage function."""

    def test_returns_string(self):
        result = default_boundary_vintage(2024)
        assert isinstance(result, str)

    def test_2024_returns_2024(self):
        assert default_boundary_vintage(2024) == "2024"

    def test_2020_returns_2020(self):
        assert default_boundary_vintage(2020) == "2020"

    def test_is_pure_function(self):
        result1 = default_boundary_vintage(2024)
        result2 = default_boundary_vintage(2024)
        assert result1 == result2


class TestDefaultAcsVintage:
    """Tests for default_acs_vintage function."""

    def test_returns_string(self):
        result = default_acs_vintage(2024)
        assert isinstance(result, str)

    def test_2024_returns_2023(self):
        assert default_acs_vintage(2024) == "2023"

    def test_2020_returns_2019(self):
        assert default_acs_vintage(2020) == "2019"

    def test_one_year_lag(self):
        for year in range(2015, 2030):
            assert default_acs_vintage(year) == str(year - 1)


class TestAlignmentPolicy:
    """Tests for AlignmentPolicy dataclass."""

    def test_default_policy_exists(self):
        assert DEFAULT_POLICY is not None
        assert isinstance(DEFAULT_POLICY, AlignmentPolicy)

    def test_default_policy_boundary_func(self):
        assert DEFAULT_POLICY.boundary_vintage_func(2024) == "2024"

    def test_default_policy_acs_func(self):
        assert DEFAULT_POLICY.acs_vintage_func(2024) == "2023"

    def test_default_policy_weighting_method(self):
        assert DEFAULT_POLICY.weighting_method == "population"

    def test_custom_policy_creation(self):
        custom_policy = AlignmentPolicy(
            boundary_vintage_func=lambda y: str(y - 1),
            acs_vintage_func=lambda y: str(y - 2),
            weighting_method="area",
        )
        assert custom_policy.boundary_vintage_func(2024) == "2023"
        assert custom_policy.acs_vintage_func(2024) == "2022"
        assert custom_policy.weighting_method == "area"


class TestAlignmentPolicySerialization:
    """Tests for policy serialization and deserialization."""

    def test_to_dict_returns_dict(self):
        result = DEFAULT_POLICY.to_dict()
        assert isinstance(result, dict)

    def test_to_dict_has_required_keys(self):
        result = DEFAULT_POLICY.to_dict()
        assert "boundary_vintage_func" in result
        assert "acs_vintage_func" in result
        assert "weighting_method" in result

    def test_to_dict_weighting_method(self):
        result = DEFAULT_POLICY.to_dict()
        assert result["weighting_method"] == "population"

    def test_from_dict_roundtrip(self):
        original = DEFAULT_POLICY.to_dict()
        restored = AlignmentPolicy.from_dict(original)

        assert restored.boundary_vintage_func(2024) == "2024"
        assert restored.acs_vintage_func(2024) == "2023"
        assert restored.weighting_method == "population"

    def test_from_dict_unknown_function_raises(self):
        data = {
            "boundary_vintage_func": "unknown_function",
            "acs_vintage_func": "default_acs_vintage",
            "weighting_method": "population",
        }
        with pytest.raises(ValueError, match="Unknown function name"):
            AlignmentPolicy.from_dict(data)

    def test_serialization_is_json_compatible(self):
        result = DEFAULT_POLICY.to_dict()
        json_str = json.dumps(result)
        parsed = json.loads(json_str)
        assert parsed == result
