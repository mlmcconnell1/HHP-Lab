"""Tests for the census/acs1 dataset adapter added in coclab-425s."""

from coclab.recipe.adapters import DatasetAdapterRegistry
from coclab.recipe.default_dataset_adapters import (
    _validate_census_acs1,
    register_dataset_defaults,
)
from coclab.recipe.recipe_schema import DatasetSpec, GeometryRef


def _make_acs1_spec(
    version: int = 1,
    geo_type: str = "metro",
    geo_source: str | None = None,
    params: dict | None = None,
    path: str | None = None,
) -> DatasetSpec:
    return DatasetSpec(
        provider="census",
        product="acs1",
        version=version,
        native_geometry=GeometryRef(type=geo_type, source=geo_source),
        params=params or {},
        path=path,
    )


class TestValidateCensusAcs1:
    def test_valid_metro_with_source(self):
        spec = _make_acs1_spec(geo_type="metro", geo_source="census_api")
        diags = _validate_census_acs1(spec)
        assert diags == []

    def test_wrong_version_produces_error(self):
        spec = _make_acs1_spec(version=2)
        diags = _validate_census_acs1(spec)
        assert any(d.level == "error" and "version" in d.message for d in diags)

    def test_metro_missing_source_produces_warning(self):
        spec = _make_acs1_spec(geo_type="metro", geo_source=None)
        diags = _validate_census_acs1(spec)
        assert any(d.level == "warning" and "source" in d.message for d in diags)

    def test_tract_geometry_with_path_valid(self):
        """Tract-level ACS1 with a materialized artifact path should not error."""
        spec = _make_acs1_spec(geo_type="tract", path="data/acs1_tract.parquet")
        diags = _validate_census_acs1(spec)
        assert not any(d.level == "error" for d in diags)

    def test_tract_geometry_without_path_errors(self):
        """Tract-level ACS1 without a path should error (must use metro natively)."""
        spec = _make_acs1_spec(geo_type="tract")
        diags = _validate_census_acs1(spec)
        assert any(d.level == "error" and "metro" in d.message for d in diags)

    def test_unknown_params_produce_warning(self):
        spec = _make_acs1_spec(
            geo_type="metro",
            geo_source="census_api",
            params={"vintage": 2023, "bogus": True},
        )
        diags = _validate_census_acs1(spec)
        assert any(d.level == "warning" and "unrecognized" in d.message for d in diags)

    def test_known_params_no_warning(self):
        spec = _make_acs1_spec(
            geo_type="metro",
            geo_source="census_api",
            params={"vintage": 2023, "align": "to_calendar_year"},
        )
        diags = _validate_census_acs1(spec)
        assert diags == []


class TestAcs1Registration:
    def test_acs1_registered_in_defaults(self):
        reg = DatasetAdapterRegistry()
        register_dataset_defaults(reg)
        assert ("census", "acs1") in reg.registered_products()

    def test_acs1_adapter_accepts_valid_spec(self):
        reg = DatasetAdapterRegistry()
        register_dataset_defaults(reg)
        spec = _make_acs1_spec(geo_type="metro", geo_source="census_api")
        diags = reg.validate(spec)
        assert diags == []
