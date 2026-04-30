"""Tests for hhplab.analysis_geo module.

Covers the analysis geography model, DataFrame helpers, and canonical
column handling introduced in coclab-djrh.1.
"""

import pandas as pd
import pytest

from hhplab.analysis_geo import (
    GEO_TYPE_COC,
    GEO_TYPE_METRO,
    GEO_TYPE_MSA,
    VALID_GEO_TYPES,
    AnalysisGeometryRef,
    ensure_canonical_geo_columns,
    infer_geo_type,
    resolve_geo_col,
)

# ---------------------------------------------------------------------------
# AnalysisGeometryRef
# ---------------------------------------------------------------------------


class TestAnalysisGeometryRef:
    def test_coc_constructor(self):
        ref = AnalysisGeometryRef.coc("2025")
        assert ref.geo_type == "coc"
        assert ref.boundary_vintage == "2025"
        assert ref.definition_version is None
        assert ref.is_coc
        assert not ref.is_metro

    def test_metro_constructor(self):
        ref = AnalysisGeometryRef.metro("glynn_fox_v1")
        assert ref.geo_type == "metro"
        assert ref.definition_version == "glynn_fox_v1"
        assert ref.boundary_vintage is None
        assert ref.is_metro
        assert not ref.is_coc

    def test_msa_constructor(self):
        ref = AnalysisGeometryRef.msa("census_msa_2023")
        assert ref.geo_type == "msa"
        assert ref.definition_version == "census_msa_2023"
        assert ref.boundary_vintage is None
        assert ref.is_msa
        assert not ref.is_metro

    def test_invalid_geo_type_raises(self):
        with pytest.raises(ValueError, match="Unknown geo_type 'county'"):
            AnalysisGeometryRef(geo_type="county")

    def test_to_dict_coc(self):
        ref = AnalysisGeometryRef.coc("2025")
        d = ref.to_dict()
        assert d == {"geo_type": "coc", "boundary_vintage": "2025"}

    def test_to_dict_metro(self):
        ref = AnalysisGeometryRef.metro("glynn_fox_v1")
        d = ref.to_dict()
        assert d == {"geo_type": "metro", "definition_version": "glynn_fox_v1"}

    def test_to_dict_msa(self):
        ref = AnalysisGeometryRef.msa("census_msa_2023")
        d = ref.to_dict()
        assert d == {"geo_type": "msa", "definition_version": "census_msa_2023"}

    def test_frozen(self):
        ref = AnalysisGeometryRef.coc("2025")
        with pytest.raises(AttributeError):
            ref.geo_type = "metro"

    def test_valid_geo_types_tuple(self):
        assert GEO_TYPE_COC in VALID_GEO_TYPES
        assert GEO_TYPE_METRO in VALID_GEO_TYPES
        assert GEO_TYPE_MSA in VALID_GEO_TYPES


# ---------------------------------------------------------------------------
# resolve_geo_col
# ---------------------------------------------------------------------------


class TestResolveGeoCol:
    def test_coc_id_preferred(self):
        df = pd.DataFrame({"coc_id": ["NY-600"], "geo_id": ["NY-600"]})
        assert resolve_geo_col(df) == "coc_id"

    def test_geo_id_fallback(self):
        df = pd.DataFrame({"geo_id": ["GF01"]})
        assert resolve_geo_col(df) == "geo_id"

    def test_metro_id_resolved(self):
        df = pd.DataFrame({"metro_id": ["GF01"]})
        assert resolve_geo_col(df) == "metro_id"

    def test_missing_raises(self):
        df = pd.DataFrame({"other_col": ["GF01"]})
        with pytest.raises(KeyError, match="neither"):
            resolve_geo_col(df)

    def test_msa_id_resolved(self):
        df = pd.DataFrame({"msa_id": ["35620"]})
        assert resolve_geo_col(df) == "msa_id"


# ---------------------------------------------------------------------------
# infer_geo_type
# ---------------------------------------------------------------------------


class TestInferGeoType:
    def test_from_geo_type_column(self):
        df = pd.DataFrame({"geo_type": ["metro", "metro"], "geo_id": ["GF01", "GF02"]})
        assert infer_geo_type(df) == "metro"

    def test_from_coc_id_column(self):
        df = pd.DataFrame({"coc_id": ["NY-600"], "year": [2020]})
        assert infer_geo_type(df) == "coc"

    def test_multiple_geo_types_raises(self):
        df = pd.DataFrame({"geo_type": ["coc", "metro"], "geo_id": ["A", "B"]})
        with pytest.raises(ValueError, match="multiple geo_type"):
            infer_geo_type(df)

    def test_fallback_to_metro_id_column(self):
        """No geo_type col, no coc_id col, but metro_id → should return 'metro'."""
        df = pd.DataFrame({"metro_id": ["GF01", "GF02"], "year": [2020, 2021]})
        assert infer_geo_type(df) == "metro"

    def test_fallback_to_msa_id_column(self):
        df = pd.DataFrame({"msa_id": ["35620", "31080"], "year": [2020, 2021]})
        assert infer_geo_type(df) == "msa"

    def test_unsupported_geo_type_value_raises(self):
        """geo_type column contains an unsupported value → ValueError."""
        df = pd.DataFrame({"geo_type": ["county", "county"], "geo_id": ["A", "B"]})
        with pytest.raises(ValueError, match="Unsupported geo_type 'county'"):
            infer_geo_type(df)

    def test_no_identifiable_columns_raises(self):
        """No geo_type, coc_id, or metro_id columns → ValueError."""
        df = pd.DataFrame({"year": [2020], "value": [42]})
        with pytest.raises(ValueError, match="Cannot infer geo_type"):
            infer_geo_type(df)

    def test_all_null_geo_type_falls_back_to_heuristic(self):
        """geo_type column present but all-null → fall back to column heuristic."""
        df = pd.DataFrame({
            "geo_type": [None, None],
            "coc_id": ["NY-600", "CA-600"],
        })
        assert infer_geo_type(df) == "coc"


# ---------------------------------------------------------------------------
# ensure_canonical_geo_columns
# ---------------------------------------------------------------------------


class TestEnsureCanonicalGeoColumns:
    def test_coc_adds_geo_id_and_geo_type(self):
        df = pd.DataFrame({"coc_id": ["NY-600", "CA-600"], "year": [2020, 2020]})
        result = ensure_canonical_geo_columns(df, "coc")
        assert "geo_id" in result.columns
        assert "geo_type" in result.columns
        assert list(result["geo_id"]) == ["NY-600", "CA-600"]
        assert list(result["geo_type"]) == ["coc", "coc"]
        # coc_id preserved
        assert "coc_id" in result.columns

    def test_metro_with_explicit_source_col(self):
        df = pd.DataFrame({"metro_id": ["GF01"], "year": [2020]})
        result = ensure_canonical_geo_columns(
            df, "metro", geo_id_source_col="metro_id"
        )
        assert list(result["geo_id"]) == ["GF01"]
        assert list(result["geo_type"]) == ["metro"]

    def test_no_copy_when_inplace(self):
        df = pd.DataFrame({"coc_id": ["NY-600"]})
        result = ensure_canonical_geo_columns(df, "coc", inplace=True)
        assert result is df

    def test_copy_by_default(self):
        df = pd.DataFrame({"coc_id": ["NY-600"]})
        result = ensure_canonical_geo_columns(df, "coc")
        assert result is not df

    def test_geo_id_already_present(self):
        df = pd.DataFrame({"geo_id": ["GF01"], "year": [2020]})
        result = ensure_canonical_geo_columns(df, "metro")
        assert list(result["geo_id"]) == ["GF01"]

    def test_missing_source_raises(self):
        df = pd.DataFrame({"metro_id": ["GF01"]})
        with pytest.raises(KeyError, match="Cannot determine"):
            ensure_canonical_geo_columns(df, "metro")
