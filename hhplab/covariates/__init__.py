"""External covariate source support."""

from hhplab.covariates.catalog import (
    COVARIATE_SOURCE_SPECS,
    CovariateSourceSpec,
    covariate_source_spec,
)
from hhplab.covariates.ingest import ingest_covariate_source

__all__ = [
    "COVARIATE_SOURCE_SPECS",
    "CovariateSourceSpec",
    "covariate_source_spec",
    "ingest_covariate_source",
]
