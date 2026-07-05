"""External covariate source support."""

from hhplab.covariates.catalog import (
    COVARIATE_SOURCE_SPECS,
    CovariateSourceSpec,
    covariate_source_spec,
)
from hhplab.covariates.ingest import ingest_covariate_source
from hhplab.covariates.mpi_contract import (
    MPI_WORKBOOK_CONTRACT,
    MpiWorkbookContract,
    validate_mpi_workbook_contract,
)

__all__ = [
    "COVARIATE_SOURCE_SPECS",
    "MPI_WORKBOOK_CONTRACT",
    "CovariateSourceSpec",
    "MpiWorkbookContract",
    "covariate_source_spec",
    "ingest_covariate_source",
    "validate_mpi_workbook_contract",
]
