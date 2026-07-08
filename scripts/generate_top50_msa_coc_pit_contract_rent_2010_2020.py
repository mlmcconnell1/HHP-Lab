"""Legacy wrapper for the tracked top-50 MSA/CoC contract-rent workflow module."""

from __future__ import annotations

from hhplab.results.workflows.generate_top50_msa_coc_pit_contract_rent_2010_2020 import (
    main as _workflow_main,
)


def main() -> None:
    _workflow_main()


if __name__ == "__main__":
    main()
