"""Legacy wrapper for the tracked rent-growth R-squared decomposition."""

from hhplab.results.workflows.analyze_rent_growth_r2_decomposition import main as _workflow_main


def main() -> None:
    _workflow_main()


if __name__ == "__main__":
    main()
