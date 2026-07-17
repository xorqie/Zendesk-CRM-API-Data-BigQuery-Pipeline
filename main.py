"""
main.py
--------
Single entrypoint for running the Zendesk -> BigQuery pipelines.

Usage:
    python main.py                  # run every pipeline, in dependency order
    python main.py users tickets    # run a specific subset
    python main.py kpi              # run just one

Dependency order: `kpi` joins ticket data in directly (it fetches tickets
itself), so it has no hard dependency on other pipelines having run first.
`users`, `tickets`, `csat`, and `kpi` are otherwise independent - the
default order below just runs the cheaper, faster syncs first.
"""

import sys

from src.pipelines import users_pipeline, tickets_pipeline, csat_pipeline, kpi_pipeline
from src.utils.logger import get_logger

logger = get_logger(__name__)

PIPELINES = {
    "users": users_pipeline.run,
    "tickets": tickets_pipeline.run,
    "csat": csat_pipeline.run,
    "kpi": kpi_pipeline.run,
}


def main() -> None:
    requested = sys.argv[1:] or list(PIPELINES.keys())

    unknown = [name for name in requested if name not in PIPELINES]
    if unknown:
        logger.error(f"Unknown pipeline(s): {unknown}. Available: {list(PIPELINES.keys())}")
        sys.exit(1)

    for name in requested:
        logger.info(f"--- Starting '{name}' pipeline ---")
        try:
            PIPELINES[name]()
        except Exception:
            logger.exception(f"Pipeline '{name}' failed.")
            raise
        logger.info(f"--- Finished '{name}' pipeline ---")


if __name__ == "__main__":
    main()
