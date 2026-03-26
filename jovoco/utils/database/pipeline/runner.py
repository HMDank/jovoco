"""ETL Pipeline runner: orchestrates Bronze → Silver → Gold in a single pass.

Usage:
    python -m jovoco.etl.runner /path/to/csv/directory

Each stage runs in its own session so that failures in a later stage
do not roll back completed earlier stages. Errors surface with full
stack traces and a clear stage label.
"""

import logging
import sys
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.schema import CreateSchema

from .bronze.loader import BronzeLoader
from .bronze.models import BronzeBase
from .gold.builder import GoldBuilder
from .gold.models import GoldBase
from .silver.models import SilverBase
from .silver.transformer import SilverTransformer
from ..connection import database_engine, get_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

_SCHEMAS = ("stg", "slv", "gold")


def _ensure_schemas() -> None:
    """Create Postgres schemas if they do not yet exist."""
    with database_engine.connect().execution_options(
        isolation_level="AUTOCOMMIT"
    ) as conn:
        for schema in _SCHEMAS:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
            logger.info(f"Dropped schema '{schema}'.")
            exists = conn.execute(
                text("SELECT 1 FROM pg_namespace WHERE nspname = :s"), {"s": schema}
            ).scalar()

            if not exists:
                conn.execute(CreateSchema(schema))
                logger.info(f"Created schema '{schema}'.")


def _create_tables() -> None:
    """Create all ORM-managed tables."""
    _ensure_schemas()
    BronzeBase.metadata.create_all(database_engine, checkfirst=True)
    SilverBase.metadata.create_all(database_engine, checkfirst=True)
    GoldBase.metadata.create_all(database_engine, checkfirst=True)
    logger.info("All tables created / verified.")


def _run_bronze(csv_dir: Path) -> None:
    logger.info("── Stage 1: Bronze (stg) ──────────────────────────────")
    loader = BronzeLoader(csv_dir)
    with get_session() as session:
        loader.load_all(session)


def _run_silver() -> None:
    logger.info("── Stage 2: Silver (slv) ──────────────────────────────")
    transformer = SilverTransformer()
    with get_session() as session:
        transformer.transform_all(session)


def _run_gold() -> None:
    logger.info("── Stage 3: Gold ──────────────────────────────────────")
    builder = GoldBuilder()
    with get_session() as session:
        builder.build_all(session)


def run_pipeline(csv_dir: Path) -> None:
    """Execute the full ETL pipeline end-to-end."""
    if not csv_dir.is_dir():
        raise FileNotFoundError(f"CSV directory not found: {csv_dir}")

    logger.info("═══════ ETL Pipeline Start ═══════════════════════════")
    _create_tables()

    try:
        _run_bronze(csv_dir)
    except Exception:
        logger.exception("Bronze stage failed.")
        raise

    try:
        _run_silver()
    except Exception:
        logger.exception("Silver stage failed.")
        raise

    try:
        _run_gold()
    except Exception:
        logger.exception("Gold stage failed.")
        raise

    logger.info("═══════ ETL Pipeline Complete ════════════════════════")


if __name__ == "__main__":
    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data/csv")
    run_pipeline(csv_path)
