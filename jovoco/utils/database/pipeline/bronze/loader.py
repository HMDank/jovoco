import logging
import re
from pathlib import Path
from typing import Type

import pandas as pd
from sqlalchemy import delete
from sqlalchemy.orm import Session

from .models import BronzeBase

logger = logging.getLogger(__name__)


def normalize_column_names(name: str) -> str:
    """Standardizes identifiers: 'Registration Date' -> 'registration_date'."""
    return re.sub(r"[\s\-]+", "_", name.strip()).lower()


class BronzeLoader:
    """Loads raw CSV data into staging with minimal name normalization."""

    def __init__(self, csv_dir: Path) -> None:
        self.csv_dir = csv_dir

    def load_all(self, session: Session) -> None:
        logger.info("Starting dynamic bronze load...")

        for model in BronzeBase.__subclasses__():
            # Matches tablename to filename (e.g., customers -> customers.csv)
            filename = f"{model.__tablename__}.csv"
            self._process_table(session, model, filename)

        session.commit()
        logger.info("Bronze load complete.")

    def _process_table(
        self, session: Session, model: Type[BronzeBase], filename: str
    ) -> None:
        path = self.csv_dir / filename
        if not path.exists():
            logger.warning(f"File not found: {filename}")
            return

        session.execute(delete(model))

        # Load as raw strings
        df = pd.read_csv(path, dtype=str).fillna("")

        # Get valid model attributes
        valid_columns = model.__table__.columns.keys()

        records = []
        for _, row in df.iterrows():
            params = {}
            for csv_col in df.columns:
                norm_name = normalize_column_names(csv_col)

                if norm_name in valid_columns:
                    val = row[csv_col]
                    params[norm_name] = val if val != "" else None

            if params:
                records.append(model(**params))

        if records:
            session.add_all(records)
            logger.info(f"Loaded {len(records)} rows into {model.__tablename__}")
