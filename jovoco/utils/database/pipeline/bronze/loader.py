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
    """Converts 'Registration Date' or 'RegistrationDate' to 'registration_date'."""
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name.replace(" ", ""))
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


class BronzeLoader:
    """Loads raw CSV data into the stg schema using dynamic regex mapping."""

    def __init__(self, csv_dir: Path) -> None:
        self.csv_dir = csv_dir

    def load_all(self, session: Session) -> None:
        logger.info("Starting dynamic bronze load...")

        for model in BronzeBase.__subclasses__():
            filename = f"{model.__tablename__}.csv"
            self._process_table(session, model, filename)

        session.commit()
        logger.info("Bronze load complete.")

    def _process_table(
        self, session: Session, model: Type[BronzeBase], filename: str
    ) -> None:
        path = self.csv_dir / filename
        if not path.exists():
            logger.warning(f"File not found, skipping: {filename}")
            return

        session.execute(delete(model))

        df = pd.read_csv(path, dtype=str).fillna("")

        csv_to_model_map = {col: normalize_column_names(col) for col in df.columns}
        valid_columns = model.__table__.columns.keys()

        records = []
        for _, row in df.iterrows():
            params = {}
            for csv_col, normalized in csv_to_model_map.items():
                if normalized in valid_columns:
                    val = row[csv_col]
                    params[normalized] = val if val != "" else None

            if params:
                records.append(model(**params))

        if records:
            session.add_all(records)
            logger.info(f"Loaded {len(records)} rows into {model.__tablename__}")
