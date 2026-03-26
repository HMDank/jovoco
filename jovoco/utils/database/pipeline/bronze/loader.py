import logging
from pathlib import Path
from typing import Type

import pandas as pd
from sqlalchemy import delete
from sqlalchemy.orm import Session

from .models import (
    BronzeBase,
    StgCustomer,
    StgOrder,
    StgOrderItem,
    StgProduct,
    StgStore,
)

logger = logging.getLogger(__name__)


class BronzeLoader:
    """Loads raw CSV data into the stg schema with raw string preservation."""

    def __init__(self, csv_dir: Path) -> None:
        self.csv_dir = csv_dir

    def load_all(self, session: Session) -> None:
        logger.info("Starting bronze load...")

        table_map = [
            (
                StgCustomer,
                "customers.csv",
                {
                    "customer_id": "CustomerID",
                    "name": "Name",
                    "city": "City",
                    "registration_date": "Registration Date",
                    "customer_type": "Type",
                },
            ),
            (
                StgOrder,
                "orders.csv",
                {
                    "order_id": "Order",
                    "customer_name": "Customer Name",
                    "store_id": "Store",
                    "order_date": "Date",
                    "status": "Status",
                },
            ),
            (
                StgOrderItem,
                "order_items.csv",
                {
                    "item_id": "Item",
                    "order_id": "Order",
                    "product_title": "Product",
                    "quantity": "Qty",
                    "unit_price": "Price",
                },
            ),
            (
                StgProduct,
                "products.csv",
                {
                    "product_id": "Product",
                    "title": "Title",
                    "category": "Category",
                    "cost": "Cost",
                },
            ),
            (
                StgStore,
                "stores.csv",
                {
                    "store_id": "Store",
                    "title": "Title",
                    "city": "City",
                    "region": "Region",
                },
            ),
        ]

        for model, filename, mapping in table_map:
            self._process_table(session, model, filename, mapping)

        session.commit()
        logger.info("Bronze load complete.")

    def _process_table(
        self, session: Session, model: Type[BronzeBase], filename: str, mapping: dict
    ) -> None:
        path = self.csv_dir / filename
        if not path.exists():
            logger.error(f"Missing file: {filename}")
            return

        session.execute(delete(model))

        df = pd.read_csv(path, dtype=str).fillna("")
        rows = df.to_dict(orient="records")

        records = []
        for r in rows:
            params = {
                model_field: (val if val != "" else None)
                for model_field, csv_col in mapping.items()
                if (val := r.get(csv_col)) is not None
            }
            records.append(model(**params))

        session.add_all(records)
        logger.info(f"Loaded {len(records)} rows into {model.__tablename__}")
