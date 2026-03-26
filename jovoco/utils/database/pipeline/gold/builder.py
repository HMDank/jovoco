import logging
from datetime import date, datetime
from typing import Any, Dict, List, Type, cast

import pandas as pd
from sqlalchemy import delete, select, inspect
from sqlalchemy.orm import Session

from ..silver.models import (
    SlvCustomer,
    SlvOrder,
    SlvOrderItem,
    SlvProduct,
    SlvStore,
)
from .models import (
    DimCustomer,
    DimDate,
    DimProduct,
    DimStore,
    FactSales,
)

logger = logging.getLogger(__name__)


def _date_to_key(date_value: date) -> int:
    return date_value.year * 10_000 + date_value.month * 100 + date_value.day


class GoldBuilder:
    def build_all(self, session: Session) -> None:
        logger.info("Starting gold build...")

        session.execute(delete(FactSales))
        for dimension_model in [DimDate, DimCustomer, DimProduct, DimStore]:
            session.execute(delete(dimension_model))

        self._build_dim_date(session)
        self._build_dim_customer(session)
        self._build_dim_product(session)
        self._build_dim_store(session)
        self._build_fact_sales(session)

        session.commit()
        logger.info("Gold build complete.")

    def _get_df(self, session: Session, model: Type[Any]) -> pd.DataFrame:
        records = session.execute(select(model)).scalars().all()
        return pd.DataFrame(
            [
                {
                    column.key: getattr(record, column.key)
                    for column in inspect(model).mapper.column_attrs
                }
                for record in records
            ]
        )

    def _bulk_save(self, session: Session, df: pd.DataFrame, model: Type[Any]) -> None:
        if df.empty:
            return
        inspection_result = inspect(model)
        valid_attributes = set(inspection_result.mapper.attrs.keys())
        filtered_df = df[
            [column for column in df.columns if column in valid_attributes]
        ]

        raw_records = cast(List[Dict[str, Any]], filtered_df.to_dict("records"))
        clean_records = [
            {key: (None if pd.isna(value) else value) for key, value in record.items()}
            for record in raw_records
        ]

        session.bulk_insert_mappings(inspection_result.mapper, clean_records)
        session.flush()
        logger.info(f"Loaded {len(clean_records)} records into {model.__name__}.")

    def _build_dim_date(self, session: Session) -> None:
        orders_df = self._get_df(session, SlvOrder)
        if orders_df.empty or orders_df["order_date"].isnull().all():
            logger.warning("No order dates found; dim_date will be empty.")
            return

        min_date = orders_df["order_date"].min()
        max_date = orders_df["order_date"].max()

        date_range = pd.date_range(start=min_date, end=max_date, freq="D")
        date_df = pd.DataFrame({"full_date": date_range.date})

        date_df["date_key"] = date_df["full_date"].map(_date_to_key)
        date_df["year"] = date_df["full_date"].map(lambda d: d.year)
        date_df["month"] = date_df["full_date"].map(lambda d: d.month)
        date_df["day_of_month"] = date_df["full_date"].map(lambda d: d.day)
        date_df["quarter"] = date_df["full_date"].map(lambda d: (d.month - 1) // 3 + 1)
        date_df["week"] = date_df["full_date"].map(lambda d: d.isocalendar()[1])
        date_df["day_of_week"] = date_df["full_date"].map(lambda d: d.isocalendar()[2])

        self._bulk_save(session, date_df, DimDate)

    def _build_dim_customer(self, session: Session) -> None:
        df = self._get_df(session, SlvCustomer)
        self._bulk_save(session, df, DimCustomer)

    def _build_dim_product(self, session: Session) -> None:
        df = self._get_df(session, SlvProduct)
        self._bulk_save(session, df, DimProduct)

    def _build_dim_store(self, session: Session) -> None:
        df = self._get_df(session, SlvStore)
        self._bulk_save(session, df, DimStore)

    def _build_fact_sales(self, session: Session) -> None:
        items_df = self._get_df(session, SlvOrderItem)
        orders_df = self._get_df(session, SlvOrder)
        products_df = self._get_df(session, SlvProduct)

        if items_df.empty:
            return

        df = items_df.merge(
            orders_df[["order_id", "order_date", "customer_id", "store_id"]],
            on="order_id",
            how="left",
        )
        df = df.merge(
            products_df[["product_id", "cost"]].rename(
                columns={"cost": "product_unit_cost"}
            ),
            on="product_id",
            how="left",
        )

        df["date_key"] = df["order_date"].map(
            lambda d: _date_to_key(d) if pd.notnull(d) else None
        )

        df["revenue"] = df["quantity"] * df["unit_price"]
        df["cost"] = df["quantity"] * df["product_unit_cost"]
        df["margin"] = df["revenue"] - df["cost"]

        self._bulk_save(session, df, FactSales)
