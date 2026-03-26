import logging
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Type, cast

import pandas as pd
from sqlalchemy import delete, select, inspect
from sqlalchemy.orm import Session

from ..bronze.models import StgCustomer, StgOrder, StgOrderItem, StgProduct, StgStore
from .models import SlvCustomer, SlvOrder, SlvOrderItem, SlvProduct, SlvStore

logger = logging.getLogger(__name__)


def _parse_date(value: Any) -> date | None:
    if pd.isna(value) or not value:
        return None
    value_string = str(value).strip()
    for date_format in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(value_string, date_format).date()
        except ValueError:
            continue
    return None


def _safe_uuid(value: Any) -> uuid.UUID | None:
    if pd.isna(value) or not value:
        return None
    value_string = str(value).strip().removesuffix(".0")
    try:
        return uuid.UUID(value_string)
    except ValueError:
        namespace_uuid = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")
        return uuid.uuid5(namespace_uuid, value_string)


def _safe_decimal(value: Any) -> Decimal | None:
    if pd.isna(value) or not value:
        return None
    try:
        return Decimal(str(value).strip().replace("$", "").replace(",", ""))
    except Exception:
        return None


def _normalize(series: pd.Series) -> pd.Series:
    return (
        series.fillna("")
        .astype(str)
        .str.lower()
        .str.strip()
        .str.split()
        .apply(lambda words: " ".join(sorted(words)) if isinstance(words, list) else "")
    )


class SilverTransformer:
    def transform_all(self, session: Session) -> None:
        logger.info("Starting silver transformation...")

        models_to_clean = [SlvOrderItem, SlvOrder, SlvProduct, SlvCustomer, SlvStore]
        for model in models_to_clean:
            session.execute(delete(model))

        self._transform_products(session)
        self._transform_stores(session)
        self._transform_customers(session)
        self._transform_orders(session)
        self._transform_order_items(session)

        session.commit()
        logger.info("Silver transformation complete.")

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

    def _transform_products(self, session: Session) -> None:
        products_df = self._get_df(session, StgProduct)
        if products_df.empty:
            return

        products_df["product_id"] = products_df["product"].map(_safe_uuid)
        products_df = products_df.dropna(subset=["product_id"]).drop_duplicates(
            "product_id"
        )
        products_df["title"] = products_df["title"].astype(str).str.strip()
        products_df["category"] = (
            products_df["category"].astype(str).str.strip().str.title()
        )
        products_df["cost"] = products_df["cost"].map(_safe_decimal)

        self._bulk_save(session, products_df, SlvProduct)

    def _transform_stores(self, session: Session) -> None:
        stores_df = self._get_df(session, StgStore)
        if stores_df.empty:
            return

        stores_df["store_id"] = stores_df["store"].map(_safe_uuid)
        stores_df = stores_df.dropna(subset=["store_id"]).drop_duplicates("store_id")
        stores_df["title"] = stores_df["title"].astype(str).str.strip().str.title()
        stores_df["region"] = stores_df["region"].astype(str).str.strip().str.title()

        missing_region_mask = (stores_df["region"].isna()) | (
            stores_df["region"] == "None"
        )
        stores_df.loc[missing_region_mask, "region"] = (
            stores_df.loc[missing_region_mask, "title"].str.split().str[-1].str.title()
        )

        self._bulk_save(session, stores_df, SlvStore)

    def _transform_customers(self, session: Session) -> None:
        customers_df = self._get_df(session, StgCustomer)
        if customers_df.empty:
            return

        customers_df["customer_id"] = customers_df["customer_id"].map(_safe_uuid)
        customers_df = customers_df.dropna(subset=["customer_id"]).drop_duplicates(
            "customer_id"
        )
        customers_df["registration_date"] = customers_df["registration_date"].map(
            _parse_date
        )
        customers_df["customer_type"] = (
            customers_df["type"].astype(str).str.strip().str.title()
        )

        self._bulk_save(session, customers_df, SlvCustomer)

    def _transform_orders(self, session: Session) -> None:
        orders_df = self._get_df(session, StgOrder)
        customers_reference = self._get_df(session, SlvCustomer)

        if orders_df.empty:
            return

        orders_df["order_id"] = orders_df["order"].map(_safe_uuid)
        orders_df = orders_df.dropna(subset=["order_id"]).drop_duplicates("order_id")

        orders_df["normalization_key"] = _normalize(orders_df["customer_name"])
        customers_reference["normalization_key"] = _normalize(
            customers_reference["name"]
        )

        merged_df = orders_df.merge(
            customers_reference[["normalization_key", "customer_id"]],
            on="normalization_key",
            how="left",
        )
        merged_df["store_id"] = merged_df["store"].map(_safe_uuid)
        merged_df["order_date"] = merged_df["date"].map(_parse_date)
        merged_df["status"] = merged_df["status"].astype(str).str.strip().str.lower()

        self._bulk_save(session, merged_df, SlvOrder)

    def _transform_order_items(self, session: Session) -> None:
        items_df = self._get_df(session, StgOrderItem)
        products_reference = self._get_df(session, SlvProduct)

        if items_df.empty:
            return

        items_df["item_id"] = items_df["item"].map(_safe_uuid)
        items_df = items_df.dropna(subset=["item_id"]).drop_duplicates("item_id")
        items_df["order_id"] = items_df["order"].map(_safe_uuid)

        items_df["normalization_key"] = _normalize(items_df["product"])
        products_reference["normalization_key"] = _normalize(
            products_reference["title"]
        )

        final_items_df = items_df.merge(
            products_reference[["normalization_key", "product_id"]],
            on="normalization_key",
            how="left",
        )
        final_items_df["quantity"] = (
            pd.to_numeric(final_items_df["qty"], errors="coerce").fillna(1).astype(int)
        )

        final_items_df["unit_price"] = final_items_df["price"].map(_safe_decimal)
        final_items_df = final_items_df.rename(columns={"product": "product_title"})

        self._bulk_save(session, final_items_df, SlvOrderItem)
