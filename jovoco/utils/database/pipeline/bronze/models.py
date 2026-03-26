"""SQLAlchemy ORM models for the bronze (stg) staging layer.

Raw data is persisted exactly as received from source CSVs.
All columns are stored as strings to preserve original values.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class BronzeBase(DeclarativeBase):
    pass


class StgCustomer(BronzeBase):
    __tablename__ = "customers"
    __table_args__ = {"schema": "stg"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    customer_id: Mapped[str | None] = mapped_column(String(50))
    name: Mapped[str | None] = mapped_column(String(100))
    city: Mapped[str | None] = mapped_column(String(80))
    registration_date: Mapped[str | None] = mapped_column(String(30))
    customer_type: Mapped[str | None] = mapped_column(String(30))
    loaded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )


class StgOrder(BronzeBase):
    __tablename__ = "orders"
    __table_args__ = {"schema": "stg"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[str | None] = mapped_column(String(50))
    customer_name: Mapped[str | None] = mapped_column(String(100))
    store_id: Mapped[str | None] = mapped_column(String(50))
    order_date: Mapped[str | None] = mapped_column(String(30))
    status: Mapped[str | None] = mapped_column(String(30))
    loaded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )


class StgOrderItem(BronzeBase):
    __tablename__ = "order_items"
    __table_args__ = {"schema": "stg"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    item_id: Mapped[str | None] = mapped_column(String(50))
    order_id: Mapped[str | None] = mapped_column(String(50))
    product_title: Mapped[str | None] = mapped_column(String(100))
    quantity: Mapped[str | None] = mapped_column(String(20))
    unit_price: Mapped[str | None] = mapped_column(String(20))
    loaded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )


class StgProduct(BronzeBase):
    __tablename__ = "products"
    __table_args__ = {"schema": "stg"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    product_id: Mapped[str | None] = mapped_column(String(50))
    title: Mapped[str | None] = mapped_column(String(100))
    category: Mapped[str | None] = mapped_column(String(50))
    cost: Mapped[str | None] = mapped_column(String(20))
    loaded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )


class StgStore(BronzeBase):
    __tablename__ = "stores"
    __table_args__ = {"schema": "stg"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    store_id: Mapped[str | None] = mapped_column(String(50))
    title: Mapped[str | None] = mapped_column(String(100))
    city: Mapped[str | None] = mapped_column(String(80))
    region: Mapped[str | None] = mapped_column(String(50))
    loaded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
