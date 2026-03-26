"""ORM models for the bronze staging layer."""

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
    type: Mapped[str | None] = mapped_column(String(30))
    loaded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )


class StgOrder(BronzeBase):
    __tablename__ = "orders"
    __table_args__ = {"schema": "stg"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order: Mapped[str | None] = mapped_column(String(50))
    customer_name: Mapped[str | None] = mapped_column(String(100))
    store: Mapped[str | None] = mapped_column(String(50))
    date: Mapped[str | None] = mapped_column(String(30))
    status: Mapped[str | None] = mapped_column(String(30))
    loaded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )


class StgOrderItem(BronzeBase):
    __tablename__ = "order_items"
    __table_args__ = {"schema": "stg"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    item: Mapped[str | None] = mapped_column(String(50))
    order: Mapped[str | None] = mapped_column(String(50))
    product: Mapped[str | None] = mapped_column(String(100))
    qty: Mapped[str | None] = mapped_column(String(20))
    price: Mapped[str | None] = mapped_column(String(20))
    loaded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )


class StgProduct(BronzeBase):
    __tablename__ = "products"
    __table_args__ = {"schema": "stg"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    product: Mapped[str | None] = mapped_column(String(50))
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
    store: Mapped[str | None] = mapped_column(String(50))
    title: Mapped[str | None] = mapped_column(String(100))
    city: Mapped[str | None] = mapped_column(String(80))
    region: Mapped[str | None] = mapped_column(String(50))
    loaded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
