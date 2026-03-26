import uuid
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    Column,
    String,
    Date,
    Numeric,
    Integer,
    ForeignKey,
    DateTime,
    SmallInteger,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base

GoldBase = declarative_base()


class DimDate(GoldBase):
    __tablename__ = "dim_date"
    __table_args__ = {"schema": "gold"}

    date_key = Column(Integer, primary_key=True)
    full_date = Column(Date, nullable=False)
    year = Column(SmallInteger, nullable=False)
    quarter = Column(SmallInteger, nullable=False)
    month = Column(SmallInteger, nullable=False)
    week = Column(SmallInteger, nullable=False)
    day_of_week = Column(SmallInteger, nullable=False)
    day_of_month = Column(SmallInteger, nullable=False)
    transformed_at = Column(DateTime, default=datetime.now, nullable=False)


class DimCustomer(GoldBase):
    __tablename__ = "dim_customer"
    __table_args__ = {"schema": "gold"}

    customer_id = Column(UUID(as_uuid=True), primary_key=True)
    name = Column(String(100))
    city = Column(String(80))
    customer_type = Column(String(30))
    registration_date = Column(Date)
    transformed_at = Column(DateTime, default=datetime.now, nullable=False)


class DimProduct(GoldBase):
    __tablename__ = "dim_product"
    __table_args__ = {"schema": "gold"}

    product_id = Column(UUID(as_uuid=True), primary_key=True)
    title = Column(String(100))
    category = Column(String(50))
    cost = Column(Numeric(12, 2))
    transformed_at = Column(DateTime, default=datetime.now, nullable=False)


class DimStore(GoldBase):
    __tablename__ = "dim_store"
    __table_args__ = {"schema": "gold"}

    store_id = Column(UUID(as_uuid=True), primary_key=True)
    title = Column(String(100))
    city = Column(String(80))
    region = Column(String(50))
    transformed_at = Column(DateTime, default=datetime.now, nullable=False)


class FactSales(GoldBase):
    __tablename__ = "fact_sales"
    __table_args__ = {"schema": "gold"}

    item_id = Column(UUID(as_uuid=True), primary_key=True)
    order_id = Column(UUID(as_uuid=True), nullable=False)
    date_key = Column(Integer, ForeignKey("gold.dim_date.date_key"))
    customer_id = Column(
        UUID(as_uuid=True), ForeignKey("gold.dim_customer.customer_id")
    )
    store_id = Column(UUID(as_uuid=True), ForeignKey("gold.dim_store.store_id"))
    product_id = Column(UUID(as_uuid=True), ForeignKey("gold.dim_product.product_id"))
    quantity = Column(Integer, nullable=False)
    unit_price = Column(Numeric(12, 2))
    revenue = Column(Numeric(15, 2))
    cost = Column(Numeric(15, 2))
    margin = Column(Numeric(15, 2))
    transformed_at = Column(DateTime, default=datetime.now, nullable=False)
