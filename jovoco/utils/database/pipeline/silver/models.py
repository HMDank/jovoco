from datetime import datetime
from sqlalchemy import (
    Column,
    String,
    Date,
    Numeric,
    Integer,
    ForeignKey,
    DateTime,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship, declarative_base

SilverBase = declarative_base()


class SlvCustomer(SilverBase):
    __tablename__ = "customers"
    __table_args__ = {"schema": "slv"}

    customer_id = Column(UUID(as_uuid=True), primary_key=True)
    name = Column(String)
    city = Column(String)
    registration_date = Column(Date)
    customer_type = Column(String)
    transformed_at = Column(DateTime, default=datetime.now, nullable=False)

    orders = relationship(
        "SlvOrder", back_populates="customer", cascade="all, delete-orphan"
    )


class SlvStore(SilverBase):
    __tablename__ = "stores"
    __table_args__ = {"schema": "slv"}

    store_id = Column(UUID(as_uuid=True), primary_key=True)
    title = Column(String)
    city = Column(String)
    region = Column(String)
    transformed_at = Column(DateTime, default=datetime.now, nullable=False)

    orders = relationship("SlvOrder", back_populates="store")


class SlvProduct(SilverBase):
    __tablename__ = "products"
    __table_args__ = {"schema": "slv"}

    product_id = Column(UUID(as_uuid=True), primary_key=True)
    title = Column(String)
    category = Column(String)
    cost = Column(Numeric(10, 2))
    transformed_at = Column(DateTime, default=datetime.now, nullable=False)

    items = relationship("SlvOrderItem", back_populates="product")


class SlvOrder(SilverBase):
    __tablename__ = "orders"
    __table_args__ = {"schema": "slv"}

    order_id = Column(UUID(as_uuid=True), primary_key=True)
    customer_id = Column(UUID(as_uuid=True), ForeignKey("slv.customers.customer_id"))
    customer_name = Column(String)
    store_id = Column(UUID(as_uuid=True), ForeignKey("slv.stores.store_id"))
    order_date = Column(Date)
    status = Column(String)
    transformed_at = Column(DateTime, default=datetime.now, nullable=False)

    customer = relationship("SlvCustomer", back_populates="orders")
    store = relationship("SlvStore", back_populates="orders")
    items = relationship(
        "SlvOrderItem", back_populates="order", cascade="all, delete-orphan"
    )


class SlvOrderItem(SilverBase):
    __tablename__ = "order_items"
    __table_args__ = {"schema": "slv"}

    item_id = Column(UUID(as_uuid=True), primary_key=True)
    order_id = Column(UUID(as_uuid=True), ForeignKey("slv.orders.order_id"))
    product_id = Column(UUID(as_uuid=True), ForeignKey("slv.products.product_id"))
    product_title = Column(String)
    quantity = Column(Integer)
    unit_price = Column(Numeric(10, 2))
    transformed_at = Column(DateTime, default=datetime.now, nullable=False)

    order = relationship("SlvOrder", back_populates="items")
    product = relationship("SlvProduct", back_populates="items")
