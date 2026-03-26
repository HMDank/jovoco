"""Database session configuration."""

import os
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv

load_dotenv(override=True)

CONNECTION_STRING = os.getenv("CONNECTION_STRING")

if CONNECTION_STRING is None:
    raise ValueError("CONNECTION_STRING environment variable is not set.")

database_engine = create_engine(
    CONNECTION_STRING, poolclass=NullPool, connect_args={"sslmode": "disable"}
)


DatabaseSession = sessionmaker(
    database_engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)


@contextmanager
def get_session() -> Iterator[Session]:
    """Context manager yielding a database session."""
    session = DatabaseSession()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
