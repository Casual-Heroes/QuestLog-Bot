# warden/db.py
"""
Database connection for Django to access Warden's MySQL database.

This allows the Django web dashboard to read/write to the same database
that the Discord bot uses. Changes made via the website are automatically
picked up by the bot.
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv()

# Build database URL from environment variables
DB_SOCKET = os.getenv("DB_SOCKET")  # Unix socket path (preferred)
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_USER = os.getenv("DB_USER", "warden")
DB_USERNAME = os.getenv("DB_USERNAME", DB_USER)  # Support both DB_USER and DB_USERNAME
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "warden")

# Use Unix socket if specified (faster and more reliable), otherwise use TCP
if DB_SOCKET:
    DATABASE_URL = f"mysql+mysqlconnector://{DB_USERNAME}:{DB_PASSWORD}@/{DB_NAME}?unix_socket={DB_SOCKET}&charset=utf8mb4"
else:
    if not DB_HOST:
        DB_HOST = "localhost"
    DATABASE_URL = f"mysql+mysqlconnector://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"

# Create engine (shared connection pool)
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,  # Check connection health
    pool_recycle=3600,   # Recycle connections after 1 hour
    echo=False           # Set True for SQL debugging
)

# Session factory
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


@contextmanager
def get_db_session():
    """
    Context manager for database sessions.

    Usage:
        with get_db_session() as db:
            trackers = db.query(ChannelStatTracker).filter_by(guild_id=123).all()
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_db():
    """
    Generator for FastAPI-style dependency injection.
    Can also be used in Django views.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
