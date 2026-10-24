import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Resolve paths
BASE_DIR = Path(__file__).resolve().parent.parent
SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"

# Load DATABASE_URL from .env using python-dotenv
load_dotenv(dotenv_path=BASE_DIR / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")

# Fix potential legacy postgres:// protocol for SQLAlchemy compatibility
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL) if DATABASE_URL else None


def get_engine():
    """
    Returns the SQLAlchemy engine. If not already instantiated,
    it reloads DATABASE_URL and creates the engine.
    """
    global engine
    if engine is None:
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise ValueError(
                "DATABASE_URL is not configured. Please ensure it is defined in your .env file."
            )
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        engine = create_engine(db_url)
    return engine


def init_db(engine_override=None):
    """
    Runs schema.sql against the database to create all tables and indexes.
    All table creation statements use CREATE TABLE IF NOT EXISTS.
    """
    target_engine = engine_override or engine or get_engine()

    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    with target_engine.begin() as conn:
        conn.exec_driver_sql(schema_sql)


if __name__ == "__main__":
    init_db()
