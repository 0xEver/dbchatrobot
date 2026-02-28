import os
import asyncpg
from typing import Optional
from loguru import logger

async def get_db_pool() -> asyncpg.Pool:
    """Creates a connection pool to the PostgreSQL database."""
    try:
        pool = await asyncpg.create_pool(
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres"),
            database=os.getenv("POSTGRES_DB", "videos_stats"),
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            min_size=1,
            max_size=10,
        )
        logger.info("Database pool created successfully.")
        return pool
    except Exception as e:
        logger.error(f"Failed to create database pool: {e}")
        raise

async def init_db(pool: asyncpg.Pool) -> None:
    """Initializes the database with the schema."""
    try:
        with open("src/schema.sql", "r") as f:
            schema = f.read()

        async with pool.acquire() as connection:
            await connection.execute(schema)
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise
