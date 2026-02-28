import asyncio
import os
import orjson
import asyncpg
from datetime import datetime
from dotenv import load_dotenv
from loguru import logger
from src.db import get_db_pool, init_db

load_dotenv()

async def load_data():
    """Loads data from videos.json into the database."""
    json_path = "videos.json"
    if not os.path.exists(json_path):
        logger.error(f"File {json_path} not found.")
        return

    logger.info(f"Reading {json_path}...")
    with open(json_path, "rb") as f:
        data = orjson.loads(f.read())

    videos = data.get("videos", [])
    if not videos:
        logger.warning("No videos found in JSON.")
        return

    logger.info(f"Found {len(videos)} videos. Preparing for insertion...")

    videos_rows = []
    snapshots_rows = []

    for video in videos:
        videos_rows.append((
            video['id'],
            video['creator_id'],
            datetime.fromisoformat(video['video_created_at']),
            video['views_count'],
            video['likes_count'],
            video['comments_count'],
            video['reports_count'],
            datetime.fromisoformat(video['created_at']),
            datetime.fromisoformat(video['updated_at'])
        ))

        for snap in video.get('snapshots', []):
            snapshots_rows.append((
                snap['id'],
                snap['video_id'],
                snap['views_count'],
                snap['likes_count'],
                snap['comments_count'],
                snap['reports_count'],
                snap['delta_views_count'],
                snap['delta_likes_count'],
                snap['delta_comments_count'],
                snap['delta_reports_count'],
                datetime.fromisoformat(snap['created_at']),
                datetime.fromisoformat(snap['updated_at'])
            ))

    pool = await get_db_pool()
    await init_db(pool)

    async with pool.acquire() as conn:
        logger.info("Inserting videos...")
        await conn.executemany("""
            INSERT INTO videos (
                id, creator_id, video_created_at, views_count, likes_count,
                comments_count, reports_count, created_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (id) DO UPDATE SET
                views_count = EXCLUDED.views_count,
                likes_count = EXCLUDED.likes_count,
                comments_count = EXCLUDED.comments_count,
                reports_count = EXCLUDED.reports_count,
                updated_at = EXCLUDED.updated_at
        """, videos_rows)

        logger.info(f"Inserted/Updated {len(videos_rows)} videos.")

        logger.info("Inserting snapshots...")
        await conn.executemany("""
            INSERT INTO video_snapshots (
                id, video_id, views_count, likes_count, comments_count, reports_count,
                delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
                created_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (id) DO NOTHING
        """, snapshots_rows)

        logger.info(f"Inserted {len(snapshots_rows)} snapshots.")

    await pool.close()
    logger.info("Data loading complete.")

if __name__ == "__main__":
    asyncio.run(load_data())
