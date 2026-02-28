import asyncio
from loguru import logger
from src.llm import GigaChatService
from src.db import get_db_pool

class StatsService:
    def __init__(self):
        self.llm = GigaChatService()
        self.pool = None

    async def initialize(self):
        if not self.pool:
            self.pool = await get_db_pool()

    async def process_query(self, user_text: str) -> str:
        if not self.pool:
            await self.initialize()

        try:
            sql_query = await self.llm.generate_sql(user_text)

            if not sql_query.strip().upper().startswith("SELECT"):
                logger.warning(f"Generated query is not a SELECT statement: {sql_query}")
                return "Ошибка"

            async with self.pool.acquire() as conn:
                logger.info(f"Executing SQL: {sql_query}")
                try:
                    result = await conn.fetchval(sql_query)
                    if result is None:
                        return "0"
                    return str(result)
                except Exception as db_err:
                    logger.error(f"Database execution error: {db_err}")
                    return "Ошибка"

        except Exception as e:
            logger.error(f"Error processing query '{user_text}': {e}")
            return "Ошибка"

    async def close(self):
        if self.pool:
            await self.pool.close()
