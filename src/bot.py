import asyncio
import os
import sys
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message
from loguru import logger
from src.service import StatsService
from src.llm import GigaChatService # ensure imports work

load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN is not set")

bot = Bot(token=TOKEN) if TOKEN else None
dp = Dispatcher()
service = StatsService()

@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    await message.answer("Привет! Я бот для сбора статистики по видео. Задай мне вопрос на естественном языке.")

@dp.message()
async def query_handler(message: Message) -> None:
    try:
        user_query = message.text
        if not user_query:
            return

        logger.info(f"Received query: {user_query}")
        result = await service.process_query(user_query)
        await message.answer(result)
    except Exception as e:
        logger.error(f"Error handling message: {e}")
        await message.answer("Ошибка")

async def main() -> None:
    if not bot:
        logger.error("Bot token not found. Exiting.")
        return

    await service.initialize()
    logger.info("Service initialized.")

    try:
        await dp.start_polling(bot)
    finally:
        await service.close()
        logger.info("Service closed.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped!")
