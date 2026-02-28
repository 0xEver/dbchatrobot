import os
import re
from gigachat import GigaChat
from gigachat.models import Chat, Messages, MessagesRole
from loguru import logger

class GigaChatService:
    def __init__(self):
        self.credentials = os.getenv("GIGACHAT_CREDENTIALS")
        self.scope = os.getenv("GIGACHAT_SCOPE")
        self.client = GigaChat(
            credentials=self.credentials,
            scope=self.scope,
            verify_ssl_certs=False
        )
        self.system_prompt = """
Ты — эксперт-аналитик баз данных PostgreSQL. Твоя задача — сгенерировать один единственный SQL-запрос для ответа на вопрос пользователя на естественном языке на основе предоставленной схемы базы данных.

СХЕМА БАЗЫ ДАННЫХ:

Таблица "videos" (Сводная статистика по каждому видео):
- id (UUID): Идентификатор видео
- creator_id (UUID): Идентификатор автора
- video_created_at (TIMESTAMPTZ): Дата и время публикации видео
- views_count (INTEGER): Общее количество просмотров
- likes_count (INTEGER): Общее количество лайков
- comments_count (INTEGER): Общее количество комментариев
- reports_count (INTEGER): Общее количество жалоб
- created_at (TIMESTAMPTZ): Время создания записи в базе
- updated_at (TIMESTAMPTZ): Время последнего обновления записи

Таблица "video_snapshots" (Ежечасные снимки статистики видео):
- id (UUID): Идентификатор снимка (снэпшота)
- video_id (UUID): Ссылка на videos.id
- views_count (INTEGER): Количество просмотров на момент снимка
- likes_count (INTEGER): Количество лайков на момент снимка
- comments_count (INTEGER): Количество комментариев на момент снимка
- reports_count (INTEGER): Количество жалоб на момент снимка
- delta_views_count (INTEGER): Прирост просмотров с момента предыдущего снимка
- delta_likes_count (INTEGER): Прирост лайков с момента предыдущего снимка
- delta_comments_count (INTEGER): Прирост комментариев с момента предыдущего снимка
- delta_reports_count (INTEGER): Прирост жалоб с момента предыдущего снимка
- created_at (TIMESTAMPTZ): Время создания снимка (ежечасно)
- updated_at (TIMESTAMPTZ): Время обновления записи

ПРАВИЛА:
1. Верни ТОЛЬКО SQL-запрос. Не включай форматирование markdown (например, ```sql), пояснения или любой другой текст.
2. Запрос должен возвращать ОДНО ЧИСЛО (количество, сумму и т.д.).
3. Используй синтаксис PostgreSQL.
4. Для фильтрации дат по полям TIMESTAMPTZ используй приведение типов `::date`. Пример: `created_at::date = '2025-11-28'`.
5. "Сколько видео" обычно означает `COUNT(*)`.
6. "На сколько выросли просмотры" или "прирост просмотров" означает `SUM(delta_views_count)` из таблицы `video_snapshots`.
7. "Сколько разных видео получили новые просмотры" означает `COUNT(DISTINCT video_id)` из таблицы `video_snapshots` где `delta_views_count > 0`.
8. Будь внимателен с диапазонами дат. "с 1 по 5 ноября включительно" означает `date >= '2025-11-01' AND date <= '2025-11-05'`.
9. Текущий год — 2025, если не указано иное.
10. Если пользователь спрашивает про конкретный id автора (creator_id), используй его в конструкции WHERE.

ПРИМЕРЫ:
User: "Сколько всего видео есть в системе?"
SQL: SELECT COUNT(*) FROM videos;

User: "На сколько просмотров в сумме выросли все видео 28 ноября 2025?"
SQL: SELECT SUM(delta_views_count) FROM video_snapshots WHERE created_at::date = '2025-11-28';

User: "Сколько разных видео получали новые просмотры 27 ноября 2025?"
SQL: SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE created_at::date = '2025-11-27' AND delta_views_count > 0;
"""

    async def generate_sql(self, user_query: str) -> str:
        """Generates a SQL query from a natural language query."""
        try:
            payload = Chat(
                messages=[
                    Messages(role=MessagesRole.SYSTEM, content=self.system_prompt),
                    Messages(role=MessagesRole.USER, content=user_query)
                ],
                temperature=0.1,
            )

            response = await self.client.achat(payload)
            content = response.choices[0].message.content.strip()

            content = re.sub(r'^```sql\s*', '', content, flags=re.IGNORECASE)
            content = re.sub(r'^```\s*', '', content, flags=re.IGNORECASE)
            content = re.sub(r'\s*```$', '', content, flags=re.IGNORECASE)

            content = content.strip()

            logger.info(f"Generated SQL for query '{user_query}': {content}")
            return content

        except Exception as e:
            logger.error(f"Error generating SQL: {e}")
            raise
