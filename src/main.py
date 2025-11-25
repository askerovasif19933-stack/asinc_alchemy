from logger import get_logger
from crud import create_table, insert, process_single_document, create_indexes
from database import async_session
import asyncio

logger = get_logger(__name__)

async def main():
    try:
        # 1. Создаём таблицы
        await create_table()
        # 2. Вставляем данные
        await insert()
        # 3. Создаём индексы
        await create_indexes()

        while True:

            # Пример обработки одного документа
            async with async_session() as session:
                row = await process_single_document(session)
                
                if not row:
                    break
                await session.commit()  # коммит после обработки
        logger.info('Все документы обработаны')
        return True
    
    except Exception as e:
        logger.info(f'Ошибка {e}')
        return False


if __name__ == '__main__':
    print(asyncio.run(main()))




