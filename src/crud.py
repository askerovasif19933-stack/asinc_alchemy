
import datetime
from database import Base, async_engine, async_session
from data_filler import make_data, make_documents
from models import Data, Documents
from logger import get_logger
from sqlalchemy import select, and_, update, Index

logger = get_logger(__name__)


# сначала удаляем старое значение потом переприсваиваем, что бы не добавлялись значения подряд
async def create_table():
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    logger.info("Таблицы пересозданы")



data = make_data()
data_tbl = list(data.values())
documents_tbl = make_documents(data)


async def insert():
    async with async_session() as session:

        insert_data = [Data(**i) for i in data_tbl]
        session.add_all(insert_data)

        insert_doc = [Documents(**i) for i in documents_tbl]
        session.add_all(insert_doc)
        await session.commit()

        logger.info(f"Вставлено {len(insert_data)} данных и {len(insert_doc)} документов")





async def create_indexes():
    """Асинхронное создание индексов для ускорения запросов"""
    async with async_engine.begin() as conn:
        # Комбинированный индекс для Documents (processed_at + document_type)
        await conn.run_sync(
            lambda sync_conn: Index(
                'idx_documents_processed_type',
                Documents.processed_at,
                Documents.document_type
            ).create(bind=sync_conn)
        )

        # Хэш-индекс для Data.parent
        await conn.run_sync(
            lambda sync_conn: Index(
                'idx_data_parent_hash',
                Data.parent,
                postgresql_using='hash'
            ).create(bind=sync_conn)
        )

        # Комбинированный индекс для Data.owner + Data.status
        await conn.run_sync(
            lambda sync_conn: Index(
                'idx_data_owner_status',
                Data.owner,
                Data.status
            ).create(bind=sync_conn)
        )

    logger.info('Индексация полей завершена')




async def select_one_doc(session: 'AsyncSession'):
    stmt = (
        select(Documents.doc_id, Documents.document_data)
        .where(
            and_(
                Documents.processed_at.is_(None),
                Documents.document_type == 'transfer_document'
            )
        )
        .order_by(Documents.recieved_at.asc())
        .limit(1)
    )

    row = await session.execute(stmt)
    row = row.first()

    if row:
        logger.info(f"Выбран документ id={row[0]}")
    else:
        logger.info("Нет необработанных документов")

    return row


def parsing_data(row: tuple):
    """Разбираем картеж на doc_id, json, разбирам json на objects, operation_details"""
    doc_id, jsonb = row
    obj = jsonb['objects']
    operation_details = jsonb['operation_details']

    logger.debug(f'Документ {doc_id}, распарсен на {len(obj)} обьектов и {len(operation_details)} операций')

    return doc_id, obj, operation_details



async def search_all_child(session: 'AsyncSession', parent_list: list):
    stmt = select(Data.object).where(Data.parent.in_(parent_list))
    res = await session.execute(stmt)
    child = res.scalars().all()

    parent_child = parent_list + child

    logger.debug(f"Найдено {len(child)} дочерних для {parent_list}")
    return parent_child


async def correct_data(session: 'AsyncSession', objects: list, operation_details: dict):
    if operation_details:
        for operation, details in operation_details.items():
            new = details["new"]
            old = details["old"]

            column = getattr(Data, operation)

            stmt = (
                update(Data)
                .where(column == old, Data.object.in_(objects))
                .values({operation: new})
            )

            result = await session.execute(stmt)

            logger.info(
                f"Обновлено поле {operation}: {old} → {new} "
                f"для {result.rowcount} объектов"
            )



async def set_processing_time(session: 'AsyncSession', doc_id: str):
    document = await session.get(Documents, doc_id)
    document.processed_at = datetime.datetime.now()

    logger.info(f"Документ {doc_id} отмечен как обработанный")


async def process_single_document(session: 'AsyncSession'):
    """Обрабатывает один документ, session передаётся из main"""
    row = await select_one_doc(session)
    if not row:
        logger.info("Документы для обработки отсутствуют")
        return None

    doc_id, objects, operation_details = parsing_data(row)
    all_parents = await search_all_child(session, objects)
    await correct_data(session, all_parents, operation_details)
    await set_processing_time(session, doc_id)
    return doc_id



