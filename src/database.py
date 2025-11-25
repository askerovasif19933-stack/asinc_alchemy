
from sqlalchemy.orm import sessionmaker , DeclarativeBase
from config import host, password, user, port
from basse_tz import new_base
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import AsyncSession


# асинхронный

async_engine = create_async_engine(
    f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{new_base}",
    echo=False,  # для дебага SQL запросов
    pool_pre_ping=True  # проверка соединения
)
class Base(DeclarativeBase):
    pass


async_session = sessionmaker(async_engine, class_= AsyncSession, expire_on_commit=False)








