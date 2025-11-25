from click import echo
from sqlalchemy.orm import sessionmaker , DeclarativeBase
from config import host, password, user
from basse_tz import new_base
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import AsyncSession


# асинхронный

async_engine = create_async_engine(f"postgresql+asyncpg://{user}:{password}@{host}/{new_base}")


class Base(DeclarativeBase):
    pass


async_session = sessionmaker(async_engine, class_= AsyncSession, expire_on_commit=False)





