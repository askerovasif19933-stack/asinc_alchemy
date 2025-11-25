from config import host, password, user
from sqlalchemy import text, create_engine
from logger import get_logger

logger = get_logger(__name__)


db_name = 'postgres'
new_base = 'tz_base'

engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/{db_name}", isolation_level='AUTOCOMMIT')




def get_new_base(base: str):
    try:
        with engine.begin() as conn:
            conn.execute(text(f"CREATE DATABASE {base}"))
        logger.info('база создана')
    except Exception as e:
        logger.info(e)


if __name__ == '__main__':
    get_new_base(new_base)