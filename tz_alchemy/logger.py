import logging



def get_logger(name: str, log_file: str = 'add.log', level = logging.INFO):

    """
    Возвращает настроенный логгер.
    - name: имя логгера (обычно __name__ модуля)
    - log_file: имя файла для логов
    - level: уровень логирования
    """

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:

        # консоль
        consol_handler = logging.StreamHandler()
        consol_handler.setLevel(level)

        # файл

        file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        file_handler.setLevel(level)


        # фармат 

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        consol_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        logger.addHandler(consol_handler)
        logger.addHandler(file_handler)

    return logger