import csv
import sqlite3
import logging
from time import time

logging.basicConfig(
    filename = "convertor.log",
    format = "%(asctime)s - %(module)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s",
    datefmt='%H:%M:%S',
    level=logging.INFO,
)


class LoadingData:
    """
    Класс для загрузки данных из csv в базу данных

    Class for load data from csv into data base
    """

    def __init__(self, dbase_name: str, table_name: str, name_file: str, *args, **kwargs) -> None:
        self.dbase_name = dbase_name
        self.table_name = table_name
        self.name_file = name_file
        

        # Подключаемся к базе данных / Connecting to the database
        self.connection = sqlite3.connect(self.dbase_name)

        # Открываем файл на чтение / Open file for reading
        logging.info('Считывание данных с файла')
        self._file = open(self.name_file)
        self.content = list(csv.reader(self._file))


        # Получаем название столбцов / 
        self._column = self.transform_column()

        # Выполнить загрузку из файла / Load from file

        logging.info('Выполняем загрузку структуры')
        self.execute_script()

    def execute_script(self) -> None:
        """
        Выполняем загрузку данных

        Performing data loading
        """
        # Подключаемся к базе / Connecting to the base
        cursor = self.connection.cursor()
        start_time = time()
        logging.info('Выполняем подключение к базе')
        # Выполняем создание таблицы исходя из структуры файла / Create table based on file structure
        cursor.execute(self.create_structure())
        logging.info('Выполняем создание таблицы исходя из структуры файла')
        
        # Выполняем запись данных / Performing data recording
        for row in self.content[1:]:
            cursor.execute(
                self.insert_structure(),
                tuple(row),
            )

        logging.info(f'Запись прошла успешна!')
        logging.info(f"Затраченное время на выполнения скрипта {time() - start_time}")
        logging.info(f"Кол-во записей: {len(self.content) - 1}")
            

    def insert_structure(self) -> str:
        """
        Метод генерирует вставку данных изходя из столбцов файла

        The method generates an insertion of data based on the columns of the file
        """

        return f'''
            INSERT INTO {self.table_name} ({', '.join([_ for _ in self._column])}) VALUES({', '.join(['?' for _ in range(len(self._column))])})
        '''

    def create_structure(self) -> str:
        """
        The method generates the table creation structure

        Метод генерирует структуру созданиея таблицы
        """
        sql_fields = ",\n".join([f"{sql_structure} TEXT" for sql_structure in self._column[1:]])

        create_table = f'''
            CREATE TABLE {self.table_name}(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                {sql_fields}
            );
        '''
        return create_table

    def transform_column(self) -> list:
        """
        Преобразует название столбцов

        Converts column names
        """
        
        structure = self.content[0]
        return [_.lower().replace(' ', '_') for _ in structure]


    def __del__(self) -> None:
        """Закрытие потока / Closing a stream"""
        
        self._file.close()

        # Сохраняем все изменения / Save all changes
        self.connection.commit()
        self.connection.close()
