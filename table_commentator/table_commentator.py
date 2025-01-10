"""
    Модуль создания комментариев к таблицам после перезагрузки в них данных.
    Справка:
    В Пандас используется метод сохранения данных, где под "капотом" перед сохранением сначала удаляется таблица,
    что приводит к потере метаданных таблицы (комментариев).
"""

# ----------------------------------------------------------------------------------------------------------------------
from typing import Union, List, Self
# ---------------------------------- airflow
# ---------------------------------- Импорт сторонних библиотек
from sqlalchemy.engine import Engine, Row
from sqlalchemy import text
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL


# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from sqlalchemy.ext.asyncio import AsyncEngine

# -------------------------------- Локальные модули

# ----------------------------------------------------------------------------------------------------------------------
class TableCommentator:
    """
        Класс для создания комментариев к таблицам и колонкам после перезагрузки в них данных.
        Справка:
        В Пандас используется метод сохранения данных, где под "капотом" перед сохранением сначала удаляется таблица,
        что приводит к потере метаданных таблицы (комментариев).
    """

    PARAMS_SQL = {
        'TABLE': 'TABLE',
        'VIEW': 'VIEW',
        'MATERIALIZED': 'MATERIALIZED VIEW',
        'COLUMN': 'COLUMN',
    }

    # COMMENT ON TABLE "schema"."table" IS 'comment'
    SQL_SAVE_COMMENT = """COMMENT ON {0} "{1}"."{2}" IS '{3}'"""  # TABLE

    # COMMENT ON COLUMN "schema"."table"."name_table" IS 'comment'
    SQL_SAVE_COMMENT_COLUMN = """COMMENT ON {0} "{1}"."{2}"."{3}" IS '{4}'"""  # COLUMN

    SQL_GET_TABLE_COMMENTS = """
        SELECT
            comments.description AS description
        FROM
            pg_class AS all_entity
        INNER JOIN
            pg_description AS comments
        ON 
            all_entity.oid = comments.objoid 
        AND 
            comments.objsubid = 0 
        AND 
            all_entity.relname = '{table_name}'
    """

    SQL_GET_ALL_COLUMN_COMMENTS = """
        SELECT
            cols.attname AS column_name, 
            comments.description AS description
        FROM
            pg_class AS all_entity
        INNER JOIN
            pg_description AS comments
        ON 
            all_entity.oid = comments.objoid 
        AND 
            comments.objsubid > 0 
        AND 
            all_entity.relname = '{table_name}'
        INNER JOIN 
            pg_attribute AS cols                    
        ON
            cols.attnum = comments.objsubid 
        AND
            cols.attrelid = all_entity.oid      
    """

    SQL_GET_COLUMN_COMMENTS_BY_INDEX = """
        SELECT 
            cols.attname AS column_name,       
            comments.description AS description         
        FROM 
            pg_class AS all_entity
        INNER JOIN 
            pg_description AS comments              
        ON 
            all_entity.oid = comments.objoid
        AND
            comments.objsubid > 0
        AND
            all_entity.relname = '{table_name}'
        INNER JOIN 
            pg_attribute AS cols                  
        ON
            cols.attnum = comments.objsubid 
        AND
            cols.attrelid = all_entity.oid      
        WHERE 
             comments.objsubid IN ({columns})
    """

    SQL_GET_COLUMN_COMMENTS_BY_NAME = """
        SELECT 
            cols.attname AS column_name, 
            comments.description AS description    
        FROM 
            pg_class AS all_entity
        INNER JOIN 
            pg_description AS comments             
        ON
            all_entity.oid = comments.objoid 
        AND 
            comments.objsubid > 0              
        AND
            all_entity.relname = '{table_name}'  
        INNER JOIN 
            pg_attribute AS cols                    
        ON
            cols.attnum = comments.objsubid 
        AND
            cols.attrelid = all_entity.oid
        WHERE
             cols.attname IN ({columns})
    """

    def __init__(self, engine: Engine, name_table: str, schema: str):  # | AsyncEngine
        self.engine = self._validator(engine, Engine)
        self.name_table: str = self._validator(name_table, str)
        self.schema = self._validator(schema, str)
        # ***
        self.table_comments_dict: Union[dict[str, str], None] = None
        self.column_comments_dict: Union[dict[str, str], None] = None
        self.all_comments_table_dict: Union[dict[str, str], None] = None

    # ***

    @staticmethod
    def _validator(value: any, check_type: any) -> any:
        """
            Проверка на корректную передачу входных параметров с требуемым типом данных.
        """
        if isinstance(value, check_type):
            return value
        else:
            raise TypeError(f'Недопустимый тип данных для аргумента: {value}.')

    def _check_all_elements(self, check_type: type, args_elements: any) -> bool:  # *args_elements
        """
            Метод для проверки соответствия условию всех элементов  в выборке.
            На входе:
                - check_type: тип данных для проверки (например, str, int).
                - *args_elements: произвольное количество аргументов для проверки.
            На выходе: True, если все элементы соответствуют типу; иначе False.
        """

        # Валидация переданного аргумента (соответствует типу данных) для дальнейшей проверки:
        valid_type = self._validator(check_type, type)
        print(f"check_type: {check_type}, args_elements: {args_elements}")

        # Проверяем, все ли элементы имеют один и тот же тип:
        return all(isinstance(element, valid_type) for element in args_elements)

    def _sql_formatter(self, sql: str, params: tuple = None) -> str:  #
        """
            Служебный вложенный метод для форматирования sql (передачи необходимых параметров в запрос).
                На входе:
                На выходе: .
        """

        valid_sql = self._validator(sql, str)

        # Если указаны параметры, тогда форматируем:
        if params:
            # valid_table_name = self._validator(table_name, str) - устарело
            valid_params = self._validator(params, tuple)

            # Имя колонки (сохраняем последовательность через запятую в кавычках:
            columns_string = ', '.join(f"'{columns}'" for columns in valid_params)

            # Форматирование sql_str с учетом параметров:
            mutable_sql = valid_sql.format(table_name=self.name_table, columns=columns_string)
        else:
            mutable_sql = valid_sql.format(table_name=self.name_table)

        return mutable_sql

    def _mutation_sql_by_logic(self, param_column_index_or_name: tuple[Union[int, str]]) -> str:  # , table_name: str
        """
           Метод изменения sql запроса в зависимости от переданных параметров
           (содержит логику вариантов форматирования).
                На входе:
                    - param_column_index_or_name: либо только индексы либо имена колонок.
                    - table_name: мя таблицы к которой выполняется запрос.
                На выходе: готовый запрос.
        """

        # Если были указаны параметры на конкретные колонки:
        if param_column_index_or_name:

            #  Проверка первого элемента на тип данных (исключает дублирования проверок):
            if isinstance(param_column_index_or_name[0], str):  # check_first_itm_type

                # Если вводим имя колонки (хотим получить комментарий для колонки по ее имени):
                if self._check_all_elements(str, param_column_index_or_name):

                    # Форматирование sql_str с учетом параметров:
                    mutable_sql = self._sql_formatter(
                        sql=self.SQL_GET_COLUMN_COMMENTS_BY_NAME,
                        # table_name=self.name_table, - устарело.
                        params=param_column_index_or_name
                    )

                # Если не все элементы имеют один и тот же тип или недопустимые:
                else:
                    raise TypeError(
                        f'Переданные аргументы не соответствуют единому типу данных, '
                        f'должны быть либо только str (имена колонок),'
                        f' либо только int (индексы колонок).'
                    )

            elif isinstance(param_column_index_or_name[0], int):

                # Если вводим индекс колонки (хотим получить комментарий для колонок по индексам):
                if self._check_all_elements(int, param_column_index_or_name):

                    # Форматирование sql_str с учетом параметров:
                    mutable_sql = self._sql_formatter(
                        sql=self.SQL_GET_COLUMN_COMMENTS_BY_INDEX,
                        # table_name=self.name_table, - устарело.
                        params=param_column_index_or_name
                    )
                # Если не все элементы имеют один и тот же тип или недопустимые:
                else:
                    raise TypeError(
                        f'Переданные аргументы не соответствуют единому типу данных, '
                        f'должны быть либо только str (имена колонок),'
                        f' либо только int (индексы колонок).'
                    )

            # Если элементы не соответствуют допустимым типам данных:
            else:
                raise TypeError(
                    f'Переданные аргументы должны быть либо str (имена колонок), либо int (индексы колонок), '
                    f'другие типы данных недопустимы.'
                )
        else:
            # Если не вводим ни имя колонки, ни индекс (хотим получить все комментарии ко всем существующим колонкам):
            mutable_sql = self._sql_formatter(sql=self.SQL_GET_ALL_COLUMN_COMMENTS)  # По умолчанию!

        return mutable_sql

    def _create_comment(self, type_comment: str, comment: str, name_column: str = None) -> None:
        """
            Универсальный метод для создания комментариев к различным сущностям в базе данных.
        """

        if type_comment == 'COLUMN':
            if name_column is not None:
                mutable_sql_variant = self.SQL_SAVE_COMMENT_COLUMN.format(
                    self.PARAMS_SQL.get(type_comment),
                    self.schema,
                    self.name_table,
                    name_column,
                    self._validator(comment, str)
                )
            else:
                raise ValueError(f'Не передано значение для аргумента: name_column.')

        else:
            mutable_sql_variant = self.SQL_SAVE_COMMENT.format(
                self.PARAMS_SQL.get(type_comment),
                self.schema,
                self.name_table,
                self._validator(comment, str)
            )

            # Добавление комментариев
            # with self.engine.connect() as conn:
            #     conn.execute(text(mutable_sql))

        self.recorder(mutable_sql_variant)

    def _get_table_comments(self) -> dict[str, str]:  # , table_name: str
        """
            Метод (приватный) для получения комментариев к таблицам.
            На выходе: str, - строка с комментарием к таблице.
        """

        # Проверка корректности типа данных для введенного значения, переменная - имея таблицы:
        # check_name_table = self._validator(table_name, str) - устарело
        mutable_sql = self.SQL_GET_TABLE_COMMENTS.format(table_name=self.name_table)

        # Получаем сырые данные после запроса (список кортежей):
        table_comment_tuple_list: list[tuple] = self.reader(sql=mutable_sql)

        # Преобразуем (обращаемся к первому элементу в списке (к кортежу, будет всего один всегда) и распаковываем:
        if table_comment_tuple_list:
            table_comment = table_comment_tuple_list[0][0]
        else:
            table_comment = ''  # Если комментарий отсутствует, возвращаем пустую строку.

        return {'table': table_comment}

    def _get_column_comments(self, *column_index_or_name: Union[int, str]) -> dict[str, dict]:  # Union[int, str]
        """
            Метод для получения комментариев к колонкам либо по индексу, либо по имени колонки.
            Важно:
                Если передать в параметры индекс и имя колонки - это вызовет исключение!
                Метод обрабатывает параметры только одного типа (либо только индексы, либо имена колонок).
            На выходе:
                словарь с вложенным словарем с комментариями: {'columns': {column: comments}}

        """

        # Значение по умолчанию - получаем все комментарии к колонкам в таблице (без указания индекса или имени):
        param_column_index_or_name = None or column_index_or_name

        # Проверка корректности типа данных для введенного значения, переменная - имея таблицы:
        # check_name_table = self._validator(self.name_table, str) -  устарело

        # Изменение sql запроса в зависимости от переданных параметров
        mutable_sql = self._mutation_sql_by_logic(param_column_index_or_name)  # , table_name: str

        # Преобразование sql_str в тип данных sqlalchemy:
        final_sql = text(mutable_sql)

        column_comments_tuple_list: list[tuple] = self.reader(sql=final_sql)

        # Генерация словаря из списка кортежей:
        # Распаковывает кортеж из 2-х элементов (1, 'Alice') принимая первый за key и второй за value:
        _column_comments_dict = {key: value for key, value in column_comments_tuple_list}

        return {'columns': _column_comments_dict}  # {'columns': {column: comments} }

    #  **

    def recorder(self, sql: Union[str, text]) -> None:  # sql | text # executer | list[tuple] Union[int, str]
        """
           Метод выполняет запросы к базе данных на запись.
        """
        engine: Engine = self.engine

        if isinstance(sql, str):
            sql = text(sql)

        with engine.connect() as conn:
            with conn.begin():
                conn.execute(sql)

    def reader(self, sql: Union[str, text]) -> list[tuple]:  # dict[str, text] list[Row]
        """
            Метод выполняет запросы к базе данных на чтение и возвращает данные.
            На входе: sql - SQL-запрос в виде строки или объекта sqlalchemy.text.
            На выходе: список картежей или [].
                Возвращает все строки результата в виде списка кортежей.
                Пример результата:
                    [
                        (1, 'Alice'),
                        (2, 'Bob'),
                        (3, 'Charlie')
                    ]
            Примечание:
                При работе с «чистым» SQL через session.execute() результат аналогичен fetchall()
                через engine.connect() и возвращает объекты Row.
        """

        engine: Engine = self.engine

        with engine.connect() as conn:
            with conn.begin():
                result = conn.execute(sql)
                # tuple_list = result.fetchall()  # Возвращает объект Row
                tuple_list = [tuple(row) for row in result.fetchall()]  # Возвращает tuple_list
        return tuple_list  # Может заменить на self

    def row_sql_recorder(self, sql: str) -> None:
        """
            Метод для создания сырых запросов на запись данных.
        """

        self.recorder(sql)

    # *

    def set_table_comment(self, comment: str) -> None:
        """
            Метод для создания комментариев к TABLE.
        """

        self._create_comment(type_comment='TABLE', comment=comment)

    def set_view_comment(self, comment: str) -> None:
        """
            Метод для создания комментариев к VIEW.
        """

        self._create_comment(type_comment='VIEW', comment=comment)

    def set_materialized_comment(self, comment: str) -> None:
        """
            Метод для создания комментариев к MATERIALIZED VIEW.
        """

        self._create_comment(type_comment='MATERIALIZED', comment=comment)

    def set_column_comment(self, **comments_columns: str) -> None:
        """
            Метод для создания комментариев к COLUMN.
        """

        for key_name_column, value_comment in comments_columns.items():
            self._create_comment(type_comment='COLUMN', comment=value_comment, name_column=key_name_column)

    #  *

    def get_table_comments(self) -> Self:  # или 'TableCommentator' на Python < 3.11., table_name: str
        """
            Метод (публичный) для получения комментариев к таблицам.
            На выходе: str, - строка с комментарием к таблице.
        """

        self.table_comments_dict = self._get_table_comments()
        return self

    def get_column_comments(self, *column_index_or_name: str) -> Self:  # -> dict[str, text] , table_name: str
        """
            Метод (публичный) для получения комментариев к колонкам либо по индексу, либо по имени колонки.
                Если передать в параметры индекс и имя колонки - это вызовет исключение!
                Метод обрабатывает параметры только одного типа (либо только индексы, либо имена колонок).
            На выходе - словарь с комментариями

        """

        self.column_comments_dict = self._get_column_comments(column_index_or_name)
        return self

    def get_all_comments(self) -> Self:  # , schema: str
        """
            Метод для получения всех комментариев к колонкам и таблицам.
            На выходе - словарь типа:
                result = {
                    'table': 'set_table_comment',
                    'columns': {
                        'column1': 'column1_comment',
                        'column2': 'column2_comment',
                        'column3': 'column3_comment',
                    }
                }.
        """

        # Получение всех комментариев:
        table_comment = self._get_table_comments()
        column_comments_dict = self._get_column_comments()

        # Преобразование полученных данных в единый словарь:
        # на выходе: {'table': set_table_comment, 'columns': column_comments_dict}
        self.all_comments_table_dict = table_comment | column_comments_dict
        return self

# ----------------------------------------------------------------------------------------------------------------------
