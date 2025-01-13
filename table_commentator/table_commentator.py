# Copyright (c) 2025 ArtemXYZ
# This project is licensed under the MIT License - see the LICENSE file for details.

"""
    todo: Поправить описание.
    Модуль создания комментариев к таблицам после перезагрузки в них данных.
    Справка:
    В pandas используется метод сохранения данных, где под "капотом" перед сохранением сначала удаляется таблица,
    что приводит к потере метаданных таблицы (комментариев).
"""

# ----------------------------------------------------------------------------------------------------------------------
import re
from typing import Union, List, Dict, Tuple

# ---------------------------------- Импорт сторонних библиотек
from sqlalchemy.engine import Engine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# -------------------------------- Локальные модули
from table_commentator.sql.postgre_sql import *


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

    def __init__(self, engine: Engine, name_table: str, schema: str):  # | AsyncEngine
        self.engine = self._validator(engine, Engine)
        self.name_table: str = self._stop_sql_injections(self._validator(name_table, str))
        self.schema = self._stop_sql_injections(self._validator(schema, str))

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

    @staticmethod
    def _stop_sql_injections(sql_param_string: str) -> str:
        """
            Метод экранирования sql-инъекций комбинирует в себе 2 подхода увеличивая безопасность:
            регулярные выражения и проверка на наличие ключевых sql-команд.

            Проверка регулярного выражения:
                Регулярное выражение ^[a-zA-Z0-9_.\-]+$ разрешает:
                    Строчные и заглавные латинские буквы (a-z, A-Z),
                    Цифры (0-9),
                    Символы _ (подчёркивание),
                    . (точка),
                    - (дефис),
                    Только строки, содержащие эти символы без пробелов, и не допускает пустые строки.

                Это соответствует задаче метода, так как позволяет использовать строки,
                состоящие из обычных идентификаторов, имён таблиц, колонок или других параметров SQL,
                но исключает неподходящие символы, такие как кавычки, пробелы или специальные символы,
                которые могут быть частью SQL-инъекции.
        """
        # Проверка на разрешённые символы
        if not re.match(r'^[a-zA-Z0-9_.\-]+$', sql_param_string):
            raise ValueError("Ошибка! Недопустимый символ в проверяемой строке.")

        # Проверка на наличие sql-ключевых слов
        disallowed_keywords = ["DROP", "CREATE", "ALTER", "INSERT", "UPDATE", "DELETE", "--", ";"]
        if any(keyword in sql_param_string.upper() for keyword in disallowed_keywords):
            raise ValueError("Ошибка! Попытка внедрения sql-инъекции.")

        return sql_param_string

    def _check_all_elements(self, check_type: type, args_elements: any) -> bool:  # *args_elements
        """
            Метод для проверки соответствия условию всех элементов в выборке.
            На входе:
                - check_type: тип данных для проверки (например, str, int).
                - *args_elements: произвольное количество аргументов для проверки.
            На выходе: True, если все элементы соответствуют типу; иначе False.
        """

        # Валидация переданного аргумента (соответствует типу данных) для дальнейшей проверки:
        valid_type = self._validator(check_type, type)
        # print(f"check_type: {check_type}, args_elements: {args_elements}")

        # Проверяем, все ли элементы имеют один и тот же тип:
        return all(isinstance(element, valid_type) for element in args_elements)

    def _sql_formatter(self, sql: str, params: tuple = None) -> text:  #
        """
            Служебный вложенный метод для форматирования sql (передачи необходимых параметров в запрос).
                На входе: необработанный sql.
                    Принимает параметры "table_name" по умолчанию и дополнительные
                    (генерирует фрагмент: строку типа "'param1', 'param2'", подставляя в sql).
                На выходе:
                    отформатированный sql с параметрами, по умолчанию без передачи дополнительных параметров
                    форматируется только table_name (атрибут экземпляра класса).
        """

        valid_sql = self._validator(sql, str)

        # Если указаны параметры, тогда форматируем:
        if params:
            valid_params = self._validator(params, tuple)

            # Имя колонки (сохраняем последовательность через запятую в кавычках:
            columns_string = ', '.join(f"'{columns}'" for columns in valid_params)

            # Форматирование sql_str с учетом параметров
            _mutable_sql = valid_sql.format(table_name=self.name_table, columns=columns_string)

        else:
            mutable_sql = valid_sql.format(table_name=self.name_table)

        return mutable_sql

    def _mutation_sql_by_logic(self, param_column_index_or_name: None | tuple[int, str]) -> str:
        """
           Метод изменения sql запроса в зависимости от переданных параметров
           (содержит логику вариантов форматирования).
                На входе:
                    - param_column_index_or_name: либо только индексы, либо имена колонок.
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
                        sql=SQL_GET_COLUMN_COMMENTS_BY_NAME,
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
                        sql=SQL_GET_COLUMN_COMMENTS_BY_INDEX,
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
            mutable_sql = self._sql_formatter(sql=SQL_GET_ALL_COLUMN_COMMENTS)  # По умолчанию!

        return mutable_sql

    def _create_comment(self, type_comment: str, comment: str, name_column: str = None) -> None:
        """
            Универсальный метод для создания комментариев к различным сущностям в базе данных.
        """

        if type_comment == 'COLUMN':
            if name_column:
                mutable_sql_variant = SQL_SAVE_COMMENT_COLUMN.format(
                    self.PARAMS_SQL.get(type_comment),
                    self.schema,
                    self.name_table,
                    name_column,
                    self._validator(comment, str)
                )
            else:
                raise ValueError(f'Не передано значение для аргумента: name_column.')

        else:
            mutable_sql_variant = SQL_SAVE_COMMENT.format(
                self.PARAMS_SQL.get(type_comment),
                self.schema,
                self.name_table,
                self._validator(comment, str)
            )

            # Добавление комментариев
            # with self.engine.connect() as conn:
            #     conn.execute(text(mutable_sql))

        self.recorder(mutable_sql_variant)

    def _set_column_comment(self, comments_columns_dict: Dict) -> None:
        """
            Метод для создания комментариев к COLUMN.
        """

        if self._validator(comments_columns_dict, Dict):

            for key_name_column, value_comment in comments_columns_dict.items():
                self._create_comment(type_comment='COLUMN', comment=value_comment, name_column=key_name_column)
        else:
            raise ValueError(f'Аргумент "comments_columns_dict" не содержит значения,'
                             f' передано: ({comments_columns_dict}).')

    #  **

    def recorder(self, sql: str | text) -> None:  # list[tuple]
        """
           Метод выполняет запросы к базе данных на запись.
        """

        try:
            engine: Engine = self.engine

            if isinstance(sql, str):
                sql = text(sql)

            with engine.connect() as conn:
                with conn.begin():
                    conn.execute(sql)
        except SQLAlchemyError as e:
            raise RuntimeError(f"Error executing query: {e}")

    def reader(self, sql: str | text) -> list[tuple]:
        """
            Метод выполняет запросы к базе данных на чтение и возвращает данные.
            На входе: sql - sql-запрос в виде строки или объекта sqlalchemy.text.
            На выходе: список картежей или [].
                Возвращает все строки результата в виде списка кортежей.
                Пример результата:
                    [
                        (1, 'Alice'),
                        (2, 'Bob'),
                        (3, 'Charlie')
                    ]
            Примечание:
                При работе с «чистым» sql через session.execute() результат аналогичен fetchall()
                через engine.connect() и возвращает объекты Row.
        """

        engine: Engine = self.engine

        try:

            if isinstance(sql, str):
                sql = text(sql)

            with engine.connect() as conn:
                with conn.begin():
                    result = conn.execute(sql)

        except SQLAlchemyError as e:
            raise RuntimeError(f"Error executing query: {e}")

        # tuple_list = result.fetchall()  # Возвращает объект Row
        tuple_list = [tuple(row) for row in result.fetchall()]  # fetchall()  возвращает [}, если нет данных.

        # Даже если fetchall() вернет пустой список, генератор безопасно вернет [].
        return tuple_list

    def row_sql_recorder(self, sql: str) -> None:
        """
            Метод для создания сырых запросов на запись данных.
        """

        self.recorder(sql)

    def get_type_entity_in_db(self) -> str:
        """
            Метод позволяет определить тип сущности ('table', 'view', 'mview', ...):

           Ищем все сущности (entity) такие как:
            - r = обычная таблица (Relation),
            - i = индекс (Index),
            - S = последовательность (Sequence),
            - t = таблица TOAST,
            - v = представление (View),
            - m = материализованное представление (Materialized view),
            - c = составной тип (Composite type),
            - f = сторонняя таблица (Foreign table),
            - p = секционированная таблица (Partitioned table),
            - I = секционированный индекс (partitioned Index).
        """

        # Определение типа сущности (варианты: 'table', 'view', 'mview'):
        type_entity = self.reader(self._sql_formatter(SQL_CHECK_TYPE_ENTITY))

        return type_entity[0][0] if type_entity else None

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

    def set_materialized_view_comment(self, comment: str) -> None:
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

    def get_table_comments(self) -> dict[str, str]:
        """
            Метод для получения комментариев к таблицам.
            На выходе: str - строка с комментарием к таблице.
        """

        # Проверка корректности типа данных для введенного значения, переменная - имея таблицы:
        # check_name_table = self._validator(table_name, str) - устарело
        mutable_sql = SQL_GET_TABLE_COMMENTS.format(table_name=self.name_table)

        # Получаем сырые данные после запроса (список кортежей):
        table_comment_tuple_list: list[tuple] = self.reader(sql=mutable_sql)

        # Преобразуем (обращаемся к первому элементу в списке (к кортежу, будет всего один всегда) и распаковываем:
        if table_comment_tuple_list:
            table_comment = table_comment_tuple_list[0][0]
        else:
            table_comment = ''  # Если комментарий отсутствует, возвращаем пустую строку.

        return {'table': table_comment}

    def get_column_comments(self, *column_index_or_name: int | str) -> dict[str, dict]:  # dict[str, dict]
        """
            Метод для получения комментариев к колонкам либо по индексу, либо по имени колонки.

            На входе:
                - column_index_or_name - индекс или имя колонки, для которой требуется считать комментарий.
            Важно:
                Если передать в параметры индекс и имя колонки - это вызовет исключение!
                Метод обрабатывает параметры только одного типа (либо только индексы, либо имена колонок).
            На выходе:
               - словарь с вложенным словарем с комментариями: {'columns': {column: comments}}
        """

        # Значение по умолчанию - получаем все комментарии к колонкам в таблице (без указания индекса или имени):
        param_column_index_or_name: tuple[int, str] | None = None or column_index_or_name

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

    def get_all_comments(self) -> dict[str, str | dict]:
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
        table_comment = self.get_table_comments()
        column_comments_dict = self.get_column_comments()

        # Преобразование полученных данных в единый словарь:
        all_comments_table_dict = table_comment | column_comments_dict

        return all_comments_table_dict  # на выходе: {'table': set_table_comment, 'columns': column_comments_dict}

    def save_comments(self, comments_dict: dict[str, str | dict]) -> None:  # Self , schema: str
        """
            Метод для сохранения комментариев, предварительно полученных методами класса из объекта в базе данных
            (таблицы, представления, материализованные представления).

            Назначение:
                Подразумевается сценарий, когда необходима перезапись таблицы в базе данных с ее полным удалением
                (DROP TABLE), например через метод pandas if_exists='append'. После удаления таблицы удаляются и
                ее метаданные (все комментарии к полям и таблице).

                В этом случае, данный метод поможет записать предварительно собранные до удаления таблицы метаданные,
                автоматически определив, какой запрос необходимо выполнить к таблице, представлению или
                к материализованному представлению, а так же самостоятельно определит тип комментария
                (к колонкам или таблице).

            На входе словарь типа:
                {
                    'table': 'set_table_comment',
                    'columns': {
                        'column1': 'column1_comment',
                        'column2': 'column2_comment',
                        'column3': 'column3_comment',
                    }
                }
                или по отдельности его составляющие (либо только словарь с ключом table, либо columns).
        """

        # Если переданный аргумент не пустой:
        if comments_dict:

            # Валидация на тип данных входных аргументов (только если словарь, продолжаем дальше обработку):
            comments_dict = self._validator(comments_dict, dict)

            # Определение типа сущности (варианты: 'table', 'view', 'mview'):
            type_entity = self.get_type_entity_in_db()

            # Проводим валидацию (исключаем работу с сущностями базы данных неподдерживаемыми методом):
            if type_entity not in ('table', 'view', 'mview'):
                raise ValueError(
                    f'Невозможно сохранить комментарий к сущности, указанной в экземпляре класса, '
                    f'метод предусматривает работу только с таблицами, представлениями,'
                    f' материализованными представлениями.')

            # Анализ входных данных:
            for key, value in comments_dict.items():

                # Если во входных данных присутствуют комментарии к сущностям:
                if key == 'table':

                    if type_entity == 'table':
                        self.set_table_comment(value)

                    elif type_entity == 'view':
                        self.set_view_comment(value)

                    elif type_entity == 'mview':
                        self.set_materialized_view_comment(value)

                # Если во входных данных присутствуют комментарии к колонкам сущностей:
                elif key == 'columns':

                    # Проверка на пустоту и валидация:
                    if self._validator(value, Dict):

                        # Принимает словарь, метод принимает dict:
                        self._set_column_comment(value)

                    else:
                        raise ValueError(f'Отсутствуют данные по комментариям для колонок (вложенный словарь пуст),'
                                         f' извлечено: ({value}).')

                else:
                    raise ValueError(f'Переданный аргумент ({comments_dict}) не соответствует требуемой структуре, '
                                     f'установленной в методе для нормальной работы.'
                                     )

        else:
            raise ValueError(f'Отсутствуют данные для обработки. '
                             f'Переданный аргумент ({comments_dict}) не содержит информации')

    def __str__(self):
        return (f'{self.__class__.__name__}(schema: {self.schema},'
                f' name_table: {self.name_table}, engine: {self.engine}).'
                )

    def __repr__(self):
        return (f'{self.__class__.__name__}(schema: {self.schema},'
                f' name_table: {self.name_table}, engine: {self.engine}).'
                )
# ----------------------------------------------------------------------------------------------------------------------