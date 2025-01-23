# Copyright (c) 2025 ArtemXYZ
# This project is licensed under the MIT License - see the LICENSE file for details.

"""
    Основной модуль библиотеки "Tcommenter" предназначенной для создания комментариев к таблицам (и другим сущностям)
    в базе данных (в текущей версии библиотеки, только для PostgreSQL).
"""

# ----------------------------------------------------------------------------------------------------------------------
import re
from typing import TypeVar # , Union

# ---------------------------------- Импорт сторонних библиотек
from sqlalchemy.engine import Engine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.elements import TextClause

# -------------------------------- Локальные модули
from .sql.postgre_sql import *

any_types = TypeVar('any_types')  # Создаем обобщённый тип данных.
# ----------------------------------------------------------------------------------------------------------------------
class Tcommenter:
    """
        Основной класс библиотеки, содержащий все необходимые методы для создания, извлечения и перегрузки
        (перед "DROP TABLE") комментариев к таблицам (и другим сущностям), колонкам в базе данных (в текущей версии
        библиотеки, только для PostgreSQL).
    """

    # Обеспечивает безопасный ввод параметров конструкции SQL для типа сущности:
    PARAMS_SQL = {
        'TABLE': 'TABLE',
        'VIEW': 'VIEW',
        'MATERIALIZED': 'MATERIALIZED VIEW',
        'COLUMN': 'COLUMN',
    }

    def __init__(self, engine: Engine, name_table: str, schema: str):  # | AsyncEngine
        self.engine = self._validator(engine, Engine)
        self.name_entity: str = self._stop_sql_injections(self._validator(name_table, str))
        self.schema = self._stop_sql_injections(self._validator(schema, str))

    @staticmethod
    def _validator(value: any_types, *check_type: type[any]) -> any_types:
        """
            *** Приватный метод валидации (основной). ***

            Предназначен для проверки корректной передачи аргументов в соответствии с требуемым типом данных.
            Используется как вложенный метод в других участках кода библиотеки.

            * Описание механики:
                В соответствии с переданным набором допустимых типов данных через параметр "*check_type",
                осуществляется сверка проверяемого аргумента "value" на основе метода "isinstance()".
                В случае соответствия хотя бы одному из набора типов данных, возвращается переданный аргумент "value",
                иначе, вызывается исключение TypeError.

            ***

            * Пример вызова:

                params = self._validator(_params, dict)

            ***

            :param value: Значение, которое требуется проверить.
            :param check_type: Один или несколько типов данных, допустимых для значения value.
            :return: Возвращает значение value, если оно соответствует одному из типов check_type.
            :rtype: any_types, Возвращается тип исходный тип данных проверяемого аргумента.
            :raises TypeError: Если значение value не соответствует ни одному из указанных типов данных.
        """

        if isinstance(value, check_type):
            return value
        else:
            raise TypeError(f'Недопустимый тип данных: "{type(value).__name__}", для аргумента: "{value}".')

    def _stop_sql_injections(self, sql_param_string: str) -> str:
        """
            *** Приватный метод экранирования запросов от sql-инъекций. ***

                Предназначен для предотвращения передачи sql-инъекций в основной запрос других методов через аргументы
                экземпляра класса. Комбинирует в себе два подхода увеличивая безопасность:

                    - регулярные выражения
                    - проверка на наличие ключевых sql-команд.

            * Описание механики:

                1) Проверка на разрешённые символы:
                Регулярное выражение ^[a-zA-Z0-9_.\\-]+$ не пропускает пустые строки и другие выражения не
                соответствующие разрешенным, вызывая исключение ValueError. Допускаются только строки, содержащие
                эти символы без пробелов:

                    - строчные и заглавные латинские буквы (a-z, A-Z)
                    - цифры (0-9)
                    - подчёркивание
                    - точка
                    - дефис

                Это позволяет использовать строки, состоящие из обычных идентификаторов, имён таблиц, колонок
                или других параметров SQL, но исключает неподходящие символы, такие как кавычки, пробелы или
                специальные символы, которые могут быть частью SQL-инъекции.

                2) Проверка на наличие ключевых SQL-команд:
                После проверки символов строка дополнительно анализируется на наличие зарезервированных SQL-ключевых
                слов, которые могут быть использованы в инъекциях. Если строка содержит любые из следующих запрещённых
                слов, будет вызвано исключение ValueError:

                    - DROP
                    - CREATE
                    - ALTER
                    - INSERT
                    - UPDATE
                    - DELETE
                    - Комментарий SQL (--)
                    - Символ завершения команды (;)

            Это позволяет предотвратить использование строк, содержащих вредоносные SQL-конструкции, которые могут
            повредить структуру базы данных или её содержимое.


            ***

            * Пример вызова:

                self.name_entity: str = self._stop_sql_injections(self._validator(name_table, str))

            ***

            :param sql_param_string: Строка, которая передаётся для проверки на безопасность перед использованием \
                в SQL-запросе.
            :return: Проверенная строка, безопасная для использования в SQL-запросе.
            :rtype: str.
            :raises ValueError: Если строка содержит недопустимые символы или ключевые слова SQL, \
                которые могут быть частью инъекции.
        """

        sql_param_string = self._validator(sql_param_string, str)

        # Проверка на разрешённые символы:
        if not re.match(r'^[a-zA-Z0-9_.\-]+$', sql_param_string):
            raise ValueError("Ошибка! Недопустимый символ в проверяемой строке.")

        # Проверка на наличие sql-ключевых слов:
        disallowed_keywords = ["DROP", "CREATE", "ALTER", "INSERT", "UPDATE", "DELETE", "--", ";"]
        if any(keyword in sql_param_string.upper() for keyword in disallowed_keywords):
            raise ValueError("Ошибка! Попытка внедрения sql-инъекции.")

        return sql_param_string

    def _check_all_elements(self, check_type: type, args_array: dict | list | tuple) -> bool:  # *args_elements
        """
            *** Приватный метод валидации всего набора аргументов на соответствие единому типу данных. ***

            Предназначен для обеспечения корректной передачи всего переданного набора аргументов "args_array"
            в соответствии требуемому (указанному к проверке в "check_type") типу данных. Используется как
            вложенный метод в других участках кода библиотеки, для управления логикой дальнейшей обработки данных
            (перенаправление данных в блоки условий).

            * Описание механики:
                В соответствии с переданным типом данных (допустимым) через параметр "check_type", осуществляется
                сверка проверяемого массива аргументов "args_array" на основе совместной работы методов "all" и
                "isinstance". Предварительно осуществляется валидация с помощью метода "self._validator()" на допустимые
                значения для аргументов:
                    "check_type" - проверка передачи типа данных, например "str", не допустимое значение,
                        например "test";
                    "args_array" - будет проверен на соответствие dict | list | tuple.

            ***

            * Пример вызова:

                if self._check_all_elements(str, param_column_index_or_name):
                    pass

            ***

            :param check_type: Один или несколько типов данных, допустимых для значения value.
            :param args_array: Массив аргументов для проверки поэлементно, допустимые типы только dict | list | tuple.
            :return: Возвращает True, если "args_array" соответствует типу "check_type" \
                и пройдена валидация, иначе False.
            :rtype: bool.
            :raises: Возможны исключения во вложенных служебных методах (подробно смотрите в их описании).
        """

        # Валидация переданного аргумента (соответствует типу данных) для дальнейшей проверки:
        valid_type = self._validator(check_type, type)
        # Разрешенные типы для args_array:
        valid_args_array = self._validator(args_array, (dict, list, tuple,))

        # Проверяем, все ли элементы имеют один и тот же тип:
        return all(isinstance(element, valid_type) for element in valid_args_array)

    def _insert_params_in_sql(self, sql: str, **sql_params) -> str:
        """
            *** Приватный метод для подстановки в sql запросы имени сущности и других параметров при необходимости. ***

            В основном, данный метод предназначен для вставки значения "self.name_entity" (имени таблицы |
            материализованного представления | представления), получаемого из  "__init__". Данный метод
            не предоставляет проверки на sql-инъекций, self.name_entity" проходит проверку при инициализации класса.
            Использовать "**sql_params" без предварительной проверки в "self._stop_sql_injections" небезопасно.
            Используется как вложенный метод в других участках кода библиотеки.

            * Описание механики:
                Если отсутствуют дополнительные аргументы "**sql_params", то по умолчанию происходит подстановка в sql
                self.name_entity", иначе осуществляется форматирование с учетом "**sql_params". Результатом работы
                будет возврат успешно отформатированного sql. В случае некорректно переданного имени плейсхолдера,
                вызывается исключение ValueError.

            ***

            * Пример вызова:

                sql = self._insert_params_in_sql(sql_blank)
                sql = self._insert_params_in_sql(sql_blank, shem_name=shem_name_value)

            ***

            :param sql: Шаблон sql с плейсхолдерами.
            :param sql_params: Именованные аргументы.
            :return: Возвращает отформатированный sql (по умолчанию с именем сущности).
            :rtype: str.
            :raises ValueError: Вызванный KeyError, в случае некорректно переданного имени плейсхолдера,
                а так же возможны другие исключения во вложенных служебных методах (подробно смотрите в их описании).
        """

        valid_sql = self._validator(sql, str)

        try:
            if not sql_params:
                # Проверка на инъекции пройдена при инициализации:
                fin_sql = valid_sql.format(name_entity=self.name_entity)

            else:
                fin_sql = valid_sql.format(name_entity=self.name_entity, **sql_params)

            return fin_sql

        # Ошибка не возникнет если, все существующие ключи совпадут, а излишние игнорируются.
        except KeyError:
            raise ValueError(f'Ошибка форматирования sql-запроса: переданный ключ не найден.')  # {error}

    def _generate_params_list_for_sql(self, params: tuple[int | str] = None) -> list[int | str]:
        """
            *** Приватный метод для генерации списка имен колонок или их индексов. ***

            Предназначен для генерации списка имен колонок или их индексов, используемых в sql запросах в качестве
            параметров (для дальнейшей передачи через multyparams в conn.execute(sql, multyparams)).
            Используется как вложенный метод в других участках кода библиотеки.

            * Описание механики:
                Преобразование картежа (образованного от *args из других методов) в список. Сначала валидация входных
                аргументов, далее работа list comprehension.

            ***

            * Пример вызова:

                params_list = self._generate_params_list_for_sql(params=param_column_index_or_name)

            ***

            :param params: Значения параметров для подстановки в sql.
            :return: Список параметров.
            :rtype: list[int | str].
            :raises: Возможны другие исключения во вложенных служебных методах (подробно смотрите в их описании).
        """

        valid_params: tuple = self._validator(params, tuple)
        return [columns for columns in valid_params]

    def _get_sql_and_params_list_only_from_indexes_or_names(
            self,
            param_column_index_or_name: tuple[int | str] | None
    ) -> tuple[str, list[int | str]]:
        """
            *** Приватный метод валидации имен колонок или их индексов передаваемых в запрос в качестве параметров. ***

            Предназначен для обеспечения корректной передачи параметров (через multyparams в conn.execute(sql,
            multyparams)) в запрос, подчиняющейся логике: в параметры попадают либо только имена колонок, либо только
            их индексы. Это позволяет избежать дублирования передаваемых параметров (исключается сценарий, когда
            пользователь вводит и численное представление колонки (индекс) и строковое (имя колонки)), что упрощает
            обработку, конкретизируя поведение метода.

            * Описание механики:
                В зависимости от типа переданных данных (либо имена колонок, либо индексы), метод возвращает их же, но
                в виде списка и соответствующий sql запрос для записи комментариев к колонкам.
                На более низком уровне происходит валидация на соответствие единому типу данных во всем массиве данных:
                либо только str, либо только int, что обеспечивает верхнеуровневую логику.
            Используется как вложенный метод в других участках кода библиотеки.

            ***

            * Пример вызова:

                # Получаем sql (или для имен, или для индексов) и список параметров:
                sql, params_list_only_from_indexes_or_name = self._get_sql_and_params_list_only_from_indexes_or_names(
                    param_column_index_or_name
                )

            ***

            :param param_column_index_or_name: Значения параметров для подстановки в sql (или индексы, \
                или имена колонок, или смешанно.
            :return: Sql запрос (или для записи по имени колонки или по индексу) и список параметров (гарантированно
            либо только индексы, либо только имена колонок).
            :rtype: tuple[str, list[int | str]].
            :raises: Возможны другие исключения во вложенных служебных методах (подробно смотрите в их описании).
        """

        # Если были указаны параметры на конкретные колонки:
        if param_column_index_or_name:

            #  Проверка первого элемента на тип данных (исключает дублирования проверок):
            if isinstance(param_column_index_or_name[0], str):  # check_first_itm_type

                # Если вводим имя колонки (хотим получить комментарий для колонки по ее имени):
                if self._check_all_elements(str, param_column_index_or_name):

                    # Соответствующий SQL и последовательность имен колонок (через запятую в кавычках): strparams.
                    return SQL_GET_COLUMN_COMMENTS_BY_NAME, self._generate_params_list_for_sql(
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

                    # Соответствующий SQL и последовательность имен колонок (через запятую в кавычках): strparams.
                    return SQL_GET_COLUMN_COMMENTS_BY_INDEX, self._generate_params_list_for_sql(
                        params=param_column_index_or_name
                    )

                # Если не все элементы имеют один и тот же тип или недопустимые:
                else:
                    raise TypeError(
                        f'Переданные аргументы не соответствуют единому типу данных, '
                        f'должны быть либо только str (имена колонок),'
                        f' либо только int (индексы колонок).'
                    )

    def _reader(self, sql: str | TextClause, **params: str | int | list) -> list[tuple]:
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

        _params = params or None
        engine: Engine = self.engine

        try:

            if isinstance(sql, str):
                sql = text(sql)

            with engine.connect() as conn:
                with conn.begin():
                    if _params:
                        _params: dict = self._validator(_params, dict)
                        result = conn.execute(sql, _params)
                    else:
                        result = conn.execute(sql)
        except SQLAlchemyError as e:
            raise RuntimeError(f"Error executing query: {e}")

        # tuple_list = result.fetchall()  # Возвращает объект Row
        tuple_list = [tuple(row) for row in result.fetchall()]  # fetchall()  возвращает [}, если нет данных.

        # Даже если fetchall() вернет пустой список, генератор безопасно вернет [].
        return tuple_list


    def _recorder(self, sql: str | TextClause, **params: None | str | int) -> None:
        """
            Метод выполняет запросы к базе данных на запись.

            Если запрос использует именованные параметры (например, :param),
            то multiparams должен содержать **один или не один или несколько словарей.


            Как работает multiparams:
            multiparams принимает позиционные аргументы (*args), которые могут быть:
            словари (для именованных параметров),
            кортежи (для позиционных параметров, если запрос использует позиции вместо имён).
        """

        _params = params or None
        engine: Engine = self.engine

        try:

            if isinstance(sql, str):
                sql = text(sql)

            with engine.connect() as conn:
                with conn.begin():

                    if _params:
                        _params: dict = self._validator(_params, dict)
                        conn.execute(sql, _params)
                    else:
                        conn.execute(sql)
        except SQLAlchemyError as e:
            raise RuntimeError(f"Error executing query: {e}")

    def _create_comment(self, type_comment: str, comment_value: str, name_column: str = None) -> None:
        """
            *** Приватный универсальный метод для создания комментариев к различным сущностям в базе данных. ***

            Предназначен для создания комментариев к различным сущностям в базе данных, таких как (колонки | таблицы |
            материализованные представления | представления) и инкапсуляции в публичные методы, предназначенные для
            конкретного типу сущности (в зависимости от указанного "type_comment").
            Используется как вложенный метод в ряде основных методов библиотеки по созданию комментариев.

            * Описание механики:
                В соответствии с переданным ключом в "type_comment", например 'COLUMN' или 'TABLE' определяется логика
                дальнейшей обработки по вставке в бланк запроса sql (выполняющего запись комментариев) и передачу
                его в приватный метод отвечающий за запись информации в базу данных "self._recorder()".
                Логика разделяется на две основные ветки обработки:
                    - если это комментарий к колонке в базе данных,
                    - иначе это комментарий к сущности, такой как таблица, представление или материализованное
                представление.
                После определения типа, осуществляется форматирование бланк запроса sql, заполняя его значениями,
                такими как "schema", "name_column" и др. Далее передача подготовленного sql в "self._recorder()".
                В случае если type_comment == 'COLUMN', а аргумент "name_column" не будет передан, вызывается
                исключение ValueError.

            ***

            * Пример вызова:

                self._create_comment(type_comment='TABLE', comment_value=comment)

            ***

            :param type_comment: Значение типа сущности, например 'TABLE'.
            :param comment_value: Комментарий, который необходимо записать к сущности в базе данных.
            :param name_column: Опционально,  if type_comment == 'COLUMN'.
            :return: None, в случае успешной записи в базу данных.
            :rtype: None.
            :raises ValueError: Если не передано значение "name_column" при условии, if type_comment == 'COLUMN', \
                а так же возможны другие исключения во вложенных служебных методах (подробно смотрите в их описании).
        """

        if type_comment == 'COLUMN':
            if name_column:

                comment_value = self._validator(comment_value, str)

                mutable_sql_variant = self._insert_params_in_sql(
                    SQL_SAVE_COMMENT_COLUMN,
                    entity_type=self.PARAMS_SQL.get(type_comment),
                    schema=self.schema,  # Проверка на инъекции есть на верхнем уровне при инициализации:
                    name_column=self._stop_sql_injections(name_column),
                )

                # Передача comment в параметры на запись безопасна (методы SQLAlchemy).
                self._recorder(mutable_sql_variant, comment=comment_value)

            else:
                raise ValueError(f'Не передано значение для аргумента: name_column.')


        # Если комментарий не для колонки, значит для любой другой сущности (таблица, представление, ...)
        else:
            mutable_sql_variant = self._insert_params_in_sql(
                SQL_SAVE_COMMENT,
                entity_type=self.PARAMS_SQL.get(type_comment),
                schema=self.schema,  # Проверка на инъекции есть на верхнем уровне при инициализации:
            )

            self._recorder(mutable_sql_variant, comment=comment_value)

    def _set_column_comment(self, comments_columns_dict: dict) -> None:
        """
            Метод для создания комментариев к COLUMN.
        """

        if self._validator(comments_columns_dict, dict):

            for key_name_column, value_comment in comments_columns_dict.items():
                self._create_comment(type_comment='COLUMN', comment_value=value_comment, name_column=key_name_column)
        else:
            raise ValueError(f'Аргумент "comments_columns_dict" не содержит значения,'
                             f' передано: ({comments_columns_dict}).')

    def get_type_entity(self) -> str:
        """
           *** Метод определения типа сущности ('table', 'view', 'mview', ...) базы данных по ее имени. ***

            Предназначен для определения типа сущности базы данных (в текущей версии библиотеки, только для PostgreSQL),
            таких как:
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

            * Описание механики:
                Выполняется sql запрос к системным таблицам в базе данных, отвечающих за хранение статистики и по имени
                сущности определяется ее тип. Варианты результата запроса (type_entity): 'table', 'view', 'mview',
                'index', 'sequence', 'toast', 'composite_type', 'foreign_table', 'partitioned_table',
                'partitioned_index'.
                Передача аргументов не требуется, используются аргументы экземпляра
                класса при инициализации (self.name_entity).
                Возможны другие исключения во вложенных служебных методах (подробно смотрите в их описании).

            ***

            * Пример вызова:

                comments = Tcommenter(engine=ENGINE, name_table='sales', schema='audit')
                type_entity = comments.get_type_entity()

            ***

            :return: Возвращает значение сущности: ('table', 'view', 'mview', ...).
            :rtype: str.
        """

        # Определение типа сущности (варианты: 'table', 'view', 'mview'):
        type_entity = self._reader(SQL_CHECK_TYPE_ENTITY, name_entity=self.name_entity)

        return type_entity[0][0] if type_entity else None

    def set_table_comment(self, comment: str) -> None:
        """
            *** Метод для создания комментариев к таблицам в базе данных. ***

            Предназначен для создания комментариев к таблицам в базе данных (в текущей версии библиотеки, только для
            PostgreSQL).

            * Описание механики:
                В данном методе используется служебный "self._create_comment()" в качестве вложенного с указанием
                type_comment='TABLE', обеспечивая  выполнение основной логики. (подробно смотрите в его описании).

            ***

            * Пример вызова:

                comments = Tcommenter(engine=ENGINE, name_table='sales', schema='audit')
                comments.set_table_comment('Этот комментарий будет записан к таблице в метаданные.')

            ***

            :param comment: Комментарий, который необходимо записать к сущности в базе данных.
            :return: None, в случае успешной записи в базу данных.
            :rtype: None.
            :raises: Возможны другие исключения во вложенных служебных методах (подробно смотрите в их описании).
        """

        self._create_comment(type_comment='TABLE', comment_value=comment)

    def set_view_comment(self, comment: str) -> None:
        """
            *** Метод для создания комментариев к представлениям в базе данных. ***

            Предназначен для создания комментариев к представлениям в базе данных (в текущей версии библиотеки, только
            для PostgreSQL).

            * Описание механики:
                В данном методе используется служебный "self._create_comment()" в качестве вложенного с указанием
                type_comment='VIEW', обеспечивая  выполнение основной логики. (подробно смотрите в его описании).

            ***

            * Пример вызова:

                comments = Tcommenter(engine=ENGINE, name_table='sales', schema='audit')
                comments.set_view_comment('Этот комментарий будет записан к представлению в метаданные.')

            ***

            :param comment: Комментарий, который необходимо записать к сущности в базе данных.
            :return: None, в случае успешной записи в базу данных.
            :rtype: None.
            :raises: Возможны другие исключения во вложенных служебных методах (подробно смотрите в их описании).
        """

        self._create_comment(type_comment='VIEW', comment_value=comment)

    def set_materialized_view_comment(self, comment: str) -> None:
        """
            *** Метод для создания комментариев к материализованным представлениям в базе данных. ***

            Предназначен для создания комментариев к представлениям в базе данных (в текущей версии библиотеки, только
            для PostgreSQL).

            * Описание механики:
                В данном методе используется служебный "self._create_comment()" в качестве вложенного с указанием
                type_comment='MATERIALIZED', обеспечивая  выполнение основной логики. (подробно смотрите
                в его описании).

            ***

            * Пример вызова:

                comments = Tcommenter(engine=ENGINE, name_table='sales', schema='audit')
                comments.set_materialized_view_comment(
                    'Этот комментарий будет записан к материализованному представлению в метаданные.'
                )

            ***

            :param comment: Комментарий, который необходимо записать к сущности в базе данных.
            :return: None, в случае успешной записи в базу данных.
            :rtype: None.
            :raises: Возможны другие исключения во вложенных служебных методах (подробно смотрите в их описании).
        """

        self._create_comment(type_comment='MATERIALIZED', comment_value=comment)

    def set_column_comment(self, **comments_columns: str) -> None:
        """
            Метод для создания комментариев к COLUMN.
        """

        for key_name_column, value_comment in comments_columns.items():
            self._create_comment(type_comment='COLUMN', comment_value=value_comment, name_column=key_name_column)

    def get_table_comments(self, str_mode=False) -> dict[str, str] | str:
        """
            Метод для получения комментариев к таблицам.
            На выходе: str - строка с комментарием к таблице.
        """

        # Валидация str_mode:
        str_mode = self._validator(str_mode, bool)

        # Получаем сырые данные после запроса (список кортежей):
        table_comment_tuple_list: list[tuple] = self._reader(
            SQL_GET_TABLE_COMMENTS, name_entity=self.name_entity
        )

        # Преобразуем (обращаемся к первому элементу в списке (к кортежу, будет всего один всегда) и распаковываем:
        if table_comment_tuple_list:
            table_comment = table_comment_tuple_list[0][0]
        else:
            table_comment = ''  # Если комментарий отсутствует, возвращаем пустую строку.

        if str_mode is True:
            return table_comment
        elif str_mode is False:
            return {'table': table_comment}

    def get_column_comments(self, *column_index_or_name: int | str, str_mode=False) -> dict[str, dict] | str:
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
        param_column_index_or_name: tuple[int | str] | None = None or column_index_or_name

        if param_column_index_or_name:

            # Получаем sql (или для имен, или для индексов) и и список параметров:
            sql, params_list_only_from_indexes_or_name = self._get_sql_and_params_list_only_from_indexes_or_names(
                param_column_index_or_name
            )

            # Передаем уточненные sql и параметры:
            column_comments_tuple_list: list[tuple] = self._reader(
                sql,
                name_entity=self.name_entity,
                columns=params_list_only_from_indexes_or_name
            )

        else:

            # Передаем sql для извлечения всех комментариев без параметров:
            column_comments_tuple_list: list[tuple] = self._reader(
                SQL_GET_ALL_COLUMN_COMMENTS,
                name_entity=self.name_entity
            )

        # Генерация словаря из списка кортежей:
        # Распаковывает кортеж из 2-х элементов (1, 'Alice') принимая первый за key и второй за value:
        _column_comments_dict = {key: value for key, value in column_comments_tuple_list}

        if str_mode is True:
            return _column_comments_dict
        elif str_mode is False:
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
            type_entity = self.get_type_entity()

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
                    if self._validator(value, dict):

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
                f' name_table: {self.name_entity}, engine: {self.engine}).'
                )

    def __repr__(self):
        return (f'{self.__class__.__name__}(schema: {self.schema},'
                f' name_table: {self.name_entity}, engine: {self.engine}).'
                )
# ----------------------------------------------------------------------------------------------------------------------


# На выходе: str, последовательность имен или индексов колонок (через запятую в кавычках): columns_string