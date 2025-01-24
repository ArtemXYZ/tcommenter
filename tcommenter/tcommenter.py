# Copyright (c) 2025 ArtemXYZ
# This project is licensed under the MIT License - see the LICENSE file for details.

"""
    Главный модуль библиотеки "Tcommenter" предназначенной для создания комментариев к таблицам (и другим сущностям)
    в базе данных (в текущей версии библиотеки, только для PostgreSQL).
    Основная область применения "Tcommenter" - "Apache Airflow" для удобства работы с метаданными в DAGs
    (DAG - Directed Acyclic Graph, https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)
"""

# ----------------------------------------------------------------------------------------------------------------------
import re
from typing import TypeVar  # , Union

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
        "Tcommenter" содержит необходимые методы для создания, извлечения и перегрузки комментариев к таблицам
        (и другим сущностям), колонкам в базе данных (в текущей версии библиотеки, только для PostgreSQL).

        Основная область применения "Tcommenter" - "Apache Airflow" для удобства работы с метаданными в DAGs
        (DAG - Directed Acyclic Graph, https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html).
    """

    PARAMS_SQL = {
        'TABLE': 'TABLE',
        'VIEW': 'VIEW',
        'MATERIALIZED': 'MATERIALIZED VIEW',
        'COLUMN': 'COLUMN',
    }

    def __init__(self, engine: Engine, name_table: str, schema: str):
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
                            # Invalid data type: "{type(value).__name__}", for the argument: "{value}".'
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
            raise ValueError(f'Ошибка проверки строки: "{sql_param_string}"! Обнаружен недопустимый символ. '
                             f'Разрешены только буквы латинского алфавита, цифры, символы: "_", ".", "-".')

        # Проверка на наличие sql-ключевых слов:
        disallowed_keywords = ["DROP", "CREATE", "ALTER", "INSERT", "UPDATE", "DELETE", "--", ";"]
        if any(keyword in sql_param_string.upper() for keyword in disallowed_keywords):
            raise ValueError(f'Ошибка проверки строки: "{sql_param_string}"! '
                             f'Обнаружено присутствие sql-ключевых слов: {disallowed_keywords}'
                             )

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
            *** Приватный метод чтения данных в SQL базе данных. ***

            Предназначен для выполнения SQL запросов на чтение данных с подстановкой параметров или без.
            Используется как вложенный метод в других участках кода библиотеки.

            * Описание механики:
                Метод основан на работе библиотеки "SQLAlchemy" ("execute()"). Опциональная передача параметров,
                позволяет сделать  self._reader() универсальным. Механизм передачи через multyparams
                в conn.execute(sql, multyparams)) обеспечивает защиту от SQL-инъекций.
                В случае возникновения ошибки при осуществлении запроса, вызывается исключение RuntimeError
                с подробностями (SQLAlchemyError).

            ***

            * Пример вызова:

                result = self._reader(sql, placeholder_sales='sales'})

            ***

            :param sql: Шаблоне sql запроса.
            :param params: kwargs (ключ: имя плейсхолдера в sql шаблоне, значение: необходимые данные).
            :return: Возвращает список кортежей, пример результата: [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')] или [].
            :rtype: list[tuple].
            :raises RuntimeError: Если (SQLAlchemyError).
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
            *** Приватный метод записи данных в SQL базе данных. ***

            Предназначен для выполнения SQL запросов на запись данных с подстановкой параметров или без.
            Используется как вложенный метод в других участках кода библиотеки.

            * Описание механики:
                Метод основан на работе библиотеки "SQLAlchemy" ("execute()"). Опциональная передача параметров,
                позволяет сделать  self._recorder() универсальным. Механизм передачи через multyparams
                в conn.execute(sql, multyparams)) обеспечивает защиту от SQL-инъекций.
                В случае возникновения ошибки при осуществлении запроса, вызывается исключение RuntimeError
                с подробностями (SQLAlchemyError).

            ***

            * Пример вызова:

                self._recorder(sql, sales='Этот комментарий будет записан для колонки в метаданные.')

            ***

            :param sql: Шаблоне sql запроса.
            :param params: kwargs (ключ: имя плейсхолдера в sql шаблоне, значение: необходимые данные).
            :return: None.
            :rtype: None.
            :raises RuntimeError: Если (SQLAlchemyError).
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
            *** Приватный метод для создания комментариев к колонкам в базе данных. ***

            Предназначен для создания комментариев к колонкам сущностей (таблиц, представлений, и т.д.) в базе данных
            (в текущей версии библиотеки, только для PostgreSQL).
            Практически одинаков с "self.set_column_comment()", разница лишь в местах применения (данный метод
            используется в качестве основы в "save_comments()") и типе принимаемого аргумента (принимает dict).


            * Описание механики:
                В данном методе используется служебный "self._create_comment()" в качестве вложенного с указанием
                type_comment='COLUMN', обеспечивая  выполнение основной логики. (подробно смотрите в его описании).
                Осуществляя итерирование по всему словарю, поочередно записывается комментарии к колонкам (sql
                синтаксис обеспечивает запись только к одной колонке).

            ***

            * Пример вызова:

                params = {'sales': 'Этот комментарий будет записан для колонки в метаданные.'}
                self._set_column_comment(params)

            ***

            :param comments_columns_dict: Комментарий, который необходимо записать к сущности в базе данных.
            :return: None, в случае успешной записи в базу данных.
            :rtype: None.
            :raises: Возможны другие исключения во вложенных служебных методах (подробно смотрите в их описании).
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
            *** Метод для создания комментариев к колонкам в базе данных. ***

            Предназначен для создания комментариев к колонкам сущностей (таблиц, представлений, и т.д.) в базе данных
            (в текущей версии библиотеки, только для PostgreSQL).
            Практически одинаков с "_self.set_column_comment()", разница лишь в местах применения (данный метод
            используется в качестве публичного по прямому назначению) и типе принимаемого аргумента (принимает kwargs).


            * Описание механики:
                В данном методе используется служебный "self._create_comment()" в качестве вложенного с указанием
                type_comment='COLUMN', обеспечивая  выполнение основной логики. (подробно смотрите в его описании).
                Осуществляя итерирование по всему словарю, поочередно записывается комментарии к колонкам (sql
                синтаксис обеспечивает запись только к одной колонке).

            ***

            * Пример вызова:

                params = {'sales': 'Этот комментарий будет записан для колонки в метаданные.'}
                self._set_column_comment(params)

            ***

            :param  comments_columns: kwargs (
                ключ: имя или индекс колонки,
                значение: комментарии, который необходимо записать к колонке в базе данных
                ).
            :return: None, в случае успешной записи в базу данных.
            :rtype: None.
            :raises: Возможны другие исключения во вложенных служебных методах (подробно смотрите в их описании).
        """

        for key_name_column, value_comment in comments_columns.items():
            self._create_comment(type_comment='COLUMN', comment_value=value_comment, name_column=key_name_column)

    def get_table_comments(self, service_mode: bool = False) -> str | dict[str, str]:
        """
            *** Метод для получения комментариев к таблицам (и к другим сущностям, кроме колонок) в базе данных. ***

            Предназначен для получения комментариев к таблицам и другим сущностям, таким как представлениям |
            материализованным представлениям | ... (имя метода лишь явно указывает на отличие от метода получения
            комментариев к колонкам) в базе данных (в текущей версии библиотеки, только для PostgreSQL).

            * Описание механики:
                В данном методе используется вложенный служебный "self._reader()". Он получает "self.name_entity"
                из "__init__" в качестве значения для плейсхолдера в sql запросе на получение комментариев из
                служебных таблиц PostgreSQL, отвечающих за хранение различной статистики и метаданных
                (tps://postgrespro.ru/docs/postgresql/14/monitoring-stats).

                Далее, результат ответа преобразуется:
                    либо в строку с комментарием - по умолчанию (service_mode=False),
                    либо если service_mode=True в словарь типа {'table': 'table_comment'}.

                Режим service_mode=True предназначен для предоставления выходных данных совместимых с методом
                "save_comments()", в случае, если необходима перегрузка комментариев (сначала получить, а после вашей
                промежуточной  логики) сразу же сохранить в ту же или другую сущность (с той же структурой).
                Подробнее смотрите описание "save_comments()".

            ***

            * Пример вызова:

                comments = Tcommenter(engine=ENGINE, name_table='sales', schema='audit')

                # -> 'comment'.
                comment_table_str = comments.get_table_comments()

                # -> {'table': 'comment'}.
                comment_table_dict = comments.get_table_comments(service_mode=True)

            ***

            :param service_mode: "Переключатель" вида выходных данных, по умолчанию False.
            :return: Если service_mode=False -> str (строка с комментарием к таблице), \
                если service_mode=True -> dict[str, str].
            :rtype: str | dict[str, str].
            :raises: Возможны другие исключения во вложенных служебных методах (подробно смотрите в их описании).
        """

        # Валидация str_mode:
        service_mode = self._validator(service_mode, bool)

        # Получаем сырые данные после запроса (список кортежей):
        table_comment_tuple_list: list[tuple] = self._reader(
            SQL_GET_TABLE_COMMENTS, name_entity=self.name_entity
        )

        # Преобразуем (обращаемся к первому элементу в списке (к кортежу, будет всего один всегда) и распаковываем:
        if table_comment_tuple_list:
            table_comment = table_comment_tuple_list[0][0]
        else:
            table_comment = ''  # Если комментарий отсутствует, возвращаем пустую строку.

        if service_mode is False:
            return table_comment
        elif service_mode is True:
            return {'table': table_comment}

    def get_column_comments(self,
                            *column_index_or_name: int | str,
                            service_mode: bool = False
                            ) -> dict[str, str] | dict[str, dict[str, str]]:
        """
            *** Метод для получения комментариев к колонкам по их имени или по индексу для сущностей базы данных. ***

            Предназначен для получения комментариев к колонкам различных сущностей, таких как представления |
            материализованные представления | ... в базе данных (в текущей версии библиотеки, только для PostgreSQL).

            * Описание механики:
                В данном методе используется вложенный служебный "self._reader()". Он получает "self.name_entity"
                из "__init__", а так же переданные пользователем "*column_index_or_name" - имя или индекс требуемой
                колонки в качестве значений для плейсхолдеров в sql запросе. Комментарии к сущностям хранятся
                в служебных таблицах PostgreSQL, отвечающих за обработку различной статистики и метаданных
                (tps://postgrespro.ru/docs/postgresql/14/monitoring-stats).

                Далее, результат ответа преобразуется:
                    либо в словарь типа ({'columns': {'column_name': 'comment'}}) - по умолчанию (service_mode=False),
                    либо если service_mode=True в словарь типа {'column_name': 'comment'}.

                Режим service_mode=True предназначен для предоставления выходных данных совместимых с методом
                "save_comments()", в случае, если необходима перегрузка комментариев (сначала получить, а после вашей
                промежуточной  логики) сразу же сохранить в ту же или другую сущность (с той же структурой).
                Подробнее смотрите описание "save_comments()".

                Важно!
                Если передать в параметры совместно и индекс и имя колонки - это вызовет исключение! Метод обрабатывает
                параметры только одного типа (либо только индексы, либо имена колонок).

            ***

            * Пример вызова:

                comments = Tcommenter(engine=ENGINE, name_table='sales', schema='audit')

                # -> {'column_name_1': 'comment_1', 'column_name_2': 'comment_2', ... }
                comment_table_dict = comments.get_table_comments()

                # -> {'columns': {'column_name_1': 'comment_1', 'column_name_2': 'comment_2', ... }}
                comment_table_str = comments.get_table_comments(str_mode=True)

            ***

            :param column_index_or_name: *args - индексы или имена колонок, для которых требуется считать комментарии.
            :param service_mode: "Переключатель" вида выходных данных, По умолчанию False.
            :return: Если service_mode=False -> dict[str, str], если service_mode=True -> dict[str, dict[str, str]].
            :rtype: dict[str, str] | dict[str, dict[str, str]].
            :raises: Возможны другие исключения во вложенных служебных методах (подробно смотрите в их описании).
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

        if service_mode is False:
            return _column_comments_dict
        elif service_mode is True:
            return {'columns': _column_comments_dict}  # {'columns': {column: comments} }

    def get_all_comments(self) -> dict[str, str | dict]:
        """
            *** Метод для получения всех комментариев для сущности (к ней и ее колонкам) базы данных. ***

            Предназначен для получения всех имеющихся комментариев для сущности (представления | материализованного
            представления | ...), ее собственных и к колонкам (в текущей версии библиотеки, только для PostgreSQL).

            * Описание механики:
                Основная и единственная логика заключается в сложении двух словарей полученных в результате работы
                "get_table_comments()" и  "get_column_comments()" с service_mode=True.

            ***

            * Пример вызова:

                comments = Tcommenter(engine=ENGINE, name_table='sales', schema='audit')

                # -> {'table': 'table_comment', 'columns': {'column_1': 'column_1_comment', ...}}.
                all_comments_dict = comments.get_all_comments(service_mode=True)
                comments.save_comments(all_comments_dict)

            ***

            :return: {'table': 'table_comment', 'columns': {'column_1': 'column_1_comment', ...}}.
            :rtype: dict[str, str | dict].
            :raises: Возможны другие исключения во вложенных служебных методах (подробно смотрите в их описании).
        """

        # Получение всех комментариев:
        table_comment = self.get_table_comments(service_mode=True)
        column_comments_dict = self.get_column_comments(service_mode=True)

        # Преобразование полученных данных в единый словарь:
        all_comments_table_dict = table_comment | column_comments_dict

        return all_comments_table_dict  # на выходе: {'table': set_table_comment, 'columns': column_comments_dict}

    def save_comments(self, comments_dict: dict[str, str | dict]) -> None:  # Self , schema: str
        """
            *** Метод для сохранения комментариев любого типа (к сущностям или их колонкам) в базу данных. ***

            Предназначен для сохранения комментариев любого типа (к сущностям или их колонкам) в базу данных (в текущей
            версии библиотеки, только для PostgreSQL). Метод "save_comments()" является универсальным, он автоматически
            определяет для каткого типа сущности предназначаются комментарии, что делает его полезным, когда необходимо
            сохранить сразу все метаданные разом и для колонок и для сущностей, вызвав всего один метод.
            Применяется в случае, если необходима перегрузка комментариев (сначала получить, а после вашей
            промежуточной логики) сразу же сохранить в ту же или другую сущность (с той же структурой).

            * Описание механики:
                Выходные данные вида "сервисной структуры" (в методах получения комментариев необходимо установить
                service_mode=True): {'columns': {...}} | {'table': 'table_comment'}, обеспечивают корректную их
                обработку. Ключи 'table' | 'columns' являются маркерами для включения логики обработки конкретного типа
                комментариев к сущностям или их к колонкам. Далее, в методе автоматически определяется тип сущности и
                вызовы соответствующих методов для сохранения метаданных.

            ***

            * Пример вызова:

                comments = Tcommenter(engine=ENGINE, name_table='sales', schema='audit')

                # Вариант 1 (аналогично для данных от get_column_comments())
                # -> {'table': 'comment'}.
                comment_table_dict = comments.get_table_comments(service_mode=True)
                comments.save_comments(comment_table_dict)

                # Вариант 2
                # -> {'table': 'table_comment', 'columns': {'column_1': 'column_1_comment', ...}}.
                comment_table_dict = comments.get_all_comments(service_mode=True)
                comments.save_comments(comment_table_dict)

            ***

            :param comments_dict: Словарь типа:
                {'table': 'table_comment', 'columns': {'column_1': 'column_1_comment', ...}} |
                {'table': 'table_comment'} | {'columns': {'column_1': 'column_1_comment', ...}}.
            :return: None.
            :rtype: None.
            :raises: ValueError, Возможны другие исключения во вложенных служебных методах (подробно смотрите в их \
                описании).
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
