"""
    Модуль тестирования проекта.

    Список методов и аргументов:
        PARAMS_SQL
        SQL_SAVE_COMMENT
        SQL_SAVE_COMMENT_COLUMN
        SQL_GET_TABLE_COMMENTS
        SQL_GET_ALL_COLUMN_COMMENTS
        SQL_GET_COLUMN_COMMENTS_BY_INDEX
        SQL_GET_COLUMN_COMMENTS_BY_NAME
        SQL_CHECK_TYPE_ENTITY
        __init__
        _validator
        _check_all_elements
        _sql_formatter
        _mutation_sql_by_logic
        _create_comment
        _set_column_comment
        recorder
        reader
        row_sql_recorder
        get_type_entity
        set_table_comment
        set_view_comment
        set_materialized_view_comment
        set_column_comment
        get_table_comments
        get_column_comments
        get_all_comments
        save_comments
"""

# import unittest
import pytest

from unittest.mock import MagicMock, patch
from sqlalchemy import text
from sqlalchemy.engine.base import Engine
from tcommenter.tcommenter import Tcommenter
from tcommenter.sql.postgre_sql import *

# from tests.connnections.connection import ENGINE_MART_SV

# todo: get_instance_test_class - сделать фикстурой и убрать из тестовых функций;
# todo: сделать класс инструментов для тестов (объединить).
# ---- ------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------- Переменные для тестирования
table_comment = {'table': 'Таблица содержит выгрузку данных из Airflow по имеющимся дагам (все доступные атрибуты).'}

_SQL_GET_TABLE_COMMENTS = """
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
        all_entity.relname = 'dags_analyzer'
"""

# """COMMENT ON {entity_type} "{schema}"."{name_entity}" IS :comment"""
_SQL_SAVE_COMMENT_TEST_1 = """COMMENT ON {entity_type} "{schema}"."dags_analyzer" IS :comment"""

_SQL_SAVE_COMMENT_TEST_2_3 = """COMMENT ON TABLE "audit"."dags_analyzer" IS :comment"""

_SQL_SAVE_COMMENT_TEST_4 = """COMMENT ON TABLE "{schema}"."dags_analyzer" IS :comment"""

_SQL_GET_TABLE_COMMENTS = """
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
        all_entity.relname = "dags_analyzer"
"""


# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------- Инструменты (методами):
class BaseToolsTests:
    """
        Набор методов для сокращения кода тестовых функций и упрощения управления тестовыми данными.
    """

    @staticmethod
    def raises_router(func, exception, match, result, *args, **kwargs):  # mocked_engine,
        """
            Функция управления логикой вызова pytest.raises().
        """

        if exception:
            with pytest.raises(exception, match=match):
                func(*args, **kwargs)  # , **kwargs
        else:
            assert func(*args, **kwargs) == result  #

        return None

    @staticmethod
    def execute_mock(mocked_engine, result):
        """
            Настраивает результат метода execute для mocked_engine.

            :param mocked_engine: Заглушка SQLAlchemy Engine.
            :param result: Результат, который вернет fetchall() после вызова execute.
        """
        # Получаем подключение через mocked_engine
        mocked_connection = mocked_engine.connect.return_value.__enter__.return_value

        # Настраиваем execute -> fetchall() -> result
        mocked_connection.execute.return_value.fetchall.return_value = result

        return mocked_connection


# ----------------------------------------- Фикстуры и Mocks (методами):
class Fixtures:
    """
        Класс, содержащий фикстуры и заглушки для тестов.
    """

    @pytest.fixture(scope="class")
    def mocked_engine(self):
        """
            Фикстура для создания заглушки Engine SQLAlchemy.
        """
        return MagicMock(spec=Engine)

    @pytest.fixture(scope="class")
    def test_class(self, mocked_engine):
        """
            Фикстура для получения экземпляра Tcommenter.
        """

        return Tcommenter(engine=mocked_engine, name_table="dags_analyzer", schema="audit")


# =================================================== Tests:
# @pytest.mark.usefixtures("mocked_engine",  "test_class")
class TestMethodsTcommenterNoExecuteSQL(Fixtures):  # (Fixtures)
    """
        Тестирование методов не требующих извлечения информации из бд (нет .execute()).
    """

    # Перегружаем класс для удобства:
    tools = BaseToolsTests

    def test_get_instance_class(self, test_class):
        assert isinstance(test_class, Tcommenter)

    # ---------------------------------- *** "_validator" ***
    @pytest.mark.parametrize(
        "value_test, check_type_test, exception, match, result",
        [
            ('test', str, None, None, 'test'),
            ('test2', (dict, int,), TypeError, 'Недопустимый тип данных: "str", для аргумента: "test2"', None),
            ('test3', (str, int,), None, None, 'test3'),
        ]
    )
    def test__validator(self, test_class, value_test, check_type_test, exception, match, result):  # mocked_engine,
        self.tools.raises_router(
            # ---------------------------------- Passing the function under test:
            test_class._validator,
            # ---------------------------------- Other variables
            exception,
            match,
            result,
            # ---------------------------------- *args:
            value_test, check_type_test
        )

    # ---------------------------------- *** "_check_all_elements" ***
    @pytest.mark.parametrize(
        "check_type_test, value_array_test, exception, match, result",
        [  # dict, list, tuple,
            (str, 'name_column_1', TypeError, 'Недопустимый тип данных:', None),
            (str, ['test', 'test', 'test', 2], None, None, False),
            (int, ['test', 'test', 'test', '2'], None, None, False),
            (int, ('test', 1), None, None, False),
        ]
    )
    def test__check_all_elements(
            self, test_class, check_type_test, value_array_test, exception, match, result
    ):
        self.tools.raises_router(
            # ---------------------------------- Passing the function under test:
            test_class._check_all_elements,
            # Other variables
            exception,
            match,
            result,
            # ---------------------------------- *args (function params):
            check_type_test, value_array_test,
        )

    # ---------------------------------- *** "_stop_sql_injections" ***
    @pytest.mark.parametrize(
        "sql_param_string, exception, match, result",
        [
            ('test', None, None, 'test'),
            ('', ValueError, 'Ошибка проверки строки:', None),
            (' - ', ValueError, 'Ошибка проверки строки:', None),
            ('-', None, None, '-'),
            ('--', ValueError, 'Ошибка проверки строки:', None),
            ('*', ValueError, 'Ошибка проверки строки:', None),
            (';', ValueError, 'Ошибка проверки строки:', None),
            ('DROP', ValueError, 'Ошибка проверки строки:', None),
            ('CREATE', ValueError, 'Ошибка проверки строки:', None),
            ('ALTER', ValueError, 'Ошибка проверки строки:', None),
            ('INSERT', ValueError, 'Ошибка проверки строки:', None),
            ('UPDATE', ValueError, 'Ошибка проверки строки:', None),
            ('DELETE', ValueError, 'Ошибка проверки строки:', None),
            ('DELET', None, None, 'DELET'),
            (1, TypeError, 'Недопустимый тип данных:', None),
            ('sdfdksdsd---*D\\', ValueError, 'Ошибка проверки строки:', None),
        ]
    )
    def test__stop_sql_injections(self, test_class, sql_param_string, exception, match, result):
        self.tools.raises_router(
            # ---------------------------------- Passing the function under test:
            test_class._stop_sql_injections,
            # ---------------------------------- Other variables
            exception,
            match,
            result,
            # ---------------------------------- *args (function params):
            sql_param_string,
        )

    # ---------------------------------- *** "_insert_params_in_sql" ***
    @pytest.mark.parametrize(
        "sql_blank, kwargs_sql_params_test, exception, match, result",
        [
            # Ошибка ValueError (KeyError на более низком уровне) возникнет если переданный ключ \
            # (имя плейсхолдера из **sql_params) не найдет совпадений:
            (
                    SQL_SAVE_COMMENT, {'name_placeholder': 'test'},
                    ValueError, 'Ошибка форматирования SQL-запроса:', _SQL_SAVE_COMMENT_TEST_1
            ),

            (SQL_SAVE_COMMENT, {'entity_type': 'TABLE', 'schema': 'audit', }, None, None, _SQL_SAVE_COMMENT_TEST_2_3),

            #  Ошибка не возникнет если, все существующие ключи (имена плейсхолдеров из **sql_params) совпадут, \
            # а избыточные проигнорируются, в данном случае - это "add_new_column_test_name".
            (
                    SQL_SAVE_COMMENT,
                    {'entity_type': 'TABLE', 'schema': 'audit', 'add_new_column_test_name': 'name_column_test', },
                    None, None, _SQL_SAVE_COMMENT_TEST_2_3
            ),

        ]
    )
    def test__insert_params_in_sql(
            self, test_class, sql_blank, kwargs_sql_params_test, exception, match, result
    ):
        self.tools.raises_router(
            # ---------------------------------- Passing the function under test:
            test_class._insert_params_in_sql,
            # ---------------------------------- Other variables
            exception,
            match,
            result,
            # ---------------------------------- *args (function params):
            sql_blank,
            # ---------------------------------- ** kwargs
            **kwargs_sql_params_test,
        )

    # ---------------------------------- *** "_generate_params_list_for_sql" ***
    @pytest.mark.parametrize(
        "params_test, exception, match, result",
        [
            (('name_column_test', 1), None, None, ['name_column_test', 1]),
            ('name_column_test', TypeError, 'Недопустимый тип данных', None),
            (('', 1), None, None, ['', 1]),
            ({'name_column_test': 1}, TypeError, 'Недопустимый тип данных', None),
            (1, TypeError, 'Недопустимый тип данных', None),
        ]
    )
    def test__generate_params_list_for_sql(
            self, test_class, params_test, exception, match, result
    ):
        self.tools.raises_router(
            # ---------------------------------- Passing the function under test:
            test_class._generate_params_list_for_sql,
            # ---------------------------------- Other variables
            exception,
            match,
            result,
            # ---------------------------------- *args (function params):
            params_test,
            # ---------------------------------- ** kwargs
        )

    # ---------------------------------- *** "_get_sql_and_params_list_only_from_indexes_or_names" ***
    @pytest.mark.parametrize(
        "param_column_index_or_name_test, exception, match, result",
        [
            (('name_column_test', 1), TypeError, 'Ошибка валидации входных данных!', None),
            (('', 1), TypeError, 'Ошибка валидации входных данных!', None),
            (('name_column_test_1', 'name_column_test_2'),
             None, None, (SQL_GET_COLUMN_COMMENTS_BY_NAME, ['name_column_test_1', 'name_column_test_2'])),
            ((1, 2), None, None, (SQL_GET_COLUMN_COMMENTS_BY_INDEX, [1, 2])),
            (None, None, None, None),  # Нет else блока для обработки пустых значений.
            ('', None, None, None),  # Нет else блока для обработки пустых значений (в данном случае для пустой строки).
            ('test', TypeError, 'Недопустимый тип данных', None),
            (['test'], TypeError, 'Недопустимый тип данных', None),
        ]
    )
    def test__get_sql_and_params_list_only_from_indexes_or_names(
            self, test_class, param_column_index_or_name_test, exception, match, result
    ):
        self.tools.raises_router(
            # ---------------------------------- Passing the function under test:
            test_class._get_sql_and_params_list_only_from_indexes_or_names,
            # ---------------------------------- Other variables
            exception,
            match,
            result,
            # ---------------------------------- *args (function params):
            param_column_index_or_name_test,
            # ---------------------------------- ** kwargs
        )


# ------------------------------------------------ С подключением к БД.
class TestMethodsTcommenterExecuteSQL:
    """
        Тестирование методов требующих извлечения информации из бд (через ".execute()").
    """

    # Перегружаем класс для удобства:
    tools = BaseToolsTests

    # ---------------------------------- *** "_reader" ***
    @pytest.mark.parametrize(
        "sql_test, params_kwargs_test, exception, match, result",
        [
            (SQL_GET_TABLE_COMMENTS, {'name_entity': 'dags_analyzer'}, None, None, [('Alice',)]),

        ]
    )
    def test__reader(self, mocked_engine, test_class, sql_test, params_kwargs_test, exception, match, result):


        # Настройка execute, чтобы он возвращал [('Alice',), ('Bob',)]
        execute_mock(mocked_engine, [('Alice',), ('Bob',)])

        self.tools.raises_router(
            # ---------------------------------- Passing the function under test:
            test_class._reader,
            # ---------------------------------- Other variables
            exception,
            match,
            result,
            # ---------------------------------- *args:
            sql_test,
            # ---------------------------------- ** kwargs
            **params_kwargs_test
        )

# def test_reader(mocked_engine):
#     test_instance = get_instance_class(mocked_engine)
#     assert test_instance._reader(SQL_GET_COLUMN_COMMENTS_BY_NAME, columns='columns')

# row_sql_recorder
# recorder


# @pytest.fixture(scope="class")
# # def mocked_connection():
# #     """
# #         Фикстура для создания замоканного подключения.
# #     """
# #
# #     mock_conn = MagicMock()  # Создаем заглушку подключения
# #     mock_conn.execute = MagicMock()  # Мокаем метод execute
# #     return mock_conn
# #
# # @pytest.fixture(scope="class")
# # def mocked_engine(mocked_connection):
# #     """
# #         Фикстура для создания заглушки SQLAlchemy Engine.
# #     """
# #     engine = MagicMock(spec=Engine)  # Заглушка SQLAlchemy Engine
# #     # engine.connect.return_value.__enter__.return_value = mocked_connection  # Возвращаем подключение
# #     # Mock execute:
# #     engine.connect.return_value.__enter__.return_value.execute = mocked_connection
# #     return engine


# # Создаем mock для engine:
# @pytest.fixture(scope="class")  # scope=class
# def mocked_engine():
#     """
#         Фикстура для создания заглушки SQLAlchemy Engine.
#     """
#
#     # Заглушка SQLAlchemy = Engine engine.__class__ = Engine
#     # engine = MagicMock(spec=Engine)
#     # Mock execute:
#     # engine.connect.return_value.__enter__.return_value.execute = MagicMock()
#     return MagicMock(spec=Engine)
