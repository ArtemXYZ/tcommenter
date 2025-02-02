"""
    Модуль юнит тестов.
"""

# import unittest
import pytest

from unittest.mock import MagicMock
from sqlalchemy import text
from sqlalchemy.engine.base import Engine
from tcommenter.tcommenter import Tcommenter
from tcommenter.sql.postgre_sql import *
from tests.utilities.utilities import *

# ----------------------------------------------------------------------------------------------------------------------
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


# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------- Фикстуры и Mocks (методами):
class Fixtures:
    """
        Класс, содержащий фикстуры и заглушки для тестов.
    """

    @pytest.fixture(scope="class")
    def mocked_engine(self) -> MagicMock:
        """
            Фикстура для создания заглушки Engine SQLAlchemy.
        """
        return MagicMock(spec=Engine)

    @pytest.fixture(scope="class")
    def mocked_connection(self, mocked_engine: MagicMock) -> MagicMock:
        """
            Возвращает макет подключения к базе данных.

            Returns:
                MagicMock: Макет подключения, который может быть использован в тестах.
        """
        return mocked_engine.connect.return_value.__enter__.return_value

    @pytest.fixture(scope="class")
    def test_class(self, mocked_engine):
        """
            Фикстура для получения экземпляра Tcommenter.
        """

        return Tcommenter(engine=mocked_engine, name_table="dags_analyzer", schema="audit")


# ======================================================= Tests ========================================================
class TestMethodsTcommenterNoExecuteSQL(Fixtures):  # (Fixtures)
    """
        Тестирование методов не требующих извлечения информации из бд (нет .execute()).
        Либо наследование, либо # @pytest.mark.usefixtures("mocked_engine", "test_class").
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
class TestMethodsTcommenterExecuteSQL(Fixtures):
    """
        Тестирование методов требующих извлечения информации из бд (через ".execute()").
    """

    # Перегружаем класс для удобства:
    tools = BaseToolsTests

    # # ---------------------------------- *** "_reader" ***
    # @pytest.mark.parametrize(
    #     "sql_test, params_kwargs_test, exception, match, result",
    #     [
    #         # Убрать в интеграционные тесты:
    #         # (SQL_GET_TABLE_COMMENTS, {'name_entity': 'dags_analyzer'}, None, None, [('Table comment',)]),
    #         # (SQL_GET_TABLE_COMMENTS, {'name_': 'dags_analyzer'}, None, None, [('Table comment',)]),
    #
    #         (SQL_GET_TABLE_COMMENTS, {'name_entity': 'dags_analyzer'}, None, None, {'name_entity': 'dags_analyzer'}),
    #     ]
    # )
    # def test__reader(self, mocked_connection, test_class, sql_test, params_kwargs_test, exception, match, result):
    #
    #     # Настройка execute, чтобы он возвращал проверяемый result:
    #     mocked_connection = self.tools.setter_execute_mock_values(mocked_connection, result)
    #     self.tools.check_sql(mocked_connection, sql_test, params_kwargs_test)
    #     # Выполнение SQL-запроса
    #     self.tools.raises_router(
    #         # ---------------------------------- Passing the function under test:
    #         test_class._reader,
    #         # ---------------------------------- Other variables
    #         exception,
    #         match,
    #         result,
    #         # ---------------------------------- *args:
    #         sql_test,
    #         # ---------------------------------- ** kwargs
    #         **params_kwargs_test
    #     )

# ================
# row_sql_recorder
# recorder


