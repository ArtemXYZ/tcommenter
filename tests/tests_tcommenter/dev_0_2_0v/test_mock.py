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


_SQL_GET_COLUMN_COMMENTS_BY_INDEX = """
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
        all_entity.relname = "dags_analyzer"
    INNER JOIN 
        pg_attribute AS cols                  
    ON
        cols.attnum = comments.objsubid 
    AND
        cols.attrelid = all_entity.oid      
    WHERE 
         comments.objsubid = 'name_column_test_1, name_column_test_2'
"""


# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------- Инструменты:
# Функция управления логикой вызова pytest.raises():
def pytest_raises_router(func, create_mocked_engine, exception, match, result, *args, **kwargs):   # , **kwargs
    """
        Функция управления логикой вызова pytest.raises().
    """

    if exception:
        with pytest.raises(exception, match=match):
            func(*args, **kwargs)  # , **kwargs
    else:
        assert func(*args, **kwargs) == result   #

    return None


# ----------------------------------------- мок бд.
# Создаем "мок" для engine:
@pytest.fixture
def create_mocked_engine():
    engine = MagicMock()  # Заглушка SQLAlchemy Engine
    engine.__class__ = Engine  # Указываем, что это мок класса Engine
    engine.connect.return_value.__enter__.return_value.execute = MagicMock()  # Мокаем execute
    return engine


# ----------------------------------------- Тестовый экземпляр класса:
def get_instance_test_class(mocked_engine) -> Tcommenter:
    """
        Возвращает экземпляр Tcommenter. Используется для всех тестов.
    """
    return Tcommenter(engine=mocked_engine, name_table="dags_analyzer", schema="audit")


class TestMethodsTcommenterNoExecuteSQL:
    """
        Тестирование методов не требующих извлечения информации из бд (нет .execute()).
    """

    def test_get_instance_class(self, create_mocked_engine):
        test_class = get_instance_test_class(create_mocked_engine)
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
    def test__validator(self, create_mocked_engine, value_test, check_type_test, exception, match, result):
        test_class = get_instance_test_class(create_mocked_engine)
        pytest_raises_router(
            # ---------------------------------- Passing the function under test:
            test_class._validator,
            # ---------------------------------- Other variables
            create_mocked_engine,
            exception,
            match,
            result,
            # ---------------------------------- *args:
            value_test, check_type_test
        )

    # ---------------------------------- *** "_check_all_elements" ***
    @pytest.mark.parametrize(
        "check_type_test, value_dict_test, exception, match, result",
        [
            (str, {'args_array': 'test'}, TypeError, 'Недопустимый тип данных: "str", для аргумента: "test"', None),
            (str, {'args_array': 1}, TypeError, 'Недопустимый тип данных: "int", для аргумента: "1".', None),
            (int, {'args_array': 1}, None, None, True),
            (str, {'args_array': ['test', 'test']}, None, None, True),
            (str, {'args_array': {'test': 'test'}}, None, None, True),
        ]
    )
    def test__check_all_elements(
            self, create_mocked_engine, check_type_test, value_dict_test, exception, match, result
    ):
        test_class = get_instance_test_class(create_mocked_engine)
        pytest_raises_router(
            # ---------------------------------- Passing the function under test:
            test_class._check_all_elements,
            # Other variables
            create_mocked_engine,
            exception,
            match,
            result,
            # ---------------------------------- *args (function params):
            check_type_test, value_dict_test,
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
    def test__stop_sql_injections(self, create_mocked_engine, sql_param_string, exception, match, result):
        test_class = get_instance_test_class(create_mocked_engine)
        pytest_raises_router(
            # ---------------------------------- Passing the function under test:
            test_class._stop_sql_injections,
            # ---------------------------------- Other variables
            create_mocked_engine,
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
            self, create_mocked_engine, sql_blank, kwargs_sql_params_test, exception, match, result
    ):
        test_class = get_instance_test_class(create_mocked_engine)
        pytest_raises_router(
            # ---------------------------------- Passing the function under test:
            test_class._insert_params_in_sql,
            # ---------------------------------- Other variables
            create_mocked_engine,
            exception,
            match,
            result,
            # ---------------------------------- *args (function params):
            sql_blank,
            # ---------------------------------- ** kwargs
            **kwargs_sql_params_test,
        )




    # _generate_params_list_for_sql todo !



    # ---------------------------------- *** "_get_sql_and_params_list_only_from_indexes_or_names" ***
    @pytest.mark.parametrize(
        "param_column_index_or_name_test, exception, match, result",
        [
            (('name_column_test', 1), TypeError, 'Ошибка валидации входных данных!', None),
            (('', 1), TypeError, 'Ошибка валидации входных данных!', None),
            # (('name_column_test_1', 'name_column_test_2'), None, None, _SQL_GET_COLUMN_COMMENTS_BY_INDEX),
            # ((1, 2), None, None, None),
            (None, TypeError, 'Недопустимый тип данных для аргумента', None),
            ('', TypeError, 'Недопустимый тип данных для аргумента', None),
            ([], TypeError, 'Недопустимый тип данных для аргумента', None),
        ]
    )
    def test__get_sql_and_params_list_only_from_indexes_or_names(
            self, create_mocked_engine, param_column_index_or_name_test, exception, match, result
    ):
        test_class = get_instance_test_class(create_mocked_engine)
        pytest_raises_router(
            # ---------------------------------- Passing the function under test:
            test_class._get_sql_and_params_list_only_from_indexes_or_names,
            # ---------------------------------- Other variables
            create_mocked_engine,
            exception,
            match,
            result,
            # ---------------------------------- *args (function params):
            param_column_index_or_name_test,
            # ---------------------------------- ** kwargs

        )











def test__get_sql_and_params_list_only_from_indexes_or_names(create_mocked_engine):
    test_ex = get_instance_class(create_mocked_engine)

    # # 1
    # with pytest.raises(TypeError,
    #                    match="Переданные аргументы не соответствуют единому типу данных, должны быть либо только str (имена колонок), либо только int (индексы колонок)."
    #
    #                    ):
    #     assert test_ex._get_strparams_only_from_indexes_or_names_for_sql(('erwer', 1))

    # 2
    with pytest.raises(TypeError, match="Недопустимый тип данных для аргумента:"):
        assert test_ex._get_sql_and_params_list_only_from_indexes_or_names('')

    # 3
    assert test_ex._get_sql_and_params_list_only_from_indexes_or_names(('erwer', 'wewe'))

    # 4
    assert test_ex._get_sql_and_params_list_only_from_indexes_or_names((1, 2))

    # 5
    with pytest.raises(TypeError, match="Недопустимый тип данных для аргумента:"):
        assert test_ex._get_sql_and_params_list_only_from_indexes_or_names(None)

    #  6
    with pytest.raises(TypeError, match="Недопустимый тип данных для аргумента:"):
        assert test_ex._get_sql_and_params_list_only_from_indexes_or_names([])


# ------------------------------------------------ С подключением к БД.
def test_reader(create_mocked_engine):
    test_instance = get_instance_class(create_mocked_engine)
    assert test_instance._reader(SQL_GET_COLUMN_COMMENTS_BY_NAME, columns='columns')

# row_sql_recorder
# recorder


# ----------------------------------------------------------------------------------------------------------------------
# @pytest.fixture
# def mock_session():
#     mock = MagicMock()
#     return mock

# ------------------------------------- Olds
# @pytest.mark.parametrize(
#     # Передаваемые значения аргументов.
#     "check_value, check_type, expected_result, expected_exception, match",
#     [
#         ('test1', 'test1', str, None, None),
#         # (create_mocked_engine, dict, dict, ValueError, 'Недопустимый тип данных: "str", для аргумента: "qqqq"'),
#         # ((4,), 11, 9, TypeError, 'Недопустимый тип данных:'),
#
#     ]
# )
# def test__validator(create_mocked_engine, check_value, check_type, expected_result, expected_exception, match):
#     test_ex = get_instance_class(create_mocked_engine)
#     if expected_exception:
#
#         with pytest.raises(
#                            check_value=check_value,
#                            check_type=check_type,
#                            expected_result=expected_result,
#                            expected_exception=expected_exception,
#                            match=match
#                            ):
#             test_ex._validator(check_value, check_type)
#     else:
#         assert test_ex._validator(check_value, check_type) == expected_result

# -----------
# def test__stop_sql_injections(create_mocked_engine):
#     test_ex = get_instance_class(create_mocked_engine)
#
#     # 1
#     with pytest.raises(ValueError, match="Ошибка! Недопустимый символ в проверяемой строке."):
#         assert test_ex._stop_sql_injections('')  # Изменить ли поведение (пропускать пустой символ)?
#
#     # 2
#     with pytest.raises(ValueError, match="Ошибка! Недопустимый символ в проверяемой строке."):
#         assert test_ex._stop_sql_injections(' - ')
#
#     # 3
#     assert test_ex._stop_sql_injections('-')
#
#     # 4
#     with pytest.raises(ValueError, match="Ошибка! Попытка внедрения sql-инъекции."):
#         assert test_ex._stop_sql_injections('--')
#
#     # 5
#     with pytest.raises(ValueError, match="Ошибка! Недопустимый символ в проверяемой строке."):
#         assert test_ex._stop_sql_injections('*')
#
#     # 6
#     with pytest.raises(ValueError, match="Ошибка! Недопустимый символ в проверяемой строке."):
#         assert test_ex._stop_sql_injections(';')
#
#     # 7
#     with pytest.raises(ValueError, match="Ошибка! Попытка внедрения sql-инъекции."):
#         assert test_ex._stop_sql_injections('DROP')
#
#     # 8
#     with pytest.raises(ValueError, match="Ошибка! Попытка внедрения sql-инъекции."):
#         assert test_ex._stop_sql_injections('CREATE')
#
#     # 9
#     with pytest.raises(ValueError, match="Ошибка! Попытка внедрения sql-инъекции."):
#         assert test_ex._stop_sql_injections('ALTER')
#
#     # 10
#     with pytest.raises(ValueError, match="Ошибка! Попытка внедрения sql-инъекции."):
#         assert test_ex._stop_sql_injections('INSERT')
#
#     # 11
#     with pytest.raises(ValueError, match="Ошибка! Попытка внедрения sql-инъекции."):
#         assert test_ex._stop_sql_injections('UPDATE')
#
#     # 12
#     with pytest.raises(ValueError, match="Ошибка! Попытка внедрения sql-инъекции."):
#         assert test_ex._stop_sql_injections('DELETE')
#
#     # 13
#     assert test_ex._stop_sql_injections('DELET')
#
#     # 14
#     with pytest.raises(TypeError, match="Недопустимый тип данных для аргумента:"):
#         assert test_ex._stop_sql_injections(1)
#
#     # 15
#     with pytest.raises(ValueError, match="Ошибка! Недопустимый символ в проверяемой строке."):
#         assert test_ex._stop_sql_injections('sdfdksdsd---*D\\')


# # Класс для тестов:
# class BaseMockTcommenter:
#     """
#         Инициализация базового класса с "мок"-ом подключения.
#     """
#
#     instance_test_class = get_test_class()


#
# Фикстура для тестового экземпляра класса:
# @pytest.fixture()  # autouse=True
# def get_test_class(create_mocked_engine):
#     """
#         Возвращает экземпляр Tcommenter с "мок"-ом.
#         Используется для всех тестов.
#     """
#
#     return Tcommenter(engine=create_mocked_engine, name_table="dags_analyzer", schema="audit")

# ====================================================
# # +
# def test__validator(self, create_mocked_engine):
#     test_class = get_instance_test_class(create_mocked_engine)
#     # 1
#     assert test_class._validator('test1', str)
#     # 2
#     with pytest.raises(TypeError, match='Недопустимый тип данных: "str", для аргумента: "test2"'):
#         test_class._validator('test2', dict, int)
#     # 3
#     assert test_class._validator('test3', str, int)

# # +
# def test__check_all_elements(self, create_mocked_engine):
#     test_class = get_instance_test_class(create_mocked_engine)
#     # 1
#     with pytest.raises(TypeError, match='Недопустимый тип данных: "str", для аргумента: "test"'):
#         assert test_class._check_all_elements(check_type=str, args_array='test')
#     # 2
#     assert test_class._check_all_elements(check_type=str, args_array=['test', 'test'])
#     # 3
#     assert test_class._check_all_elements(check_type=str, args_array={'test': 'test'})
#     # 4
#     with pytest.raises(TypeError, match='Недопустимый тип данных: "int", для аргумента: "1".'):
#         test_class._check_all_elements(check_type=str, args_array=1)