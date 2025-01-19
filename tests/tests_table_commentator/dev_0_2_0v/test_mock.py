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
        get_type_entity_in_db
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
from table_commentator.table_commentator import TableCommentator
from table_commentator.sql.postgre_sql import *

# from tests.connnections.connection import ENGINE_MART_SV

table_comment = {'table': 'Таблица содержит выгрузку данных из Airflow по имеющимся дагам (все доступные атрибуты).'}


# Можно ли не передавать в тест экземпляр подключения?
# ----------------------------------------------------------------------------------------------------------------------
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

_SQL_SAVE_COMMENT = """COMMENT ON TABLE "audit"."dags_analyzer" IS ':comment'"""
# ----------------------------------------- Тесты с мок бд.

# Создаем мок для engine:
@pytest.fixture
def mocked_engine():
    engine = MagicMock()  # Заглушка SQLAlchemy Engine
    engine.__class__ = Engine  # Указываем, что это мок класса Engine
    engine.connect.return_value.__enter__.return_value.execute = MagicMock()  # Мокаем execute
    return engine


# Возвращаем экземпляр класса с моком:
def get_instance_class(mocked_engine) -> TableCommentator:
    return TableCommentator(engine=mocked_engine, name_table="dags_analyzer", schema="audit")


# +
def test_get_instance_class(mocked_engine):
    test_instance = get_instance_class(mocked_engine)
    assert isinstance(test_instance, TableCommentator)


def test_reader(mocked_engine):
    test_instance = get_instance_class(mocked_engine)
    assert test_instance._reader(SQL_GET_COLUMN_COMMENTS_BY_NAME, columns='columns')


# row_sql_recorder
# recorder
# ------------------------------------- Не требуют подключения:
# +
def test__validator(mocked_engine):  # +
    test_ex = get_instance_class(mocked_engine)
    # 1
    assert test_ex._validator('qqqq', str)
    # 2
    with pytest.raises(TypeError, match="Недопустимый тип данных для аргумента:."):
        test_ex._validator('qqqq', dict, int)
    # 3
    assert test_ex._validator('qqqq', str, int)

# +
def test__check_all_elements(mocked_engine):
    test_ex = get_instance_class(mocked_engine)
    # 1
    with pytest.raises(TypeError, match="Недопустимый тип данных для аргумента:"):
        assert test_ex._check_all_elements(check_type=str, args_array='test')
    # 2
    assert test_ex._check_all_elements(check_type=str, args_array=['test', 'test'])
    # 3
    assert test_ex._check_all_elements(check_type=str, args_array={'test': 'test'})
    # 4
    with pytest.raises(TypeError, match="Недопустимый тип данных для аргумента:"):
        test_ex._check_all_elements(check_type=str, args_array=1)

def test__stop_sql_injections(mocked_engine):
    test_ex = get_instance_class(mocked_engine)

    # 1
    with pytest.raises(ValueError, match="Ошибка! Недопустимый символ в проверяемой строке."):
        assert test_ex._stop_sql_injections('')  # Изменить ли поведение (пропускать пустой символ)?

    # 2
    with pytest.raises(ValueError, match="Ошибка! Недопустимый символ в проверяемой строке."):
        assert test_ex._stop_sql_injections(' - ')

    # 3
    assert test_ex._stop_sql_injections('-')

    # 4
    with pytest.raises(ValueError, match="Ошибка! Попытка внедрения sql-инъекции."):
        assert test_ex._stop_sql_injections('--')

    # 5
    with pytest.raises(ValueError, match="Ошибка! Недопустимый символ в проверяемой строке."):
        assert test_ex._stop_sql_injections('*')

    # 6
    with pytest.raises(ValueError, match="Ошибка! Недопустимый символ в проверяемой строке."):
        assert test_ex._stop_sql_injections(';')

    # 7
    with pytest.raises(ValueError, match="Ошибка! Попытка внедрения sql-инъекции."):
        assert test_ex._stop_sql_injections('DROP')

    # 8
    with pytest.raises(ValueError, match="Ошибка! Попытка внедрения sql-инъекции."):
        assert test_ex._stop_sql_injections('CREATE')

    # 9
    with pytest.raises(ValueError, match="Ошибка! Попытка внедрения sql-инъекции."):
        assert test_ex._stop_sql_injections('ALTER')

    # 10
    with pytest.raises(ValueError, match="Ошибка! Попытка внедрения sql-инъекции."):
        assert test_ex._stop_sql_injections('INSERT')

    # 11
    with pytest.raises(ValueError, match="Ошибка! Попытка внедрения sql-инъекции."):
        assert test_ex._stop_sql_injections('UPDATE')

    # 12
    with pytest.raises(ValueError, match="Ошибка! Попытка внедрения sql-инъекции."):
        assert test_ex._stop_sql_injections('DELETE')

    # 13
    assert test_ex._stop_sql_injections('DELET')

    # 14
    with pytest.raises(TypeError, match="Недопустимый тип данных для аргумента:"):
        assert test_ex._stop_sql_injections(1)

    # 15
    with pytest.raises(ValueError, match="Ошибка! Недопустимый символ в проверяемой строке."):
        assert test_ex._stop_sql_injections('sdfdksdsd---*D\\')

def test__get_strparams_only_from_indexes_or_names_for_sql(mocked_engine):
    test_ex = get_instance_class(mocked_engine)

    # # 1
    # with pytest.raises(TypeError,
    #                    match="Переданные аргументы не соответствуют единому типу данных, должны быть либо только str (имена колонок), либо только int (индексы колонок)."
    #
    #                    ):
    #     assert test_ex._get_strparams_only_from_indexes_or_names_for_sql(('erwer', 1))

    # 2
    with pytest.raises(TypeError, match="Недопустимый тип данных для аргумента:"):
        assert test_ex._get_params_list_only_from_indexes_or_names_for_sql('')

    # 3
    assert test_ex._get_params_list_only_from_indexes_or_names_for_sql(('erwer', 'wewe'))

    # 4
    assert test_ex._get_params_list_only_from_indexes_or_names_for_sql((1, 2))

    # 5
    with pytest.raises(TypeError, match="Недопустимый тип данных для аргумента:"):
        assert test_ex._get_params_list_only_from_indexes_or_names_for_sql(None)

    #  6
    with pytest.raises(TypeError, match="Недопустимый тип данных для аргумента:"):
        assert test_ex._get_params_list_only_from_indexes_or_names_for_sql([])

def test__insert_params_in_sql(mocked_engine):
    test_ex = get_instance_class(mocked_engine)
    # 1 +
    assert test_ex._insert_params_in_sql(SQL_GET_TABLE_COMMENTS) == _SQL_GET_TABLE_COMMENTS

    # 2 +
    assert test_ex._insert_params_in_sql(SQL_SAVE_COMMENT, entity_type='TABLE', schema='audit') == _SQL_SAVE_COMMENT

    # 3 +
    assert test_ex._insert_params_in_sql(
        SQL_SAVE_COMMENT, entity_type='TABLE', schema='audit', columnwewe='asd'
    ) == _SQL_SAVE_COMMENT # todo Ошибка не возникнет если, все существующие ключи совпадут, а излишние проигнорируются.

    # 4 +  # todo Ошибка  KeyError возникнет если не передать нужный ключ, переделать ValueError: Ошибка форматирования sql-запроса: переданный ключ не найден.
    assert test_ex._insert_params_in_sql(SQL_SAVE_COMMENT, entity_type='TABLE',) == _SQL_SAVE_COMMENT

# ----------------------------------------------------------------------------------------------------------------------
# @pytest.fixture
# def mock_session():
#     mock = MagicMock()
#     return mock

# ------------------------------------- Более новый подход
# @pytest.mark.parametrize(
#     # Имена передаваемых аргументов.
#     "sql_param_string, expected_exception, match",
#     # Передаваемые значения аргументов.
#     [
#         ('', ValueError, "Ошибка! Недопустимый символ в проверяемой строке."),
#         (' - ', ValueError, "Ошибка! Недопустимый символ в проверяемой строке."),
#         ('-', ValueError, "Ошибка! Недопустимый символ в проверяемой строке."),
#         ('*', ValueError, "Ошибка! Недопустимый символ в проверяемой строке."),
#         (';', ValueError, "Ошибка! Недопустимый символ в проверяемой строке."),
#         ('DROP', ValueError, "Ошибка! Недопустимый символ в проверяемой строке."),
#         ('CREATE', ValueError, "Ошибка! Недопустимый символ в проверяемой строке."),
#         ('ALTER', ValueError, "Ошибка! Недопустимый символ в проверяемой строке."),
#         ('INSERT', ValueError, "Ошибка! Недопустимый символ в проверяемой строке."),
#         ('UPDATE', ValueError, "Ошибка! Недопустимый символ в проверяемой строке."),
#         ('DELETE', ValueError, "Ошибка! Недопустимый символ в проверяемой строке."),
#         ('DELET', ValueError, "Ошибка! Недопустимый символ в проверяемой строке."),
#         (1, ValueError, "Ошибка! Недопустимый символ в проверяемой строке."),
#         ('sdfdksdsd---*D\\', ValueError, "Ошибка! Недопустимый символ в проверяемой строке."),
#     ]
# )
# def test__stop_sql_injections(mocked_engine, sql_param_string, expected_exception, match):
#     test_ex = get_instance_class(mocked_engine)
#
#     with pytest.raises(expected_exception, match=match):
#         test_ex._stop_sql_injections(sql_param_string)
