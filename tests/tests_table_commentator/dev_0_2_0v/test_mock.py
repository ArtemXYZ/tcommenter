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

# from tests.connnections.connection import ENGINE_MART_SV

table_comment = {'table': 'Таблица содержит выгрузку данных из Airflow по имеющимся дагам (все доступные атрибуты).'}


# Можно ли не передавать в тест экземпляр подключения?
# ----------------------------------------------------------------------------------------------------------------------
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


def test_get_instance_class(mocked_engine):
    test_instance = get_instance_class(mocked_engine)
    assert isinstance(test_instance, TableCommentator)




# ---------------- Не требуют подключения:
def test__validator(mocked_engine):    # +
    test_ex = get_instance_class(mocked_engine)
    assert test_ex._validator('qqqq', str)
    with pytest.raises(TypeError, match="Недопустимый тип данных для аргумента:."):
        test_ex._validator('qqqq', dict, int)
    assert test_ex._validator('qqqq', str, int)


def test__check_all_elements(mocked_engine):
    test_ex = get_instance_class(mocked_engine)
    # with pytest.raises(TypeError,):
    #     assert test_ex._check_all_elements(check_type=str, args_array='test')
    assert test_ex._check_all_elements(check_type=str, args_array=['test', 'test'])
    # assert test_ex._check_all_elements(check_type=str, args_array={'test': 'test'})
    # with pytest.raises(TypeError, match="'int' object is not iterable"):
    #     test_ex._check_all_elements(check_type=str, args_array=1)




# def test__stop_sql_injections(mocked_engine):
#     test_ex = get_instance_class(mocked_engine)
#     assert test_ex._stop_sql_injections(check_type=str, args_elements=['eeee', 'wert'])




# ----------------------------------------------------------------------------------------------------------------------
# @pytest.fixture
# def mock_session():
#     mock = MagicMock()
#     return mock
