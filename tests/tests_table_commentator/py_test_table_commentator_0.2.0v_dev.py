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
# from typing import Optional
from table_commentator.table_commentator import TableCommentator
from tests.connnections.connection import ENGINE_MART_SV

table_comment = {'table': 'Таблица содержит выгрузку данных из Airflow по имеющимся дагам (все доступные атрибуты).'}


#     Можно ли не передавать в тест экземпляр подключения?

def get_instance_class() -> TableCommentator:  # callable
    # test_ex: TableCommentator = TableCommentator(engine=ENGINE_MART_SV, name_table='dags_analyzer', schema='audit')
    return TableCommentator(engine=ENGINE_MART_SV, name_table='dags_analyzer', schema='audit')


def test_get_column_comments():
    test_ex = get_instance_class()
    assert test_ex._validator()


def test__check_all_elements():
    test_ex = get_instance_class()
    assert test_ex._check_all_elements()


def test__sql_formatter():
    test_ex = get_instance_class()
    assert test_ex._sql_formatter()


def test__mutation_sql_by_logic():
    test_ex = get_instance_class()
    assert test_ex._mutation_sql_by_logic()


def test__create_comment():
    test_ex = get_instance_class()
    assert test_ex._create_comment()


def test__set_column_comment():
    test_ex = get_instance_class()
    assert test_ex._set_column_comment()


def test_set_table_comment():
    test_ex = get_instance_class()
    assert test_ex.set_table_comment()


def test_set_view_comment():
    test_ex = get_instance_class()
    assert test_ex.set_view_comment()


def test_set_materialized_view_comment():
    test_ex = get_instance_class()
    assert test_ex.set_materialized_view_comment()


def test_set_column_comment():
    test_ex = get_instance_class()
    assert test_ex.set_column_comment()


def test_get_table_comments():
    test_ex = get_instance_class()
    assert test_ex.get_table_comments() == table_comment


def test_get_all_comments():
    test_ex = get_instance_class()
    assert test_ex.get_all_comments()


def test_get_type_entity_in_db():
    test_ex = get_instance_class()
    assert test_ex.get_type_entity_in_db()


def test_reader():
    test_ex = get_instance_class()
    assert test_ex.reader()  # todo:  Необходимо добавить проверку в ридер на инъекции?


def test_recorder():
    test_ex = get_instance_class()
    assert test_ex.recorder()  # todo:  Необходимо добавить проверку в ридер на инъекции?


def test_row_sql_recorder():
    test_ex = get_instance_class()
    assert test_ex.row_sql_recorder()  # todo:  Необходимо добавить проверку в ридер на инъекции?


def test_save_comments():
    test_ex = get_instance_class()
    assert test_ex.save_comments()  # todo:  Необходимо добавить проверку в ридер на инъекции?
