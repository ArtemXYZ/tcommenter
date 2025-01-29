"""
    Модуль интеграционных тестов.
"""

import pytest
import psycopg2
from tests.connnections.connection import ENGINE_REAL_DB
from tcommenter.tcommenter import Tcommenter
from tcommenter.sql.postgre_sql import *

from tests.utilities.utilities import *



def test_get_instance_class() -> Tcommenter:
    return Tcommenter(engine=ENGINE_REAL_DB, name_table='dags_analyzer', schema='audit')





class TestMethodsTcommenterExecuteSQL:
    """
        Тестирование методов требующих извлечения информации из бд (через ".execute()").
    """

    # Перегружаем класс для удобства:
    tools = BaseToolsTests
    test_class = test_get_instance_class()

    # ---------------------------------- *** "_reader" ***
    @pytest.mark.parametrize(
        "exception, match, result",
        [
            (None, None, 'Таблица содержит выгрузку данных из Airflow по имеющимся дагам (все доступные атрибуты).'),
        ]
    )
    def test__reader(self, exception, match, result):
        self.tools.raises_router(
            # ---------------------------------- Passing the function under test:
            self.test_class.get_table_comments,
            # ---------------------------------- Other variables
            exception,
            match,
            result,
            # ---------------------------------- *args:
            # ---------------------------------- ** kwargs
        )


# Забрать названия:
# def test_get_column_comments():
#     # test_ex = test_get_instance_class()
#     assert comments._validator()
#
#
# def test__check_all_elements():
#     test_ex = test_get_instance_class()
#     assert test_ex._check_all_elements()
#
#
# def test__sql_formatter():
#     test_ex = test_get_instance_class()
#     assert test_ex._sql_formatter()
#
#
# def test__mutation_sql_by_logic():
#     test_ex = test_get_instance_class()
#     assert test_ex._mutation_sql_by_logic()
#
#
# def test__create_comment():
#     test_ex = test_get_instance_class()
#     assert test_ex._create_comment()
#
#
# def test__set_column_comment():
#     test_ex = test_get_instance_class()
#     assert test_ex._set_column_comment()
#
#
# def test_set_table_comment():
#     test_ex = test_get_instance_class()
#     assert test_ex.set_table_comment()
#
#
# def test_set_view_comment():
#     test_ex = test_get_instance_class()
#     assert test_ex.set_view_comment()
#
#
# def test_set_materialized_view_comment():
#     test_ex = test_get_instance_class()
#     assert test_ex.set_materialized_view_comment()
#
#
# def test_set_column_comment():
#     test_ex = test_get_instance_class()
#     assert test_ex.set_column_comment()
#
#
# def test_get_table_comments():
#     test_ex = test_get_instance_class()
#     assert test_ex.get_table_comments() == table_comment
#
#
# def test_get_all_comments():
#     test_ex = test_get_instance_class()
#     assert test_ex.get_all_comments()
#
#
# def test_get_type_entity_in_db():
#     test_ex = test_get_instance_class()
#     assert test_ex.get_type_entity()
#
#
# def test_reader():
#     test_ex = test_get_instance_class()
#     assert test_ex.reader()
#
#
# def test_recorder():
#     test_ex = test_get_instance_class()
#     assert test_ex.recorder()
#
#
# def test_row_sql_recorder():
#     test_ex = test_get_instance_class()
#     assert test_ex.row_sql_recorder()
#
#
# def test_save_comments():
#     test_ex = test_get_instance_class()
#     assert test_ex.save_comments()