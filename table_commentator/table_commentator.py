"""
    Модуль создания комментариев к таблицам после перезагрузки в них данных.
    Справка:
    В Пандас используется метод сохранения данных, где под "капотом" перед сохранением сначала удаляется таблица,
    что приводит к потере метаданных таблицы (комментариев).
"""


# ----------------------------------------------------------------------------------------------------------------------
from typing import Union
# ---------------------------------- airflow
# ---------------------------------- Импорт сторонних библиотек
from sqlalchemy.engine import Engine
from sqlalchemy import text
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL


# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from sqlalchemy.ext.asyncio import AsyncEngine

# -------------------------------- Локальные модули

# ----------------------------------------------------------------------------------------------------------------------
class TableCommentator:
    """
        Класс для создания комментариев к таблицам и колонкам после перезагрузки в них данных.
        Справка:
        В Пандас используется метод сохранения данных, где под "капотом"  перед сохранением сначала удаляется таблица,
        что приводит к потере метаданных таблицы (комментариев).
    """

    PARAMS_SQL = {
        'TABLE': 'TABLE',
        'VIEW': 'VIEW',
        'MATERIALIZED': 'MATERIALIZED VIEW',
        'COLUMN': 'COLUMN',
    }

    # COMMENT ON TABLE "schema"."table" IS 'comment'
    SQL_SAVE_COMMENT = """COMMENT ON {0} "{1}"."{2}" IS '{3}'"""  # TABLE

    # COMMENT ON COLUMN "schema"."table"."name_table" IS 'comment'
    SQL_SAVE_COMMENT_COLUMN = """COMMENT ON {0} "{1}"."{2}"."{3}" IS '{4}'"""  # COLUMN

    def __init__(self, engine: Engine, name_table: str, schema: str):  # | AsyncEngine
        self.engine = self._validator(engine, Engine)
        self.name_table = self._validator(name_table, str)
        self.schema = self._validator(schema, str)

    @staticmethod
    def _validator(value: any, check_type: any) -> any:
        """
            Проверка на корректную передачу входных параметров с требуемым типом данных.
        """
        if isinstance(value, check_type):
            return value
        else:
            raise TypeError(f'Недопустимый тип данных для аргумента: {value}.')

    def recorder(self, sql: Union[str, text]) -> None:  # sql | text # executer | list[tuple] Union[int, str]
        """
           Метод выполняет запросы к базе данных на запись.
        """
        engine: Engine = self.engine

        if isinstance(sql, str):
            sql = text(sql)

        # result.fetchall() if result else None - не верно

        with engine.connect() as conn:
            with conn.begin():
                conn.execute(sql)

            # tuple_list = result.fetchall()

        # if tuple_list:
        #     return tuple_list
        # else:
        #     return None

    def _create_comment(self, type_comment: str, comment: str, name_column: str = None) -> None:
        """
            Универсальный метод для создания комментариев к различным сущностям в базе данных.
        """

        if type_comment == 'COLUMN':
            if name_column is not None:
                mutable_sql_variant = self.SQL_SAVE_COMMENT_COLUMN.format(
                    self.PARAMS_SQL.get(type_comment),
                    self.schema,
                    self.name_table,
                    name_column,
                    self._validator(comment, str)
                )
            else:
                raise ValueError(f'Не передано значение для аргумента: name_column.')

        else:
            mutable_sql_variant = self.SQL_SAVE_COMMENT.format(
                self.PARAMS_SQL.get(type_comment),
                self.schema,
                self.name_table,
                self._validator(comment, str)
            )

            # Добавление комментариев
            # with self.engine.connect() as conn:
            #     conn.execute(text(mutable_sql))

        self.recorder(mutable_sql_variant)

    def row_sql_recorder(self, sql: str) -> None:
        """
            Метод для создания сырых запросов на запись данных.
        """

        self.recorder(sql)

    def table_comment(self, comment: str) -> None:
        """
            Метод для создания комментариев к TABLE.
        """

        self._create_comment(type_comment='TABLE', comment=comment)

    def view_comment(self, comment: str) -> None:
        """
            Метод для создания комментариев к VIEW.
        """

        self._create_comment(type_comment='VIEW', comment=comment)

    def materialized_comment(self, comment: str) -> None:
        """
            Метод для создания комментариев к MATERIALIZED VIEW.
        """

        self._create_comment(type_comment='MATERIALIZED', comment=comment)

    def column_comment(self, **comments_columns: str) -> None:
        """
            Метод для создания комментариев к COLUMN.
        """

        for key_name_column, value_comment in comments_columns.items():
            self._create_comment(type_comment='COLUMN', comment=value_comment, name_column=key_name_column)










# ----------------------------------------------------------------------------------------------------------------------
# comments = TableCommentator(engine=engine_, name_table='', schema='')
# comments.table_comment()
