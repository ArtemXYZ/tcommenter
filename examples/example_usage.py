"""
    Examples of Tcommenter usage.

    Note:
        Each example should be run independently.
        Be sure to comment out other examples before running the current one.
        Additionally, replace the mock engine with a real SQLAlchemy engine.
        If this is not done, all example calls will be executed in sequence
"""

from unittest.mock import MagicMock
from sqlalchemy.engine.base import Engine

from tcommenter import Tcommenter

# You need to set up your own SQLAlchemy engine here.
# For example, you can import your engine like this:
# from .connection import engine_real_db

# Create a mock engine for demonstration purposes only.
# This simulates a SQLAlchemy Engine object without connecting to a real database.
engine = MagicMock(spec=Engine)

# Uncomment the following line to use your real database engine.
# engine = engine_real_db  # Replace this with your actual engine for real use.

# ----------------------------------------------------------------------------------------------------------------------
# Creating an instance of a class to work with a specific entity in the database:
commenter = Tcommenter(engine=engine, name_table='dags_analyzer', schema='audit')

# ------------------------------- Metadata extraction methods:

# Getting a comment to the table (only to the entity itself, excluding comments to columns):
comments = commenter.get_table_comments()
print(comments)  # -> 'The table contains data unloading from Airflow.'

# Getting comments on all columns of an entity:
comments = commenter.get_column_comments()
print(comments)  # -> {'dag_id': 'pass', 'description': 'pass', 'tags': 'pass', pass}

# Getting a comment on a column by column name:
comments = commenter.get_column_comments('tags')
print(comments)  # -> {'tags': 'pass'}'

# Getting comments on columns by index (ordinal number in essence):
comments = commenter.get_column_comments(1, 2)
print(comments)  # -> {'dag_id': 'pass', 'description': 'pass'}

# Getting all available comments on an entity and its columns:
comments = commenter.get_all_comments()
print(comments)  # -> '{'table': 'pass', 'columns': {'dag_id': 'pass', 'description': 'pass', pass}}'

# ------------------------------- Metadata recording methods:
# Writing a comment on an entity:
commenter.set_table_comment('Таблица содержит выгрузку данных из Airflow.')
comments = commenter.get_table_comments()
print(comments)  # -> 'The table contains data unloading from Airflow.'

# Similarly for methods:
# * set_view_comment()
# * set_materialized_view_comment()

# Record comments on an entity by column tag:
commenter.set_column_comment(description='description_test', dag_id='dag_id_test')
comments = commenter.get_column_comments('description', 'dag_id')
print(comments)  # -> {'dag_id': 'dag_id_test', 'description': 'description_test'}

# -------------------------------Service methods:
# Method for determining the type of entity ('table', 'view', 'mview', ...)
type_entity = commenter.get_type_entity()
print(type_entity)  # -> 'table'

# ------------------------------- Examples of metadata overload:

# Getting comments of a special kind compatible with the "save_comments()" method.
# If it is necessary to overload all available comments (first to receive, and after your intermediate
# logic) immediately save to the same or another entity (with the same structure), there is a method "save_comments()".

# A universal method for saving comments of any type (to entities or their columns):
# commenter.save_comments(comments)

# It takes a special kind of data that allows you to explicitly indicate the affiliation of comments from all methods
# to receive comments: "get_table_comments()", "get_column_comments()", "get_all_comments()".
# However, for the first two it is necessary to set the flag: "service_mode=True" (by default service_mode=False).
# There is no "service_mode" in "get_all_comments()", but the output corresponds to this flag.
# The universal "save_comments()" method allows you to save all metadata for both columns and entities at once,
# limited to just one line of code.


# We receive comments in "service_mode" mode before overloading:
comments = commenter.get_table_comments(service_mode=True)
print(comments)  # -> {'table': 'The table contains data unloading from Airflow.'}
commenter.save_comments(comments)

# We receive comments in "service_mode" mode before overloading:
comments = commenter.get_column_comments(2, 3, service_mode=True)
print(comments)  # -> {'columns': {'description': 'pass', 'tags': 'pass'}}
commenter.save_comments(comments)

# We receive all available comments:
comments = commenter.get_all_comments()
print(comments)  # -> {'table': 'pass', 'columns': {pass}}
commenter.save_comments(comments)
