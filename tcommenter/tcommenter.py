# Copyright (c) 2025 ArtemXYZ
# This project is licensed under the MIT License - see the LICENSE file for details.

"""
    The main module of the "Tcommenter" library is designed to create comments on tables (and other entities)
    in a database (in the current version of the library, only for PostgreSQL).

    Initially, the library was conceived as a tool for working with metadata in DAGs (DAG - Directed Acyclic Graph,
    https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html) "Apache Airflow". The need to
    rewrite the metadata of database objects arises when working with pandas, namely with "pandas.Data Frame.to_sql"
    (https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html). If the method has
    the "if_exists=replace" flag, drops the table before inserting new values. In this case, all metadata is
    they are deleted along with the table. This library was created to solve this kind of problem, as well as to
    ensure the convenience of working without using SQL directly.
"""

# ----------------------------------------------------------------------------------------------------------------------
import re
from typing import TypeVar

# ---------------------------------- Importing third-party libraries
from sqlalchemy.engine import Engine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.elements import TextClause

# -------------------------------- Local modules
from .sql.postgre_sql import *

any_types = TypeVar('any_types')  # Creating a generalized data type.


# ----------------------------------------------------------------------------------------------------------------------
class Tcommenter: # TableCommenter
    """
        "Tcommenter" contains the necessary methods for creating, extracting, and overloading comments to tables
        (and other entities), columns in the database (in the current version of the library, only for PostgreSQL).
    """

    _PARAMS_SQL = {
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
            *** Private validation method (basic). ***

            It is intended for checking the correct transmission of arguments in accordance with the required data type.
            It is used as a nested method in other sections of the library code.

            * Description of the mechanics:

                According to the passed set of valid data types via the "*check_type" parameter,
                The "value" argument being checked is being reconciled based on the "isinstance()" method.
                If at least one of the data types matches, the passed argument "value" is returned,
                otherwise, a TypeError exception is thrown.

            ***

            * Example of a call:

                params = self._validator(_params, dict)

            ***

            :param value: The value to be checked.
            :param check_type: One or more data types allowed for the value.
            :return: Returns the passed original value if it matches one of the check_type types.
            :rtype: any_types, returns the type of the original data type of the argument being checked.
            :raises TypeError: If the value does not match any of the specified data types.
        """

        if isinstance(value, check_type):
            return value
        else:
            raise TypeError(f'Invalid data type: "{type(value).__name__}", for the argument: "{value}".')

    def _stop_sql_injections(self, sql_param_string: str) -> str:
        """
            *** A private method for escaping queries from SQL injections. ***

            It is designed to prevent SQL injections from being passed to the main query of other methods
            through arguments.
            an instance of the class. Combines two approaches, increasing security:

                - regular expressions
                - checking for key SQL commands.

            * Description of the mechanics:

                1) Checking for allowed characters:
                The regular expression ^[a-zA-Z0-9_.\\-]+$ does not skip empty lines and other expressions do
                not match the allowed ones by throwing a ValueError exception. Only lines containing
                are allowed.
                these characters are without spaces:

                    - lowercase and uppercase Latin letters (a-z, A-Z);
                    - numbers (0-9);
                    - underline;
                    - period;
                    - hyphen.

                This allows you to use strings consisting of regular identifiers, table names, and columns.
                or other SQL parameters, but excludes inappropriate characters such as quotation marks, spaces,
                or special characters that may be part of an SQL injection.

                2) Checking for SQL key commands:
                After checking the characters, the string is additionally analyzed for reserved SQL keywords.
                words that can be used in injections. If the string contains any of the following prohibited
                For example, a ValueError exception will be thrown.:

                    - DROP;
                    - CREATE;
                    - ALTER;
                    - INSERT;
                    - UPDATE;
                    - DELETE;
                    - SQL comment "--";
                    - The command completion symbol ";".

            This prevents the use of strings containing malicious SQL constructs that can
            damage the database structure or its contents.


            ***

            * Example of a call:

                self.name_entity: str = self._stop_sql_injections(self._validator(name_table, str))

            ***

            :param sql_param_string: A string that is passed for security verification before use \
                in the SQL query.
            :return: A validated string that is safe to use in an SQL query.
            :rtype: str.
            :raises ValueError: If the string contains invalid SQL characters or keywords, \
                which may be part of the injection.
        """

        sql_param_string = self._validator(sql_param_string, str)

        # Checking for allowed characters:
        if not re.match(r'^[a-zA-Z0-9_.\-]+$', sql_param_string):
            raise ValueError(
                f'String validation error: "{sql_param_string}"! An invalid character was detected. '
                f'Only letters of the Latin alphabet, numbers, symbols are allowed.: "_", ".", "-".'
            )

        # Checking for SQL keywords:
        disallowed_keywords = ["DROP", "CREATE", "ALTER", "INSERT", "UPDATE", "DELETE", "--", ";"]
        if any(keyword in sql_param_string.upper() for keyword in disallowed_keywords):
            raise ValueError(
                f'String verification error:"{sql_param_string}"! '
                f'The presence of SQL keywords was detected:{disallowed_keywords}'
            )

        return sql_param_string

    def _check_all_elements(self, check_type: type, args_array: dict | list | tuple) -> bool:  # *args_elements
        """
            *** A private method for validating the entire set of arguments against a single data type. ***

            It is designed to ensure the correct transmission of the entire passed set of arguments "args_array"
            in accordance with the required data type (specified for verification in "check_type"). Used as
            a nested method in other parts of the library code to control the logic of further data processing
            (redirecting data to condition blocks).

            * Description of the mechanics:

                In accordance with the passed data type (acceptable) through the "check_type" parameter, the following
                is performed reconciliation of the checked array of arguments "args_array" based on the collaboration
                of the "all" and "isinstance" methods. Validation is pre-performed using the "self._validator()" method
                for valid values for arguments:
                "check_type" - checking the transmission of a data type, for example "str", an invalid value, for
                example, "test";
                "args_array" - will be checked for compliance with dict|list | tuple.

            ***

            * Example of a call:

                if self._check_all_elements(str, param_column_index_or_name):
                    pass

            ***

            :param check_type: One or more data types allowed for the value.
            :param args_array: An array of arguments to check element-by-element, only dict|list|tuple \
                types are allowed.
            :return: Returns True if "args_array" matches the type "check_type" \
                and the validation was passed, otherwise False.
            :rtype: bool.
            :raises: Exceptions are possible in nested utility methods (see their description for details).
        """

        # Validation of the passed argument (corresponds to the data type) for further verification:
        valid_type = self._validator(check_type, type)
        # Allowed types for args_array:
        valid_args_array = self._validator(args_array, (dict, list, tuple,))

        # Check if all the elements have the same type:
        return all(isinstance(element, valid_type) for element in valid_args_array)

    def _insert_params_in_sql(self, sql: str, **sql_params) -> str:
        """
            *** A private method for substituting entity name and other parameters in sql queries, if necessary. ***

            Basically, this method is designed to insert the value "self.name_entity" (table name |
            a materialized view|representation) obtained from "__init__". This method
            It does not provide checks for SQL injection, self.name_entity" is checked during class initialization.
            It is unsafe to use "**sql_params" without first checking in "self._stop_sql_injections".
            It is used as a nested method in other sections of the library code.

            * Description of the mechanics:

                If there are no additional arguments "**sql_params", then SQL substitution occurs by default.
                self.name_entity", otherwise formatting is performed taking into account "**sql_params". The
                result of the work the successfully formatted SQL will be returned. In case of incorrectly
                transmitted placeholder name, a ValueError exception is raised.

            ***

            * Example of a call:

                sql = self._insert_params_in_sql(sql_blank)
                sql = self._insert_params_in_sql(sql_blank, shem_name=shem_name_value)

            ***

            :param sql: An SQL template with placeholders.
            :param sql_params: Named arguments.
            :return: Returns formatted SQL (with the entity name by default).
            :type: str.
            :raise ValueError: Caused by a KeyError if the placeholder name is incorrectly passed.,
                there may also be other exceptions in nested utility methods (see their description for details).
        """

        valid_sql = self._validator(sql, str)

        try:
            if not sql_params:
                # Injection check passed during initialization:
                # !: an error occurs if not all keys are transferred
                fin_sql = valid_sql.format(name_entity=self.name_entity)
                # todo: maybe you still need to transfer the schema!?
            else:
                fin_sql = valid_sql.format(name_entity=self.name_entity, **sql_params)

            return fin_sql

        # An error will not occur if all existing keys match, and unnecessary ones are ignored.
        except KeyError as error:
            raise ValueError(
                f'SQL query formatting error: placeholder "{error.args[0]}" did not receive a value, '
                f'(the corresponding argument was not passed, or the placeholder name is incorrect).'
            )

    def _generate_params_list_for_sql(self, params: tuple[int | str] = None) -> list[int | str]:
        """
            *** A private method for generating a list of column names or their indexes. ***

            It is designed to generate a list of column names or their indexes used in SQL queries as
            parameters (for further transmission via multiparams to conn.execute(sql, multiparams)).
            It is used as a nested method in other sections of the library code.

            * Description of the mechanics:

                Converting a tuple (formed from *args from other methods) to a list. First, validation of the input
                arguments, then the list comprehension works.

            ***

            * Example of a call:

                params_list = self._generate_params_list_for_sql(params=param_column_index_or_name)

            ***

            :param params: Parameter values to be substituted in SQL.
            :return: A list of parameters.
            :rtype: list[int | str].
            :raises: Other exceptions are possible in nested utility methods (see their description for details).
        """

        valid_params: tuple = self._validator(params, tuple)
        return [columns for columns in valid_params]

    def _get_sql_and_params_list_only_from_indexes_or_names(
            self,
            param_column_index_or_name: tuple[int | str] | None
    ) -> tuple[str, list[int | str]] | None:
        """
            *** A private method for validating column names or their indexes passed to the query as parameters. ***

            It is designed to ensure the correct transmission of parameters (via multiparams to conn.execute(sql,
            multiparams)) to a query that follows logic: either only column names or only column names are included in
            the parameters. their indexes. This allows you to avoid duplicating the passed parameters the scenario is
            excluded when the user enters both a numerical representation of the column (index) and a string
            representation (column name), which simplifies processing, specifying the behavior of the method.

            * Description of the mechanics:

                Depending on the type of data transmitted (either column names or indexes), the method returns the same
                data, but in the form of a list and the corresponding SQL query for writing column comments.
                At a lower level, validation takes place for compliance with a single data type in the entire data
                array either str only or int only, which provides top-level logic.
                It is used as a nested method in other sections of the library code.

            ***

            * Example of a call:

               # We get SQL (either for names or indexes) and a list of parameters:
                sql, params_list_only_from_indexes_or_name = \
                    self._get_sql_and_params_list_only_from_indexes_or_names(param_column_index_or_name)

            ***

            :param param_column_index_or_name: Parameter values for SQL substitution (either indexes, \
                or column names, or mixed).
            :return: SQL query (either for writing by column name or by index) and a list of parameters (guaranteed
                either indexes only, or column names only).
            :rtype: tuple[str, list[int | str]].
            :raises: Other exceptions are possible in nested utility methods (see their description for details).
        """

        # If the parameters for specific columns were specified:
        if param_column_index_or_name:

            # Checking the first element for the data type (eliminates duplicate checks):
            if isinstance(param_column_index_or_name[0], str):  # check_first_itm_type

                # If we enter the column name (we want to get a comment for the column by its name):
                if self._check_all_elements(str, param_column_index_or_name):

                    # The corresponding SQL and the sequence of column names \
                    # (separated by commas in quotation marks): strparams.
                    return SQL_GET_COLUMN_COMMENTS_BY_NAME, self._generate_params_list_for_sql(
                        params=param_column_index_or_name
                    )

                # If not all elements have the same type or are invalid:
                else:
                    raise TypeError(
                        f'Input validation error! '
                        f'The arguments passed do not correspond to a single data type, received: '
                        f'{[f'(value: "{param}", type: {type(param).__name__})' \
                            for param in param_column_index_or_name]}'
                        f'There must be either only str (column names) or only int (column indexes).'
                    )

            elif isinstance(param_column_index_or_name[0], int):

                # If we enter the column index (we want to get a comment for the columns by indexes):
                if self._check_all_elements(int, param_column_index_or_name):

                    # The corresponding SQL and the sequence of column names \
                    # (separated by commas in quotation marks): strparams.
                    return SQL_GET_COLUMN_COMMENTS_BY_INDEX, self._generate_params_list_for_sql(
                        params=param_column_index_or_name
                    )

                # If not all elements have the same type or are invalid:
                else:
                    raise TypeError(
                        f'Input validation error! '
                        f'The arguments passed do not correspond to a single data type, received: '
                        f'{[f'(value: "{param}", type: {type(param).__name__})' \
                            for param in param_column_index_or_name]}'
                        f'There must be either only str (column names) or only int (column indexes).'
                    )
        # todo there is no else block, there is no action on emptiness.

    def _reader(self, sql: str | TextClause, **params: str | int | list) -> list[tuple]:
        """
           *** Private method of reading data in an SQL database. ***

            It is designed to execute SQL queries to read data with or without parameter substitution.
            It is used as a nested method in other sections of the library code.

            * Description of the mechanics:

                The method is based on the "SQLAlchemy" ("execute()") library. Optional parameter transmission,
                allows you to make a self._reader () is universal. Transmission mechanism via Multi params
                conn.execute (sql, multiparams) provides protection against SQL injection.
                If an error occurs when a request is made, a RuntimeError exception is thrown.
                with errors (SQLAlchemy Error).

            ***

            * Example of a call:

                result = self._reader(sql, placeholder_sales='sales')

            ***

            :param sql:SQL query template.
            :param params: kwargs (key: placeholder name in the SQL template, value: required data).
            :return: Returns a list of tuples, example result: [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')] or [].
            :rtype: list[tuple].
            :raises RuntimeError: If (SQLAlchemyError).
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

        # tuple_list = result.fetchall()  # Returns the Row object
        tuple_list = [tuple(row) for row in result.fetchall()]  # fetchall() returns [] if there is no data.

        # Even if fetchall() returns an empty list, the generator will safely return [].
        return tuple_list

    def _recorder(self, sql: str | TextClause, **params: None | str | int) -> None:
        """
            *** A private method for writing data to an SQL database. ***

            It is designed to execute SQL queries for writing data with or without parameter substitution.
            It is used as a nested method in other sections of the library code.

            * Description of the mechanics:

                The method is based on the "SQLAlchemy" ("execute()") library. Optional parameter transmission,
                allows you to make a self._recorder () is universal. Transmission mechanism via Multi params
                conn.execute (sql, multiparams) provides protection against SQL injection.
                If an error occurs when a request is made, a RuntimeError exception is thrown.
                with errors (SQLAlchemy Error).

            ***

            * Example of a call:

                self._recorder(sql, sales='This comment will be recorded in the metadata for the columns.')

            ***

            :param sql: SQL query template.
            :param params: kwargs (key: placeholder name in the SQL template, value: optional data).
            :return: None.
            :rtype: Note.
            :raises Runtime Error: is (SQLAlchemy Error).
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
            *** A private universal method for creating comments on various entities in a database. ***

            Designed to create comments on various entities in the database, such as (columns| tables |
            materialized views|representations) and encapsulations in public methods designed for
            a specific type of entity (depending on the specified "type_comment").
            It is used as a nested method in a number of the library's main methods for creating comments.

            * Description of the mechanics:

                According to the passed key in "type_comment", for example, 'COLUMN' or 'TABLE', the logic is defined
                further processing by inserting an SQL query (recording comments) into the form and transmitting
                it's a private method responsible for writing information to the database "self._recorder()".
                The logic is divided into two main processing branches:
                - if it is a comment to a column in the database;
                    - otherwise, it is a comment on an entity, such as a table, view, or materialized
                performance.
                After determining the type, the SQL query form is formatted, filling it with values.,
                such as "schema", "name_column", etc. Next, transfer the prepared SQL to "self._recorder()".
                If type_comment == 'COLUMN' and the "name_column" argument is not passed, it is called
                the ValueError exception.

            ***

            * Example of a call:

                self._create_comment(type_comment='TABLE', comment_value=comment)

            ***

            :param type_comment: The value of the entity type, for example, 'TABLE'.
            :param comment_value: A comment to be written to an entity in the database.
            :param name_column: Optional, if type_comment == 'COLUMN'.
            :return: None, in case of a successful write to the database.
            :rtype: Note.
            :raise ValueError: If the value "name_column" is not passed, provided if type_comment == 'COLUMN', \
            and other exceptions are possible in nested utility methods (see their description for details).
        """

        if type_comment == 'COLUMN':
            if name_column:

                comment_value = self._validator(comment_value, str)
                # todo: it may need to be moved by default to _insert_params_in_sql
                mutable_sql_variant = self._insert_params_in_sql(
                    SQL_SAVE_COMMENT_COLUMN,
                    entity_type=self._PARAMS_SQL.get(type_comment),
                    schema=self.schema,  # There is an injection check at the top level during initialization:
                    name_column=self._stop_sql_injections(name_column),
                )

                # Passing comment to write parameters is safe (SQLAlchemy methods).
                self._recorder(mutable_sql_variant, comment=comment_value)

            else:
                raise TypeError(
                    f'The required named argument "name_column" is missing! If the type of comment being created is a '
                    f'column (type_comment == "COLUMN"), passing the "name_column" argument is required! '
                    f'In all other cases, it is optional.'
                )


        # If a comment is not for a column, it means for any other entity (table, view, ...)
        else:
            # todo: it may need to be moved by default to _insert_params_in_sql
            mutable_sql_variant = self._insert_params_in_sql(
                SQL_SAVE_COMMENT,
                entity_type=self._PARAMS_SQL.get(type_comment),
                schema=self.schema,  # There is an injection check at the top level during initialization:
            )

            self._recorder(mutable_sql_variant, comment=comment_value)

    def _set_column_comment(self, comments_columns_dict: dict) -> None:
        """
            *** A private method for creating comments on columns in the database. ***

            Designed to create comments on columns of entities (tables, views, etc.) in a database
            (in the current version of the library, only for PostgreSQL).
            It is almost the same as "self.set_column_comment()", the only difference is in the places of application
            (this method it is used as the basis in "save_comments()") and the type of the accepted argument
            (accepts dict).


            * Description of the mechanics:

                This method uses the service "self._create_comment()" as a nested method with
                type_comment='COLUMN', ensuring the execution of the basic logic. (for more information, see its
                description). By iterating through the entire dictionary, column comments are written one at a time
                (SQL syntax provides entry to only one column).

            ***

            * Example of a call:

                params = {'sales': 'This comment will be recorded in the metadata for the column.'}
                self._set_column_comment(params)

            ***

            :param comments_columns_dict: A comment to be written to an entity in the database.
            :return: None, in case of a successful write to the database.
            :rtype: Note.
            :raises: Other exceptions are possible in nested utility methods (see their description for details).
        """

        if self._validator(comments_columns_dict, dict):

            for key_name_column, value_comment in comments_columns_dict.items():
                self._create_comment(type_comment='COLUMN', comment_value=value_comment, name_column=key_name_column)
        else:
            raise ValueError(
                f'Error: The "comments_columns_dict" argument cannot be empty. '
                f' The transmitted dictionary does not contain any values: {comments_columns_dict}.'
            )

    def get_type_entity(self) -> str:
        """
           *** A method for determining the type of entity ('table', 'view', 'view', ...) of a database by its name. ***

            It is intended for determining the type of database entity (in the current version of the library, only for
            PostgreSQL), such as:
                - r = regular table (Relation),
                - i = Index,
                - S = Sequence,
                - t = TOAST table,
                - v = View,
                - m = Materialized view,
                - c = Composite type,
                - f = Foreign table,
                - p = Partitioned table,
                - I = partitioned Index.

            * Description of the mechanics:

                An SQL query is executed to the system tables in the database responsible for storing statistics
                and by name An entity is defined by its type. Query result options (type_entity):
                'table', 'view', 'mview', 'index', 'sequence', 'toast', 'composite_type', 'foreign_table',
                'partitioned_table', 'partitioned_index'.
                Passing arguments is not required, instance arguments are used.
                of the class during initialization (self.name_entity).
                There may be other exceptions in nested utility methods (see their description for details).

            ***

            * Example of a call:

                comments = Tcommenter(engine=ENGINE, name_table='sales', schema='audit')
                type_entity = comments.get_type_entity()

            ***

            :return: Returns the value of the entity: ('table', 'view', 'mview', ...).
            :rtype: str.
        """

        # Defining the type of entity (options: 'table', 'view', 'view'):
        type_entity = self._reader(SQL_CHECK_TYPE_ENTITY, name_entity=self.name_entity)

        return type_entity[0][0] if type_entity else None

    def set_table_comment(self, comment: str) -> None:
        """
            *** A method for creating comments to tables in a database. ***

            It is intended for creating comments to tables in the database (in the current version of the library,
            only for PostgreSQL).

            * Description of the mechanics:

                This method uses the service "self._create_comment()" as a nested method with
                type_comment='TABLE', ensuring the execution of the basic logic. (for more information,
                see its description).

            ***

            * Example of a call:

                comments = Tcommenter(engine=ENGINE, name_table='sales', schema='audit')
                comments.set_table_comment('This comment will be written to the table in the metadata.')

            ***

            :param comment: A comment to be written to an entity in the database.
            :return: None, in case of a successful write to the database.
            :rtype: Note.
            :raises: Other exceptions are possible in nested utility methods (see their description for details).
        """

        self._create_comment(type_comment='TABLE', comment_value=comment)

    def set_view_comment(self, comment: str) -> None:
        """
            *** A method for creating comments on views in the database. ***

            It is intended for creating comments to views in the database (in the current version of the library, only
            for PostgreSQL).

            * Description of the mechanics:

                This method uses the service "self._create_comment()" as a nested method with
                type_comment='VIEW', ensuring the execution of the basic logic. (for more information,
                see its description).

            ***

            * Example of a call:

                comments = Tcommenter(engine=ENGINE, name_table='sales', schema='audit')
                comments.set_view_comment('This comment will be written to the metadata representation.')

            ***

            :param comment: A comment to be written to an entity in the database.
            :return: None, in case of a successful write to the database.
            :rtype: Note.
            :raises: Other exceptions are possible in nested utility methods (see their description for details).
        """

        self._create_comment(type_comment='VIEW', comment_value=comment)

    def set_materialized_view_comment(self, comment: str) -> None:
        """
            ***A method for creating comments on materialized views in a database. ***

            It is intended for creating comments to views in the database (in the current version of the library, only
            for PostgreSQL).

            * Description of the mechanics:

                This method uses the service "self._create_comment()" as a nested method with
                type_comment='MATERIALIZED', ensuring the execution of the basic logic. (for more information, see
                in his description).

            ***

            * Example of a call:

                comments = Tcommenter(engine=ENGINE, name_table='sales', schema='audit')
                comments.set_materialized_view_comment(
                    'This comment will be written to the materialized representation in the metadata.'
                )

            ***

            :param comment: A comment to be written to an entity in the database.
            :return: None, in case of a successful write to the database.
            :rtype: Note.
            :raises: Other exceptions are possible in nested utility methods (see their description for details).
        """

        self._create_comment(type_comment='MATERIALIZED', comment_value=comment)

    def set_column_comment(self, **comments_columns: str) -> None:
        """
            *** A method for creating comments on columns in the database. ***

            Designed to create comments on columns of entities (tables, views, etc.) in a database
            (in the current version of the library, only for PostgreSQL).
            It is almost the same as "_self.set_column_comment()", the only difference is in the places of application
            (this method used as public for its intended purpose) and the type of argument being accepted
            (accepts kwargs).


            * Description of the mechanics:

                This method uses the service "self._create_comment()" as a nested method with
                type_comment='COLUMN', ensuring the execution of the basic logic. (for more information,
                see its description). By iterating through the entire dictionary, column comments are written one at a
                time (SQL syntax provides entry to only one column).

            ***

            * Example of a call:

                params = {'sales':'This comment will be recorded in the metadata for the column.'}
                self._set_column_comment(params)

            ***

            :param  comments_columns: kwargs (
                key: column name,
                value: comments to be written to a column in the database
                ).
            :return: None, in case of a successful write to the database.
            :rtype: Note.
            :raises: Other exceptions are possible in nested utility methods (see their description for details).
        """

        for key_name_column, value_comment in comments_columns.items():
            self._create_comment(type_comment='COLUMN', comment_value=value_comment, name_column=key_name_column)

    def get_table_comments(self, service_mode: bool = False) -> str | dict[str, str]:
        """
            *** A method for getting comments on tables (and other entities other than columns) in a database. ***

            It is intended for receiving comments on tables and other entities, such as views |
            materialized representations | ... (the name of the method only explicitly indicates the difference from
            the method of obtaining column comments) in the database (in the current version of the library,
            only for PostgreSQL).

            * Description of the mechanics:

                This method uses the nested service "self._reader()". It gets "self.name_entity"
                from "__init__" as the value for the placeholder in an SQL query for comments from
                PostgreSQL service tables responsible for storing various statistics and metadata.
                (tps://postgrespro.ru/docs/postgresql/14/monitoring-stats).

                Next, the response result is converted:
                either to a string with a comment - by default (service_mode=False),
                    or if service_mode=True to a dictionary of type {'table': 'table_comment'}.

                Mode "service_mode"=True is intended to provide output data compatible with the method
                "save_comments()", in case it is necessary to overload the comments (first receive, and then your
                intermediate logic) immediately save to the same or another entity (with the same structure).
                For more information, see the description of "save_comments()".

            ***

            * Example of a call:

                comments = Tcommenter(engine=ENGINE, name_table='sales', schema='audit')

                # -> 'comment'.
                comment_table_str = comments.get_table_comments()

                # -> {'table': 'comment'}.
                comment_table_dict = comments.get_table_comments(service_mode=True)

            ***

            :param service_mode: The "Switch" of the output type, by default False.
            :return: If service_mode=False -> str (row with table comment), \
                if service_mode=True -> dict[str, str].
            :rtype: str | dict[str, str].
            :raises: Other exceptions are possible in nested utility methods (see their description for details).
        """

        # str_mode validation:
        service_mode = self._validator(service_mode, bool)

        # Getting the raw data after the request (list of tuples):
        table_comment_tuple_list: list[tuple] = self._reader(
            SQL_GET_TABLE_COMMENTS, name_entity=self.name_entity
        )

        # Convert (refer to the first element in the list (to a tuple, there will always be only one) and unpack:
        if table_comment_tuple_list:
            table_comment = table_comment_tuple_list[0][0]
        else:
            table_comment = ''  # If there is no comment, we return an empty string.

        return {'table': table_comment} if service_mode else table_comment

    def get_column_comments(self,
                            *column_index_or_name: int | str,
                            service_mode: bool = False
                            ) -> dict[str, str] | dict[str, dict[str, str]]:
        """
            *** A method for getting comments on columns by their name or by index for database entities. ***

            It is intended for receiving comments on columns of various entities, such as views |
            materialized views | ... in the database (in the current version of the library, only for PostgreSQL).

            * Description of the mechanics:

                This method uses the nested service "self._reader()". He gets "self.name_entity"
                from "__init__", as well as the "*column_index_or_name" passed by the user - the name or index of
                the required columns as values for placeholders in an SQL query. Comments on entities are stored
                in PostgreSQL service tables responsible for processing various statistics and metadata
                (tps://postgrespro.ru/docs/postgresql/14/monitoring-stats).

                Next, the response result is converted:
                either to a dictionary of the type ({'columns': {'column_name': 'comment'}}) - by default
                (service_mode=False), or if service_mode=True to the dictionary type {'column_name': 'comment'}.

                service_mode mode=True is intended to provide output data compatible with the method
                "save_comments()", in case it is necessary to overload the comments (first receive, and then your
                intermediate logic) immediately save to the same or another entity (with the same structure).
                For more information, see the description of "save_comments()".
                Important!
                If you pass both the index and the column name to the parameters together, this will cause an exception!
                The method processes there is only one type of parameter (either indexes only or column names).

            ***

            * Example of a call:

                comments = Tcommenter(engine=ENGINE, name_table='sales', schema='audit')

                # -> {'column_name_1': 'comment_1', 'column_name_2': 'comment_2', ... }
                comment_table_dict = comments.get_table_comments()

                # -> {'columns': {'column_name_1': 'comment_1', 'column_name_2': 'comment_2', ... }}
                comment_table_str = comments.get_table_comments(str_mode=True)

            ***

            :param column_index_or_name: *args - indexes or column names for which comments should be counted.
            :param service_mode: The "Switch" of the output type, By default False.
            :return: If service_mode=False -> dict[str, str], if service_mode=True -> dict[str, dict[str, str]].
            :rtype: dict[str, str] | dict[str, dict[str, str]].
            :raises: Other exceptions are possible in nested utility methods (see their description for details).
        """

        self._validator(service_mode, bool)

        # Default value - we get all the comments to the columns in the table (without specifying the index or name):
        param_column_index_or_name: tuple[int | str] | None = None or column_index_or_name

        if param_column_index_or_name:

            # We get sql (either for names or indexes) and a list of parameters:
            sql, params_list_only_from_indexes_or_name = self._get_sql_and_params_list_only_from_indexes_or_names(
                param_column_index_or_name
            )

            # Passing the updated sql and parameters:
            column_comments_tuple_list: list[tuple] = self._reader(
                sql,
                name_entity=self.name_entity,
                columns=params_list_only_from_indexes_or_name
            )

        else:

            # Passing sql to extract all comments without parameters:
            column_comments_tuple_list: list[tuple] = self._reader(
                SQL_GET_ALL_COLUMN_COMMENTS,
                name_entity=self.name_entity
            )

        # Generating a dictionary from a list of tuples:
        # Unpacks a tuple of 2 elements (1, 'Alice'), taking the first as key and the second as value:
        _column_comments_dict = {key: value for key, value in column_comments_tuple_list}

        return {'columns': _column_comments_dict} if service_mode else _column_comments_dict

    def get_all_comments(self) -> dict[str, str | dict]:
        """
            *** A method for getting all comments for an entity (to it and its columns) in the database. ***

            It is intended to receive all available comments for an entity (representation| materialized
            views | ...), its own, and columns (in the current version of the library, only for PostgreSQL).

            * Description of the mechanics:

                The main and only logic is to add two dictionaries obtained as a result of the work.
                "get_table_comments()" and "get_column_comments()" with service_mode=True.

            ***

            * Example of a call:

                comments = Tcommenter(engine=ENGINE, name_table='sales', schema='audit')

                # -> {'table': 'table_comment', 'columns': {'column_1': 'column_1_comment', ...}}.
                all_comments_dict = comments.get_all_comments(service_mode=True)
                comments.save_comments(all_comments_dict)

            ***

            :return: {'table': 'table_comment', 'columns': {'column_1': 'column_1_comment', ...}}.
            :rtype: dict[str, str | dict].
            :raises: There may be other exceptions in nested utility methods (see their description for details).
        """

        # Getting all the comments:
        table_comment = self.get_table_comments(service_mode=True)
        column_comments_dict = self.get_column_comments(service_mode=True)

        # Converting the received data into a single dictionary:
        all_comments_table_dict = table_comment | column_comments_dict

        return all_comments_table_dict  # на выходе: {'table': set_table_comment, 'columns': column_comments_dict}

    def save_comments(self, comments_dict: dict[str, str | dict]) -> None:  # Self , schema: str
        """
            *** A method for saving comments of any type (to entities or their columns) to a database. ***

            It is intended for saving comments of any type (to entities or their columns) to the database (in the
            current library versions, only for PostgreSQL). The "save_comments()" method is universal, it is automatic
            defines which type of entity comments are intended for, which makes it useful when needed.
            save all metadata at once for both columns and entities by calling just one method.
            It is used if it is necessary to overload the comments (first to receive, and after your
            intermediate logic) immediately save to the same or another entity (with the same structure).

            * Description of the mechanics:

                The output data of the "service structure" type (in the methods for receiving comments, you must set
                service_mode=True): {'columns': {...}} | {'table': 'table_comment'}, ensure their correct operation
                processing. The 'table' | 'columns' keys are markers for enabling specific type of processing logic
                comments on entities or their columns. Next, the method automatically determines the type of entity
                and calls the appropriate methods to save metadata.

            ***

            * Example of a call:

                comments = Tcommenter(engine=ENGINE, name_table='sales', schema='audit')

                # Option 1 (similar for data from get_column_comments())
                # -> {'table': 'comment'}.
                comment_table_dict = comments.get_table_comments(service_mode=True)
                comments.save_comments(comment_table_dict)

                # Option 2
                # -> {'table': 'table_comment', 'columns': {'column_1': 'column_1_comment', ...}}.
                comment_table_dict = comments.get_all_comments(service_mode=True)
                comments.save_comments(comment_table_dict)

            ***

            :param comments_dict: Type dictionary:
                {'table': 'table_comment', 'columns': {'column_1': 'column_1_comment', ...}} |
                {'table': 'table_comment'} | {'columns': {'column_1': 'column_1_comment', ...}}.
            :return: None.
            :rtype: Note.
            :raise ValueError: An exception will be thrown if an attempt is made to save comments on the entity type. \
                not provided in the current library implementation. The method only works with tables, \
            views, and materialized views.
                There may be other exceptions in nested utility methods (see their description for details).
        """

        if comments_dict:

            # Validation for the data type of input arguments \
            # (only if there is a dictionary, we continue processing further):
            comments_dict = self._validator(comments_dict, dict)

            # # Defining the type of entity (options: 'table', 'view', 'view'):
            type_entity = self.get_type_entity()

            # We perform validation (we exclude working with database entities that are unsupported by the method):
            if type_entity not in ('table', 'view', 'mview'):
                raise ValueError(
                    f'Error: It is impossible to save a comment to the entity specified in the class instance! '
                    f'The method only works with tables, views, and materialized views. '
                    f'Entity type {type_entity}, schema: "{self.schema}", name: "{self.name_entity}").'

                )

            # Input data analysis:
            for key, value in comments_dict.items():

                # If the input data contains comments on entities:
                if key == 'table':

                    if type_entity == 'table':
                        self.set_table_comment(value)

                    elif type_entity == 'view':
                        self.set_view_comment(value)

                    elif type_entity == 'mview':
                        self.set_materialized_view_comment(value)

                # If the input data contains comments on the entity columns:
                elif key == 'columns':

                    # Void checking and validation:
                    if self._validator(value, dict):

                        # The method accepts a dictionary:
                        self._set_column_comment(value)

                    else:
                        raise ValueError(f'Error: there is no data to process (comments for columns)! '
                                         f'Received: {comments_dict}!')


                else:
                    structure = "{'columns': {...}} | {'table': 'table_comment'} | {'columns': {...}, \
                    table': 'table_comment'}"
                    raise ValueError(
                        f'Error: The passed "comments_dict" argument does not match the required input data service '
                        f'structure set in the method for proper operation! The normal "service structure" '
                        f'looks like this: {structure}. Received: {comments_dict}'
                    )

        else:
            raise ValueError(
                f'Error: data is being deleted for processing! '
                f'The passed argument "comments_dict" does not contain information:"{comments_dict}".'
            )

    def __str__(self):
        return (
            f'{self.__class__.__name__}(schema: {self.schema},'
            f' name_table: {self.name_entity}, engine: {self.engine}).'
        )

    def __repr__(self):
        return (
            f'{self.__class__.__name__}(schema: {self.schema},'
            f' name_table: {self.name_entity}, engine: {self.engine}).'
        )
# ----------------------------------------------------------------------------------------------------------------------
