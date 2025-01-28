# pip freeze > requirements.txt


# Выявленные ошибки:   d = {'columns': {1: ''}}, в " {1:" - в запросе ошибка так как не существует колонки такой.
# Необходимо добавить обработку  (надо ли?) валидацию на интеджер для имени колонки наверное.
# возможно добавить это в следующий выпуск
# создать тесты в сл выпуске


# ***
# self.table_comments_dict: Union[dict[str, str], None] = None
# self.column_comments_dict: Union[dict[str, str], None] = None
# self.all_comments_table_dict: Union[dict[str, str], None] = None


# def get_table_comments(self) -> Self:  # или 'TableCommentator' на Python < 3.11., table_name: str
#     """
#         Метод (публичный) для получения комментариев к таблицам.
#         На выходе: str - строка с комментарием к таблице.
#     """
#
#     self.table_comments_dict = self._get_table_comments()
#     return self


# def get_column_comments(self, *column_index_or_name: str) -> dict[str, text]:  # Self  , table_name: str
#     """
#         Метод (публичный) для получения комментариев к колонкам либо по индексу, либо по имени колонки.
#             Если передать в параметры индекс и имя колонки - это вызовет исключение!
#             Метод обрабатывает параметры только одного типа (либо только индексы, либо имена колонок).
#         На выходе - словарь с комментариями
#
#     """
#
#     self.column_comments_dict = self._get_column_comments(column_index_or_name)
#     return self

# def _validate_table_exists(self) -> bool:
#     sql = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{self.name_table}')"
#     result = self.reader(sql)
#     return result[0][0] if result else False
#
#
# # Оптимизация sql-запросов: Использование форматирования строк через .format() в sql небезопасно.
# # Замените это на параметризованные запросы SQLAlchemy для предотвращения sql-инъекций:
# def get_table_comments(self) -> dict[str, str]:
#     sql = text("SELECT obj_description(oid) FROM pg_class WHERE relname = :table_name")
#     table_comment_tuple_list = self.reader(sql.bindparams(table_name=self.name_table))
#     table_comment = table_comment_tuple_list[0][0] if table_comment_tuple_list else ''
#     return {'table': table_comment}
#
#
# sql = text("SELECT * FROM my_table WHERE id = :id")
# sql = sql.bindparams(bindparam("id", value=42))
#
# # Когда использовать bindparam?
# # Использование bindparam может быть полезным, если:
# #
# # Вы хотите создавать sql-запросы динамически, но при этом избегать уязвимостей.
# # Вам нужно задать параметры заранее для последующей подстановки (например,
# # при многократных вызовах одного запроса с разными параметрами).
# # Заменяем dict[str, str | dict] на Dict[str, Union[str, Dict]]
#
# my_dict: Dict[str, Union[str, Dict]] = {
#     "key1": "value",
#     "key2": {
#         "nested_key": "nested_value"
#     }
# }


# validation from sql injection


# if sys.version_info < (3, 13):
#     from .loader import findTestCases as findTestCases, getTestCaseNames as getTestCaseNames, makeSuite as makeSuite
#
#     __all__ += ["getTestCaseNames", "makeSuite", "findTestCases"]
#
# if sys.version_info >= (3, 11):
#     __all__ += ["enterModuleContext", "doModuleCleanups"]
#
# if sys.version_info < (3, 12):
#     def load_tests(loader: TestLoader, tests: TestSuite, pattern: str | None) -> TestSuite: ...
#
# def __dir__() -> set[str]: ...


#             params = {"id": 42}
#
#             # Выполнение запроса с параметрами
#             result = connection.execute(prepared_sql, params)



# if sys.version_info >= (3, 11):
#     from .case import doModuleCleanups as doModuleCleanups, enterModuleContext as enterModuleContext
#
# # Определяем список публичных объектов для этого модуля:
# __all__ = ['recorder', 'get_table_comments', 'reader', 'row_sql_recorder', 'get_type_entity_in_db',
#            'set_table_comment', 'set_view_comment', 'set_materialized_view_comment', 'set_column_comment',
#            'get_table_comments', 'get_column_comments', 'get_all_comments', 'save_comments'
#            ]


# Спецификация семантического управления версиями
# Программное обеспечение, использующее семантическое управление версиями, должно объявлять общедоступный API. Этот API может быть объявлен в самом коде или существовать исключительно в документации. Как бы то ни было, он должен быть точным и исчерпывающим.
#
# Номер версии имеет вид X.Y.Z, где X, Y и Z — целые числа. X — основная версия, Y — дополнительная версия, а Z — версия исправления. Каждый элемент увеличивается в числовом порядке, так что версия 1.0.10 следует за версией 1.0.9.
#
# При маркировке релизов в системе контроля версий тег версии ДОЛЖЕН иметь вид «vX.Y.Z», например «v3.1.0».
#
# После выпуска пакета с определённой версией его содержимое НЕ ДОЛЖНО изменяться. Любые изменения ДОЛЖНЫ выпускаться как новая версия.
#
# Нулевая основная версия (0.y.z) предназначена для начальной разработки. В любой момент что-то может измениться. Публичный API не следует считать стабильным.
#
# Версия 1.0.0 определяет общедоступный API. Способ увеличения номера версии теперь зависит от этого общедоступного API и от того, как он меняется.
#
# Версия исправления Z (x.y.Z	x > 0) ДОЛЖНО увеличиваться, если вводятся только исправления ошибок, совместимые с предыдущими версиями. Исправление ошибки определяется как внутреннее изменение, устраняющее некорректное поведение.
# Младшая версия Y (x.Y.z	x > 0) ДОЛЖНО увеличиваться, если в общедоступный API вводится новая функция, совместимая с предыдущими версиями. Оно МОЖЕТ увеличиваться, если в закрытый код вводится новая существенная функция или улучшения. Оно МОЖЕТ включать изменения на уровне патчей.
# Основная версия X (X.y.z	X > 0) ДОЛЖНО увеличиваться, если в общедоступный API вносятся изменения, несовместимые с предыдущими версиями. Это МОЖЕТ включать незначительные изменения и исправления.

# Здесь планируется разместить папки (структуру) для дашборда со статистикой по базе данных имеющихся на сервере.
#  - Наибольшее внимание уделяется наличию заполненных комментариев в созданных таблицах,
#  - а так же наличие брошенных таблиц ("мертвых").


# ----------------------------------------------------------------------------------------------------------------------
#
    # def _sql_formatter(self, sql: str, params: tuple = None) -> text:  #
    #     """
    #         Служебный вложенный метод для форматирования sql (передачи необходимых параметров в запрос).
    #             На входе: необработанный sql.
    #                 Принимает параметры "table_name" по умолчанию и дополнительные
    #                 (генерирует фрагмент: строку типа "'param1', 'param2'", подставляя в sql).
    #             На выходе:
    #                 отформатированный sql с параметрами, по умолчанию без передачи дополнительных параметров
    #                 форматируется только table_name (атрибут экземпляра класса).
    #     """
    #
    #     valid_sql = self._validator(sql, str)
    #
    #     # Если указаны параметры, тогда форматируем:
    #     if params:
    #         valid_params = self._validator(params, tuple)
    #
    #         # Имя колонки (сохраняем последовательность через запятую в кавычках:
    #         columns_string = ', '.join(f"'{columns}'" for columns in valid_params)
    #
    #         # Форматирование sql_str с учетом параметров
    #         _mutable_sql = valid_sql.format(table_name=self.name_entity, columns=columns_string)
    #
    #     else:
    #         # Подставляем только name_entity:
    #         mutable_sql = valid_sql.format(table_name=self.name_entity)
    #
    #     return mutable_sql


    # def _mutation_sql_by_logic(self, param_column_index_or_name: str | tuple[int, str]) -> str:
    #     """
    #        Метод изменения sql запроса в зависимости от переданных параметров
    #        (содержит логику вариантов форматирования).
    #             На входе:
    #                 - param_column_index_or_name: либо только индексы, либо имена колонок.
    #                 - table_name: мя таблицы к которой выполняется запрос.
    #             На выходе: готовый запрос.
    #     """
    #
    #     # Если были указаны параметры на конкретные колонки:
    #     if param_column_index_or_name:
    #
    #         #  Проверка первого элемента на тип данных (исключает дублирования проверок):
    #         if isinstance(param_column_index_or_name[0], str):  # check_first_itm_type
    #
    #             # Если вводим имя колонки (хотим получить комментарий для колонки по ее имени):
    #             if self._check_all_elements(str, param_column_index_or_name):
    #
    #                 # Форматирование sql_str с учетом параметров:
    #                 mutable_sql = self._sql_formatter(
    #                     sql=SQL_GET_COLUMN_COMMENTS_BY_NAME,
    #                     params=param_column_index_or_name
    #                 )
    #
    #             # Если не все элементы имеют один и тот же тип или недопустимые:
    #             else:
    #                 raise TypeError(
    #                     f'Переданные аргументы не соответствуют единому типу данных, '
    #                     f'должны быть либо только str (имена колонок),'
    #                     f' либо только int (индексы колонок).'
    #                 )
    #
    #         elif isinstance(param_column_index_or_name[0], int):
    #
    #             # Если вводим индекс колонки (хотим получить комментарий для колонок по индексам):
    #             if self._check_all_elements(int, param_column_index_or_name):
    #
    #                 # Форматирование sql_str с учетом параметров:
    #                 mutable_sql = self._sql_formatter(
    #                     sql=SQL_GET_COLUMN_COMMENTS_BY_INDEX,
    #                     # table_name=self.name_table, - устарело.
    #                     params=param_column_index_or_name
    #                 )
    #             # Если не все элементы имеют один и тот же тип или недопустимые:
    #             else:
    #                 raise TypeError(
    #                     f'Переданные аргументы не соответствуют единому типу данных, '
    #                     f'должны быть либо только str (имена колонок),'
    #                     f' либо только int (индексы колонок).'
    #                 )
    #
    #         # Если элементы не соответствуют допустимым типам данных:
    #         else:
    #             raise TypeError(
    #                 f'Переданные аргументы должны быть либо str (имена колонок), либо int (индексы колонок), '
    #                 f'другие типы данных недопустимы.'
    #             )
    #     else:
    #         # Если не вводим ни имя колонки, ни индекс (хотим получить все комментарии ко всем существующим колонкам):
    #         mutable_sql = self._sql_formatter(sql=SQL_GET_ALL_COLUMN_COMMENTS)  # По умолчанию!
    #
    #     return mutable_sql

    # def _create_comment(self, type_comment: str, comment: str, name_column: str = None) -> None:
    #     """
    #         Универсальный метод для создания комментариев к различным сущностям в базе данных.
    #     """
    #
    #
    #
    #
    #     if type_comment == 'COLUMN':
    #         if name_column:
    #
    #             _comment = self._validator(comment, str)
    #
    #
    #
    #             mutable_sql_variant = SQL_SAVE_COMMENT_COLUMN.format(
    #                 entity_type=self.PARAMS_SQL.get(type_comment),
    #                 schema=self.schema,  # Проверка на инъекции есть на верхнем уровне при инициализации:
    #                 name_entity=self.name_entity,  # Проверка на инъекции есть на верхнем уровне при инициализации:
    #                 name_column=self._stop_sql_injections(name_column),
    #
    #             )
    #         else:
    #             raise ValueError(f'Не передано значение для аргумента: name_column.')
    #
    #     # Если комментарий не для колонки, значит для любой другой сущности (таблица, представление, ...)
    #     else:
    #         mutable_sql_variant = SQL_SAVE_COMMENT.format(
    #             self.PARAMS_SQL.get(type_comment),
    #             self.schema,
    #             self.name_entity,
    #             self._validator(comment, str)
    #         )
    #
    #         # Добавление комментариев
    #         # with self.engine.connect() as conn:
    #         #     conn.execute(text(mutable_sql))
    #
    #
    #     self.recorder(mutable_sql_variant, comment)






    # def row_sql_recorder(self, sql: str) -> None:  #  убрать не нужен - дублирование функционала.
    #     """
    #         Метод для создания сырых запросов на запись данных.
    #     """
    #
    #     # Проверка на инъекции:
    #     _sql = self._stop_sql_injections(sql)
    #     self.recorder(_sql)
    #     return None


# def _generate_strparams_for_sql(self, params: tuple = None) -> str:
#     """
#         Приватный вложенный метод для генерации строки с последовательностью имен или индексов колонок.
#         Служит для подготовки передаваемых параметров в соответствии с синтаксисом sql требуемого вида для
#         дальнейшей передачи в запрос через multyparams (conn.execute(sql, multyparams).
#
#         На входе: параметры для подстановки в sql.
#         На выходе: str, последовательность имен или индексов колонок (через запятую в кавычках): columns_string
#
#     """
#
#     valid_params = self._validator(params, tuple)
#     return ', '.join(f"'{columns}'" for columns in valid_params)