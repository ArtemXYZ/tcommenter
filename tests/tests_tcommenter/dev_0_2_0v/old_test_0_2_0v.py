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


# def test__get_sql_and_params_list_only_from_indexes_or_names(create_mocked_engine):
#     test_ex = get_instance_test_class(create_mocked_engine)
#
#     # # 1
#     # with pytest.raises(TypeError,
#     #                    match="Переданные аргументы не соответствуют единому типу данных, должны быть либо только str (имена колонок), либо только int (индексы колонок)."
#     #
#     #                    ):
#     #     assert test_ex._get_strparams_only_from_indexes_or_names_for_sql(('erwer', 1))
#
#     # 2
#     with pytest.raises(TypeError, match="Недопустимый тип данных для аргумента:"):
#         assert test_ex._get_sql_and_params_list_only_from_indexes_or_names('')
#
#     # 3
#     assert test_ex._get_sql_and_params_list_only_from_indexes_or_names(('erwer', 'wewe'))
#
#     # 4
#     assert test_ex._get_sql_and_params_list_only_from_indexes_or_names((1, 2))
#
#     # 5
#     with pytest.raises(TypeError, match="Недопустимый тип данных для аргумента:"):
#         assert test_ex._get_sql_and_params_list_only_from_indexes_or_names(None)
#
#     #  6
#     with pytest.raises(TypeError, match="Недопустимый тип данных для аргумента:"):
#         assert test_ex._get_sql_and_params_list_only_from_indexes_or_names([])