"""
    Модуль содержит вспомогательные функции для тестирования.
"""

import pytest


class BaseToolsTests:
    """
        Набор методов для сокращения кода тестовых функций и упрощения управления тестовыми данными.
    """

    def __init__(self):
        pass

    # todo - следующая переделка (обновление), позволит убрать mocked_connection как параметр из методов, однако,
    #  инициализация в классе тестовом будет с явной передачей mocked_connection.
    # def __init__(self, mocked_connection: MagicMock):
    #     self.mocked_connection = mocked_connection

    @staticmethod
    def raises_router(func, exception, match, result, *args, **kwargs):  # mocked_engine,
        """
            Функция управления логикой вызова pytest.raises().
        """

        if exception:
            with pytest.raises(exception, match=match):
                func(*args, **kwargs)  # , **kwargs
        else:
            assert func(*args, **kwargs) == result  #

        return None

    @staticmethod
    def setter_execute_mock_values(mocked_connection, result):
        """
            Настраивает результат метода execute для mocked_engine.

            :param mocked_connection: Заглушка подключения.
            :param result: Результат, который вернет fetchall() после вызова execute.
        """

        # Настраиваем execute -> fetchall() -> result
        mocked_connection.execute.return_value.fetchall.return_value = result

        return mocked_connection

    @staticmethod
    def check_sql(mocked_connection, sql_test, params):
        """
            Проверка, что execute вызван с корректным SQL.
        """

        # Проверяем, что execute был вызван с правильными аргументами
        mocked_connection.execute.assert_called_once_with(text(sql_test), params)

    # -------------------------
    # @staticmethod
    # def setter_execute_mock_values(mocked_engine, exception=None):
    #     """
    #         Настраивает поведение метода execute для mocked_engine.
    #
    #         :param mocked_engine: Заглушка SQLAlchemy Engine.
    #         :param exception: Исключение, которое будет вызвано, если передано.
    #     """
    #     mocked_connection = mocked_engine.connect.return_value.__enter__.return_value
    #
    #     if exception:
    #         mocked_connection.execute.side_effect = exception
    #     else:
    #         mocked_connection.execute.return_value = None
