
import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy import text
from sqlalchemy.engine.base import Engine
from tcommenter.tcommenter import Tcommenter
from tcommenter.sql.postgre_sql import *


# ----------------------------------------- Фикстуры и Mocks (функциями):
# Создаем mock для engine: +
@pytest.fixture(scope="class")  # scope=class
def _mocked_engine():
    """
        Фикстура для создания заглушки Engine SQLAlchemy.
    """
    return MagicMock(spec=Engine)


# ----------------------------------------- Инструменты (функциями):
# Функция управления логикой вызова pytest.raises():
def pytest_raises_router(func, exception, match, result, *args, **kwargs):  # mocked_engine,
    """
        Функция управления логикой вызова pytest.raises().
    """

    if exception:
        with pytest.raises(exception, match=match):
            func(*args, **kwargs)  # , **kwargs
    else:
        assert func(*args, **kwargs) == result  #

    return None





# Функция для настройки поведения execute -
def execute_mock(mocked_engine, result):
    """
        Настраивает результат метода execute для mocked_engine.

        :param mocked_engine: Заглушка SQLAlchemy Engine.
        :param result: Результат, который вернет fetchall() после вызова execute.
    """

    # Получаем подключение через mocked_engine
    mocked_connection = mocked_engine.connect.return_value.__enter__.return_value

    # Настраиваем execute -> fetchall() -> result
    mocked_connection.execute.return_value.fetchall.return_value = result
    return mocked_connection




# ----------------------------------------- Тестовый экземпляр класса:
def get_instance_test_class(mocked_engine) -> Tcommenter:
    """
        Возвращает экземпляр Tcommenter. Используется для всех тестов.
    """
    return Tcommenter(engine=mocked_engine, name_table="dags_analyzer", schema="audit")