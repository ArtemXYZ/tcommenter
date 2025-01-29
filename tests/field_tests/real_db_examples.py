"""
    Модуль тестов с реальной базой данных.
"""

from tcommenter.tcommenter import Tcommenter

# Import Engine SQLAlchemy for db:
from tests.connnections.connection import ENGINE_REAL_DB

# ----------------------------------------------------------------------------------------------------------------------
# Создание экземпляра класса для работы с определенной сущностью в базе данных:
commenter = Tcommenter(engine=ENGINE_REAL_DB, name_table='dags_analyzer', schema='audit')

# ------------------------------- Методы извлечения метаданных:

# Получение комментария к таблице (только к самой сущности, исключая комментарии к колонкам):
comments = commenter.get_table_comments()
print(comments)  # -> 'Таблица содержит выгрузку данных из Airflow.'

# Получение комментариев ко всем колонкам сущности:
comments = commenter.get_column_comments()
print(comments)  # -> {'dag_id': 'pass', 'description': 'pass', 'tags': 'pass', pass}

# Получение комментария к колонке по имени колонки:
comments = commenter.get_column_comments('tags')
print(comments)  # -> {'tags': 'pass'}'

# Получение комментариев к колонкам по индексу (порядковому номеру в сущности):
comments = commenter.get_column_comments(1, 2)
print(comments)  # -> {'dag_id': 'pass', 'description': 'pass'}

# Получение всех имеющихся комментариев к сущности и ее колонкам:
comments = commenter.get_all_comments()
print(comments)  # -> '{'table': 'pass', 'columns': {'dag_id': 'pass', 'description': 'pass', pass}}'


# ------------------------------- Методы записи метаданных:
# Запись комментария к сущности:
commenter.set_table_comment('Таблица содержит выгрузку данных из Airflow.')
comments = commenter.get_table_comments()
print(comments)  # -> 'Таблица содержит выгрузку данных из Airflow.'

# Аналогично для методов:
# * set_view_comment()
# * set_materialized_view_comment()

# Запись комментариев к сущности по тегу колонок:
commenter.set_column_comment(description='description_test', dag_id='dag_id_test')
comments = commenter.get_column_comments('description', 'dag_id')
print(comments)  # -> {'dag_id': 'dag_id_test', 'description': 'description_test'}



# ------------------------------- Сервисные методы:
# Метод определения типа сущности ('table', 'view', 'mview', ...)
type_entity = commenter.get_type_entity()
print(type_entity)  # -> 'table'


# ------------------------------- Примеры перегрузки метаданных:

# Получение комментариев специального вида совместимого с методом "save_comments()".
# Если необходимо выполнить перегрузку всех имеющихся комментариев (сначала получить, а после вашей промежуточной
# логики) сразу же сохранить в ту же или другую сущность (с одинаковой структурой), существует метод "save_comments()".

# Универсальный метод сохранения комментариев любого типа (к сущностям или их колонкам):
# commenter.save_comments(comments)

# Он принимает специальный вид данных, позволяющий явно указывать на принадлежность комментариев от всех методов
# получения комментариев: "get_table_comments()", "get_column_comments()", "get_all_comments()".
# Однако, для первых двух для этого необходимо выставить флаг: "service_mode=True" (по умолчанию service_mode=False).
# В "get_all_comments()" "service_mode" отсутствует, но выходные данные соответствуют данному флагу.
# Универсальный метод "save_comments()" позволяет сохранить сразу все метаданные и для колонок и для сущностей,
# ограничившись всего одной строчкой кода.


# # Получаем комментарии в "service_mode" режиме перед перегрузкой:
comments = commenter.get_table_comments(service_mode=True)
print(comments)  # -> {'table': 'Таблица содержит выгрузку данных из Airflow.'}
commenter.save_comments(comments)

# Получаем комментарии в "service_mode" режиме перед перегрузкой:
comments = commenter.get_column_comments(2, 3, service_mode=True)
print(comments)  # -> {'columns': {'description': 'pass', 'tags': 'pass'}}
commenter.save_comments(comments)

# Получаем все имеющиеся комментарии:
comments = commenter.get_all_comments()
print(comments)  # -> {'table': 'pass', 'columns': {pass}}
commenter.save_comments(comments)


# ----------------------------------
# Список всех методов:
# for r in Tcommenter.__dict__:
#     print(r)
#





