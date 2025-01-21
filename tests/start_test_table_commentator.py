from tcommenter.tcommenter import Tcommenter
from tests.connnections.connection import ENGINE_MART_SV

comments = Tcommenter(engine=ENGINE_MART_SV, name_table='dags_analyzer', schema='audit')
# a = comments.get_table_comments()
# dict_comments = comments.get_column_comments('tags')
# dict_comments = comments.get_all_comments()
# print(dict_comments)


# d = {'columns': {tc_0_2_0v_dev: ''}}  # {'columns': {'tags': 'Тест удался3.'}}
# # d = {'columns': {'tags': 'Список тегов, связанных с DAG (список строк) для быстрого поиска DAG.'}}
# comments.save_comments(d)
# dict_comments = comments.get_column_comments()
# # dict_comments = comments.get_column_comments('tags')
# print(dict_comments)

# Выявленные ошибки:   d = {'columns': {tc_0_2_0v_dev: ''}}, в " {tc_0_2_0v_dev:" - в запросе ошибка так как не существует колонки такой.
# Необходимо добавить обработку  (надо ли?) валидацию на интеджер для имени колонки наверное.
# возможно добавить это в следующий выпуск
# создать тесты в сл выпуске

# Список всех методов:
for r in Tcommenter.__dict__:
    print(r)