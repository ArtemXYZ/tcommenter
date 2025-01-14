"""
    Модуль содержит SQL-запросы к таблицам со статистикой в соответствии с синтаксисом одноименной СУБД.
"""


# COMMENT ON TABLE "schema"."table" IS 'comment'  для любой сущности (таблица, представление, ...)
SQL_SAVE_COMMENT = """COMMENT ON {entity_type} "{schema}"."{name_entity}" IS ':comment'"""


# COMMENT ON COLUMN "schema"."name_entity"."name_column" IS 'comment' # Для колонок в любой сущности: # COLUMN
SQL_SAVE_COMMENT_COLUMN = """COMMENT ON {entity_type} "{schema}"."{name_entity}"."{name_column}" IS ':comment'"""

SQL_GET_TABLE_COMMENTS = """
    SELECT
        comments.description AS description
    FROM
        pg_class AS all_entity
    INNER JOIN
        pg_description AS comments
    ON 
        all_entity.oid = comments.objoid 
    AND 
        comments.objsubid = 0 
    AND 
        all_entity.relname = '{table_name}'
"""

SQL_GET_ALL_COLUMN_COMMENTS = """
    SELECT
        cols.attname AS column_name, 
        comments.description AS description
    FROM
        pg_class AS all_entity
    INNER JOIN
        pg_description AS comments
    ON 
        all_entity.oid = comments.objoid 
    AND 
        comments.objsubid > 0 
    AND 
        all_entity.relname = '{table_name}'
    INNER JOIN 
        pg_attribute AS cols                    
    ON
        cols.attnum = comments.objsubid 
    AND
        cols.attrelid = all_entity.oid      
"""

SQL_GET_COLUMN_COMMENTS_BY_INDEX = """
    SELECT 
        cols.attname AS column_name,       
        comments.description AS description         
    FROM 
        pg_class AS all_entity
    INNER JOIN 
        pg_description AS comments              
    ON 
        all_entity.oid = comments.objoid
    AND
        comments.objsubid > 0
    AND
        all_entity.relname = '{table_name}'
    INNER JOIN 
        pg_attribute AS cols                  
    ON
        cols.attnum = comments.objsubid 
    AND
        cols.attrelid = all_entity.oid      
    WHERE 
         comments.objsubid IN (:columns)
"""

SQL_GET_COLUMN_COMMENTS_BY_NAME = """
    SELECT 
        cols.attname AS column_name, 
        comments.description AS description    
    FROM 
        pg_class AS all_entity
    INNER JOIN 
        pg_description AS comments             
    ON
        all_entity.oid = comments.objoid 
    AND 
        comments.objsubid > 0              
    AND
        all_entity.relname = '{table_name}'  
    INNER JOIN 
        pg_attribute AS cols                    
    ON
        cols.attnum = comments.objsubid 
    AND
        cols.attrelid = all_entity.oid
    WHERE
         cols.attname IN (':columns')
"""

SQL_CHECK_TYPE_ENTITY = """
    SELECT
        case
                when all_entity.relkind in ('r') then 'table'			
                when all_entity.relkind in ('v') then 'view'			
                when all_entity.relkind in ('m') then 'mview'
                when all_entity.relkind in ('m') then 'index'
                when all_entity.relkind in ('m') then 'sequence'
                when all_entity.relkind in ('m') then 'toast'
                when all_entity.relkind in ('m') then 'composite_type'
                when all_entity.relkind in ('m') then 'foreign_table'
                when all_entity.relkind in ('m') then 'partitioned_table'
                when all_entity.relkind in ('m') then 'partitioned_index'    			
        end as type_entity		
    FROM
    pg_class as all_entity
    WHERE		
    all_entity.relname = '{table_name}'	
"""



# ---------------------- old

# COMMENT ON TABLE "schema"."table" IS 'comment' для любой сущности (таблица, представление, ...)
# SQL_SAVE_COMMENT = """COMMENT ON {0} "{1}"."{2}" IS '{3}'"""  # TABLE


# COMMENT ON COLUMN "schema"."table"."name_table" IS 'comment' # Для колонок в любой сущности:
# SQL_SAVE_COMMENT_COLUMN = """COMMENT ON {0} "{1}"."{2}"."{3}" IS '{4}'"""  # COLUMN