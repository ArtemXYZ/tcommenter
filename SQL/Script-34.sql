-- ###################################  	+ Показцывает сколько таблиц имеют пропуски
SELECT 
     nsp.nspname AS schema_name                 -- Имя схемы объекта
   ,  all_entity.relname AS name_entity        -- Имя таблицы, представления или материала
   ,  CASE
       WHEN all_entity.relkind IN ('r') THEN 'таблица'  -- Если r = обычная таблица,
       WHEN all_entity.relkind IN ('v') THEN 'представление'  -- Если v = представление,
       WHEN all_entity.relkind IN ('m') THEN 'мат. представление'  -- Если m = материальное представление,
      END AS type_entity            -- Количество пропущенных комментариев
	,  all_entity.relnatts as count_column  -- Количество колонок в таблице.
FROM 
    pg_class AS all_entity
INNER JOIN 
    pg_namespace AS nsp                         -- Джоин для получения имени схемы
    ON all_entity.relnamespace = nsp.oid		--- ( гринплане нет этого поля если вызывать через  *, если на прямую то, есть)  
WHERE 
    all_entity.relkind IN ('r', 'v', 'm')       -- r = обычная таблица, v = представление, m = материальное представление
    AND nsp.nspname = 'mart_sv'  -- только эти схемы
ORDER BY 
      nsp.nspname ASC   

      
      
-- Количество таблиц в GP ('mart_sv')      
select
	count(tables.tablename) as count_table
	--tables.tablename
	--tables.tableowner
from  
	pg_tables as tables
where 
	tables.tableowner != 'postgres' -- (postgres) Системные таблицы
	and tables.schemaname = 'mart_sv' 

	
-- ####################################################  -- Количество представлений в GP ('mart_sv')  
select
	 count(views.viewname) as count_view
from  
	pg_views as views
where 
	views.viewowner != 'postgres' -- (postgres) Системные 
	and views.schemaname = 'mart_sv' 
	
	
	
-- ####################################################  -- Количество материальных представлений в GP ('mart_sv')
select	
	 count(mviews.matviewname) as count_matvie
from  
	pg_matviews as mviews
where 
	mviews.matviewowner != 'postgres' -- (postgres) Системные 		
	and mviews.schemaname = 'mart_sv' 
	
	
	
-- ####################################################  -- -- Нет комментариев (в таблицах и столбцах) в GP ('mart_sv')	gpt
SELECT 
    all_entity.relname AS name_entity,          -- Имя таблицы, представления или материала
    nsp.nspname AS schema_name,                 -- Имя схемы объекта
    pg_roles.rolname AS owner,                  -- Владелец объекта
    comments.description AS description         -- Комментарий к таблице (NULL, если комментарий отсутствует)
FROM 
    pg_class AS all_entity
INNER JOIN 
    pg_namespace AS nsp                         -- Джоин для получения имени схемы
    ON all_entity.relnamespace = nsp.oid
LEFT JOIN 
    pg_description AS comments                  -- Джоин для комментариев
    ON all_entity.oid = comments.objoid
LEFT JOIN
    pg_roles                                    -- Джоин для получения владельца
    ON all_entity.relowner = pg_roles.oid
WHERE 
    all_entity.relkind IN ('r', 'v', 'm')       -- r = обычная таблица, v = представление, m = материальное представление
    AND nsp.nspname = 'mart_sv' -- 
    AND comments.description IS NULL            -- Только записи без комментариев
ORDER BY 
    nsp.nspname,                                -- Сортировка по схеме
    all_entity.relname,                         -- Сортировка по имени сущности  
    comments.description ASC	
    
    
    
-- ####################################################  -- Нет комментариев (в таблицах и столбцах) в GP ('mart_sv')	я
SELECT 
     all_entity.relname AS name_entity          -- Имя таблицы, представления или материала
,    nsp.nspname AS schema_name                 -- Имя схемы объекта
,    comments.description AS description         -- Комментарий к таблице (NULL, если комментарий отсутствует)
,    pg_roles.rolname AS owner                  -- Владелец объекта
FROM 
    pg_class AS all_entity
INNER JOIN 
    pg_namespace AS nsp                         -- Джоин для получения имени схемы
    ON all_entity.relnamespace = nsp.oid
LEFT JOIN 
    pg_description AS comments                  -- Джоин для комментариев
    ON all_entity.oid = comments.objoid
   	-- and comments.objsubid = 0 	--  Только комментарии к таблице: 
 	-- and comments.objsubid > 0  -- Только комментарии к столбцам:
LEFT JOIN
    pg_roles                                    -- Джоин для получения владельца
    ON all_entity.relowner = pg_roles.oid    
WHERE 
    all_entity.relkind IN ('r', 'v', 'm')       -- r = обычная таблица, v = представление, m = материальное представление
--    AND nsp.nspname NOT IN ('pg_catalog', 'information_schema') -- Исключение системных схем
    	AND nsp.nspname = 'mart_sv'
    AND comments.description is NULL
ORDER BY 
      nsp.nspname                                -- Сортировка по схеме
    , all_entity.relname                         -- Сортировка по имени сущности	
    , comments.description asc     