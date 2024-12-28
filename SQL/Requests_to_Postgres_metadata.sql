

	
	
	-- ####################################################
	-- *** ПОЛУЧИТЬ ДОПОЛНИТЕЛЬНЫЕ СВЕДЕНИЯ О ПОДКЛЮЧЕНИИ. ***
--	Сведения, относящейся к текущей деятельности этого процессов:
SELECT 
		  w.datid --OID базы данных, к которой подключён этот серверный процесс
		 ,w.datname 	--name Имя базы данных, к которой подключён этот серверный процесс
		-- ,w.pid --integer 	Идентификатор процесса этого серверного процесса
		--,w.leader_pid --integer	Идентификатор ведущего процесса группы, если текущий процесс является исполнителем параллельного запроса.
				-- NULL, если этот процесс является ведущим или не задействован в параллельном запросе.
		 --*
		,w.usesysid --oid  OID пользователя, подключённого к этому серверному процессу
		,w.usename --name Имя пользователя, подключённого к этому серверному процессу
		,w.client_addr --inetIP-адрес клиента, подключённого к этому серверному процессу. 
						--Значение null в этом поле означает, что клиент подключён через сокет Unix на стороне сервера или что это внутренний процесс, например, автоочистка.
		,w.backend_start --timestamp with time zone	Время запуска процесса. Для процессов, обслуживающих клиентов, это время подключения клиента к серверу.	
		 --*
		,w.application_name --text Название приложения, подключённого к этому серверному процессу
		--,w.client_hostname --text Имя компьютера для подключённого клиента, получаемое в результате обратного поиска в DNS по client_addr.
							-- Это поле будет отлично от null только в случае соединений по IP и только при включённом режиме log_hostname.
		--,w.client_port --integer	Номер TCP-порта, который используется клиентом для соединения с этим обслуживающим процессом, или -1, если используется сокет Unix.
					   --  Если поле содержит NULL, это означает, что это внутренний серверный процесс.				
		,w.xact_start -- timestamp with time zoneВремя начала текущей транзакции в этом процессе или null при отсутствии активной транзакции.
					  -- Если текущий запрос был первым в своей транзакции, то значение в этом столбце совпадает со значением столбца query_start.		
		,w.query_start --timestamp with time zone	Время начала выполнения активного в данный момент запроса, или, если state не active, то время начала выполнения последнего запроса		
		,w.state_change --timestamp with time zone		Время последнего изменения состояния (поля state)		
		,w.wait_event_type --text	Тип события, которого ждёт обслуживающий процесс, если это ожидание имеет место; в противном случае — NULL. См. Таблицу 28.4.		
		,w.wait_event --text	Имя ожидаемого события, если обслуживающий процесс находится в состоянии ожидания, а в противном случае — NULL. См. также Таблица 28.5 – Таблица 28.13.		
		,w.state --text Общее текущее состояние этого серверного процесса. Возможные значения:		
				--		active: серверный процесс выполняет запрос.				--		
				--		idle: серверный процесс ожидает новой команды от клиента.				--		
				--		idle in transaction: серверный процесс находится внутри транзакции, но в настоящее время не выполняет никакой запрос.				
				--		idle in transaction (aborted): Это состояние подобно idle in transaction, за исключением того, что один из операторов в транзакции вызывал ошибку.			
				--		fastpath function call: серверный процесс выполняет fast-path функцию.		
				--		disabled: Это состояние отображается для серверных процессов, у которых параметр track_activities отключён.		
		--,w.backend_xid --xid	Идентификатор верхнего уровня транзакции этого серверного процесса, если такой идентификатор есть.
		--,w.backend_xmin --xid	Текущая граница xmin для серверного процесса.
		--,w.query_id --bigint	Идентификатор последнего запроса этого серверного процесса. 
					--		Если state имеет значение active, то в этом поле отображается идентификатор запроса, выполняемого в данный момент. 
					--		Если процесс находится в любом другом состоянии, то в этом поле отображается идентификатор последнего выполненного запроса. 
					--		По умолчанию идентификаторы запросов не вычисляются, поэтому данное поле будет иметь значение NULL,
					--		если не включён параметр compute_query_id или если не загружен сторонний модуль, вычисляющий идентификаторы запросов.
		,w.query --text Текст последнего запроса этого серверного процесса. 
				--		Если state имеет значение active, то в этом поле отображается запрос, который выполняется в настоящий момент. 
				--		Если процесс находится в любом другом состоянии, то в этом поле отображается последний выполненный запрос.
				--		По умолчанию текст запроса обрезается до 1024 байт; это число определяется параметром track_activity_query_size.
		,w.backend_type --text Тип текущего серверного процесса. Возможные варианты: 
--						autovacuum launcher, autovacuum worker, logical replication launcher, logical replication worker, parallel worker,
--						background writer, client backend, checkpointer, archiver, startup, walreceiver, walsender и walwriter.
--						Кроме того, фоновые рабочие процессы, регистрируемые расширениями, могут иметь дополнительные типы. 
FROM 
	pg_stat_activity as w -- получить дополнительные сведения о подключении.
	
	-- ####################################################
--	Сведения о статистике по операциям ввода/вывода для этой последовательности
SELECT
*
FROM
	pg_statio_all_sequences as q  -- получить дополнительные сведения о подключении.


---pg_stat_user_tables pg_statio_user_tables - одно и то же
	

	-- ####################################################
	
--	Сведения о комментариях !таблицы и колонки!  pg_description pg_shdescription (какие то системные сущности, например default template for new databases )
select
	s.objoid as o    --oid объекта, к которому относится это описание (ссылается на какой-либо столбец OID)
 	, s.classoid as c   -- (по простому: таблицы, представления, материализованные представления и тд.)   -- oid (ссылается на pg_class.oid) OID системного каталога, к которому относится этот объект
	, s.objsubid  as s  -- (по простому: если 0 - таблица, если номер - колонка) int4 Для комментария к столбцу таблицы это номер столбца (objoid и classoid указывают на саму таблицу). Для всех других типов объектов это поле содержит ноль.
	, s.description  as d  --(по простому: коммент либ к табл либ к колонке.)  text Произвольный текст, служащий описанием данного объекта
from  pg_description s 
	--pg_catalog.pg_statio_all_tables as st
	--pg_catalog.pg_description as w
	--information_schema.columns as rr
where  s.objsubid = 
	

-- #################################################### Комментарии все
select
	s.objoid as o    --oid объекта, к которому относится это описание (ссылается на какой-либо столбец OID)
 	, s.classoid as c   -- (по простому: таблицы, представления, материализованные представления и тд.)   -- oid (ссылается на pg_class.oid) OID системного каталога, к которому относится этот объект
	, s.objsubid  as s  -- (по простому: если 0 - таблица, если номер - колонка) int4 Для комментария к столбцу таблицы это номер столбца (objoid и classoid указывают на саму таблицу). Для всех других типов объектов это поле содержит ноль.
	, s.description  as d  --(по простому: коммент либ к табл либ к колонке.)  text Произвольный текст, служащий описанием данного объекта
from  
	pg_description s
--left join 
	
	
	
-- ####################################################
--Ищем все таблицы.	
select
	  tables.schemaname as schema--Имя схемы, содержащей таблицу ссылается на pg_namespace.nspname
	, tables.tablename as name
	, tables.tableowner as owner
from  
	pg_tables as tables
where 
	tables.tableowner != 'postgres' -- (postgres) Системные таблицы

--Ищем все представления
select
	  views.schemaname as schema  -- Имя схемы, содержащей представление 
	, views.viewname as name
	, views.viewowner as owner
from  
	pg_views as views
where 
	views.viewowner != 'postgres' -- (postgres) Системные 
--	
	--pg_user	
	
--Ищем все таблицы, мат. представления	
select
	  mviews.schemaname as schema  -- Имя схемы, содержащей представление 
	, mviews.matviewname as name
	, mviews.matviewowner as owner
	, mviews.ispopulated as ispopulated  
	, mviews.tablespace as tablespace  -- (ссылается на pg_tablespace.spcname) Имя табличного пространства, содержащего материализованное представление (NULL, если это пространство по умолчанию
from  
	pg_matviews as mviews
where 
	mviews.matviewowner != 'postgres' -- (postgres) Системные 	
	
	
-- ####################################################
-- Покажет комментарии к колонкам в таблицах (в том числе и с пропусками), возможно можно заполнять пропуски прямо из выборки, но эт не точно.
select
	c.table_schema,
	st.relname as TableName,
	c.column_name,
	pgd.description
from
	pg_catalog.pg_statio_all_tables as st
inner join information_schema.columns c
on
	c.table_schema = st.schemaname
	and 
	c.table_name = st.relname
left join pg_catalog.pg_description pgd --+
on
	pgd.objoid = st.relid
	and pgd.objsubid = c.ordinal_position
--where st.relname = 'YourTableName'

	
	
-- #################################################### Джоин с комментами
--Ищем все сущности (entity) такие как 	
-- r = обычная таблица (Relation), i = индекс (Index), S = последовательность (Sequence),
-- t = таблица TOAST, v = представление (View), m = материализованное представление (Materialized view),
--c = составной тип (Composite type), f = сторонняя таблица (Foreign table),
-- p = секционированная таблица (Partitioned table), I = секционированный индекс (partitioned Index)
select
	 -- all_entity.oid as oid --Идентификатор строки
		--all_entity.relname as name_entity!
	,	all_entity.relname as name_entity	-- Имя сущности
	--,	all_entity.relowner as owner_id
	,	all_entity.relnamespace as relnamespace
	,	all_entity.relnatts as count_column  -- Количество колонок в таблице.
	-- OID пространства имён, содержащего это отношение -> pg_namespace
	--all_entity.relkind as type_all_entity
from
	pg_class as all_entity
where
	all_entity.relkind in ('r', 'v', 'm')
	--and all_entity.relowner not in ('10') -- -- (10 = postgres = Системные сущности) не верно


	
	
	
-- #################################################### 
select  -- Все сущности (очищенные от системных)
	 --  all_entity.oid as oid	--Идентификатор строки
	--,	all_entity.relowner as owner_id
	--,	all_entity.relnamespace as relnamespace
		all_entity.relname as name_entity	-- Имя сущности
		-- #
		, comments.description as description
from
	pg_class as all_entity
inner join --Джоин с комментами
	-- Комментарии все
	(select
		s.objoid as oid    --oid объекта, к которому относится это описание (ссылается на какой-либо столбец OID)
	 	, s.classoid as c   -- (по простому: таблицы, представления, материализованные представления и тд.)   -- oid (ссылается на pg_class.oid) OID системного каталога, к которому относится этот объект
		, s.objsubid  as s  -- (по простому: если 0 - таблица, если номер - колонка) int4 Для комментария к столбцу таблицы это номер столбца (objoid и classoid указывают на саму таблицу). Для всех других типов объектов это поле содержит ноль.
		, s.description  as description  --(по простому: коммент либ к табл либ к колонке.)  text Произвольный текст, служащий описанием данного объекта
	from  
		pg_description s) as comments
on all_entity.oid = comments.oid
where --comments.description =
		all_entity.relkind in ('r', 'v', 'm')
	--and all_entity.relowner not in ('10') -- -- (10 = postgres = Системные сущности) не верно
	
	
-- #################################################### Только комментарии к таблицам / колонкам без пропусков / только пропуски	
SELECT 
    all_entity.oid AS entity_oid,               -- Идентификатор объекта (таблица, представление и т.д.)
    all_entity.relname AS name_entity,          -- Имя таблицы, представления или материала
    nsp.nspname AS schema_name,                 -- Имя схемы объекта
    comments.description AS description         -- Комментарий к таблице (NULL, если комментарий отсутствует)
FROM 
    pg_class AS all_entity
INNER JOIN 
    pg_namespace AS nsp                         -- Джоин для получения имени схемы
    ON all_entity.relnamespace = nsp.oid
LEFT JOIN 
    pg_description AS comments                  -- Джоин для комментариев
    ON all_entity.oid = comments.objoid
   	and comments.objsubid = 0 	--  Только комментарии к таблице: 
 	-- comments.objsubid > 0  -- Только комментарии к столбцам:
WHERE 
    all_entity.relkind IN ('r', 'v', 'm')       -- r = обычная таблица, v = представление, m = материальное представление
    AND nsp.nspname NOT IN ('pg_catalog', 'information_schema') -- Исключение системных схем
    AND comments.description is NOT NULL
ORDER BY 
      nsp.nspname                                -- Сортировка по схеме
    , all_entity.relname                         -- Сортировка по имени сущности	
    , comments.description asc 
    
    
    
    
    
   -- #################################################### Всего схем в бд (где хранятся все сущности кроме системных)
SELECT 
    count(distinct nsp.nspname) AS count_schem   -- Имя схемы объекта
FROM 
    pg_class AS all_entity
INNER JOIN 
    pg_namespace AS nsp                         -- Джоин для получения имени схемы
    ON all_entity.relnamespace = nsp.oid
WHERE 
    all_entity.relkind IN ('r', 'v', 'm')       -- r = обычная таблица, v = представление, m = материальное представление
    AND nsp.nspname NOT IN ('pg_catalog', 'information_schema') -- Исключение системных схем
 
    
   
     
   -- #################################################### Всего сущностей
SELECT 
	 count(all_entity.relname) AS count_entity  -- distinct убираем тк повторяются названия (возможно вью созданы по названию таблиц или другое) -- Имя таблицы, представления или материала
,	 count(distinct nsp.nspname) AS count_schem   -- Имя схемы объекта
FROM 
    pg_class AS all_entity
INNER JOIN 
    pg_namespace AS nsp                         -- Джоин для получения имени схемы
    ON all_entity.relnamespace = nsp.oid
WHERE 
    all_entity.relkind IN ('r', 'v', 'm')       -- r = обычная таблица, v = представление, m = материальное представление
    AND nsp.nspname NOT IN ('pg_catalog', 'information_schema') -- Исключение системных схем
    
    
    -- #################################################### Всего сущностей 	количество сущностей, процент соотношения 
SELECT 
	 count(all_entity.relname) AS count_entity  -- distinct убираем тк повторяются названия (возможно вью созданы по названию таблиц или другое) -- Имя таблицы, представления или материала
FROM 
    pg_class AS all_entity
INNER JOIN 
    pg_namespace AS nsp                         -- Джоин для получения имени схемы
    ON all_entity.relnamespace = nsp.oid
WHERE 
    all_entity.relkind IN ('r', 'v', 'm')       -- r = обычная таблица, v = представление, m = материальное представление
    AND nsp.nspname NOT IN ('pg_catalog', 'information_schema') -- Исключение системных схем   
    


-- ###################################  	+ Показцывает сколько таблиц имеют пропуски
  	WITH schema_stats AS (
    SELECT 
        nsp.nspname AS schema_name,                   -- Имя схемы
        COUNT(DISTINCT all_entity.oid) AS count_entities, -- Количество таблиц/представлений/материалов
        SUM(CASE 
                WHEN comments.description IS NULL THEN 1 -- Если комментарий отсутствует
                ELSE 0 
            END) AS missing_comments_count            -- Количество пропущенных комментариев
    FROM 
        pg_class AS all_entity
    INNER JOIN 
        pg_namespace AS nsp 
        ON all_entity.relnamespace = nsp.oid
    LEFT JOIN 
        pg_description AS comments
        ON all_entity.oid = comments.objoid 
        --AND comments.objsubid = 0                    -- Учитываем только комментарии к таблицам
    WHERE 
        all_entity.relkind IN ('r', 'v', 'm')        -- r = обычная таблица, v = представление, m = материальное представление
        AND nsp.nspname NOT IN ('pg_catalog', 'information_schema') -- Исключение системных схем
    GROUP BY 
        nsp.nspname
)
SELECT 
    schema_name,                                     -- Имя схемы
    count_entities,                                 -- Общее количество сущностей в схеме
    missing_comments_count,                         -- Количество сущностей без комментариев
    ROUND((missing_comments_count::NUMERIC / NULLIF(count_entities, 0)) * 100, 2) AS missing_percentage -- Процент пропущенных комментариев
FROM 
    schema_stats
ORDER BY 
    missing_percentage DESC                        -- Сортировка по убыванию процента пропущенных комментариев
    
    
 -- ####################################################   --Список обращений к сущностям в бд
SELECT 
	  stat_activity.datname as name_db  -- Имя базы данных
   ,  nsp.nspname AS schema_name                 -- Имя схемы объекта
   ,  all_entity.relname AS name_entity        -- Имя таблицы, представления или материала
   ,  CASE
       WHEN all_entity.relkind IN ('r') THEN 'таблица'  -- Если r = обычная таблица,
       WHEN all_entity.relkind IN ('v') THEN 'представление'  -- Если v = представление,
       WHEN all_entity.relkind IN ('m') THEN 'мат. представление'  -- Если m = материальное представление,
      END AS type_entity            -- Количество пропущенных комментариев
	,  all_entity.relnatts as count_column  -- Количество колонок в таблице.
	-- **
	,	stat_activity.state 
	,	stat_activity.usename 
	,	stat_activity.application_name 
	,	stat_activity.wait_event 
	,	stat_activity.state_change 
FROM 
    pg_class AS all_entity
INNER JOIN 
    pg_namespace AS nsp                         -- Джоин для получения имени схемы
    ON all_entity.relnamespace = nsp.oid   
left JOIN 
	(	
		SELECT 
				w.datid  as id  --OID базы данных, к которой подключён этот серверный процесс
		    ,   w.datname 	--name Имя базы данных, к которой подключён этот серверный процесс
			,	w.usesysid --oid  OID пользователя, подключённого к этому серверному процессу
			,	w.usename --name Имя пользователя, подключённого к этому серверному процессу
			--,	w.client_addr --inetIP-адрес клиента, подключённого к этому серверному процессу. 
							--Значение null в этом поле означает, что клиент подключён через сокет Unix на стороне сервера или что это внутренний процесс, например, автоочистка.
			--,	w.backend_start --timestamp with time zone	Время запуска процесса. Для процессов, обслуживающих клиентов, это время подключения клиента к серверу.	
			,	w.application_name --text Название приложения, подключённого к этому серверному процессу
			--,	w.client_hostname --text Имя компьютера для подключённого клиента, получаемое в результате обратного поиска в DNS по client_addr.
								-- Это поле будет отлично от null только в случае соединений по IP и только при включённом режиме log_hostname.
			,	w.state_change --timestamp with time zone		Время последнего изменения состояния (поля state)		
			--,	w.wait_event_type --text	Тип события, которого ждёт обслуживающий процесс, если это ожидание имеет место; в противном случае — NULL. См. Таблицу 28.4.		
			,	w.wait_event --text	Имя ожидаемого события, если обслуживающий процесс находится в состоянии ожидания, а в противном случае — NULL. См. также Таблица 28.5 – Таблица 28.13.		
			,	w.state --text Общее текущее состояние этого серверного процесса. Возможные значения:		
					--		active: серверный процесс выполняет запрос.				--		
					--		idle: серверный процесс ожидает новой команды от клиента.				--		
					--		idle in transaction: серверный процесс находится внутри транзакции, но в настоящее время не выполняет никакой запрос.				
					--		idle in transaction (aborted): Это состояние подобно idle in transaction, за исключением того, что один из операторов в транзакции вызывал ошибку.			
					--		fastpath function call: серверный процесс выполняет fast-path функцию.		
					--		disabled: Это состояние отображается для серверных процессов, у которых параметр track_activities отключён.		
			--,w.query -- text Текст последнего запроса этого серверного процесса. 
					--		Если state имеет значение active, то в этом поле отображается запрос, который выполняется в настоящий момент. 
					--		Если процесс находится в любом другом состоянии, то в этом поле отображается последний выполненный запрос.
					--		По умолчанию текст запроса обрезается до 1024 байт; это число определяется параметром track_activity_query_size.
			--,w.backend_type --text Тип текущего серверного процесса. Возможные варианты: 
			--						autovacuum launcher, autovacuum worker, logical replication launcher, logical replication worker, parallel worker,
			--						background writer, client backend, checkpointer, archiver, startup, walreceiver, walsender и walwriter.
			--						Кроме того, фоновые рабочие процессы, регистрируемые расширениями, могут иметь дополнительные типы. 
    	FROM 
			pg_stat_activity as w -- получить дополнительные сведения о подключении.
		WHERE 
          w.datname = current_database()  -- Соединяем только текущую базу данных
	 ) as stat_activity
on true  -- LEFT JOIN для всех записей, так как связь с pg_stat_activity косвенная   --- ON all_entity.relnamespace = stat_activity.id --(OID базы данных, )
WHERE 
    all_entity.relkind IN ('r', 'v', 'm')       -- r = обычная таблица, v = представление, m = материальное представление
    AND nsp.nspname NOT IN ('pg_catalog', 'information_schema') -- Исключение системных схем
ORDER BY 
      nsp.nspname ASC                               -- Сортировка по схеме

      
      
      

 -- ####################################################   Итог
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
    ON all_entity.relnamespace = nsp.oid   
WHERE 
    all_entity.relkind IN ('r', 'v', 'm')       -- r = обычная таблица, v = представление, m = материальное представление
    AND nsp.nspname NOT IN ('pg_catalog', 'information_schema') -- Исключение системных схем
ORDER BY 
      nsp.nspname ASC                               -- Сортировка по схеме
      
      
      
    -- ####################################################    
 select 
	  q.dattablespace 
	, q.dattablespace -- ссылается на pg_tablespace.oid
	, q.datname  -- Имя базы данных
	-- datdba oid (ссылается на pg_authid.oid)
FROM      
    pg_database as q 
    
    
    
    
      
  -- ####################################################   Справочник связей неуд попытка
WITH all_entity_data  as
( 
	select
		all_entity.oid as oid
   ,	nsp.nspname as schema_name		-- Имя схемы объекта
   ,	all_entity.relname as name_entity		-- Имя таблицы, представления или материала
   ,	case
			when all_entity.relkind in ('r') then 'таблица'			-- Если r = обычная таблица,
			when all_entity.relkind in ('v') then 'представление'			-- Если v = представление,
			when all_entity.relkind in ('m') then 'мат. представление'			-- Если m = материальное представление,
		end as type_entity		
	,	all_entity.relnatts as count_column		-- Количество колонок в таблице.
		--**,
		--,primary_key_in_table.name_column as primary_key
	from
		pg_class as all_entity
	inner join 
    	pg_namespace as nsp		-- Джоин для получения имени схемы
    on
		all_entity.relnamespace = nsp.oid
	where
		all_entity.relkind in ('r', 'v', 'm')		-- r = обычная таблица, v = представление, m = материальное представление
		and nsp.nspname not in ('pg_catalog', 'information_schema')		-- Исключение системных схем
	order by
		nsp.nspname asc 
)      
SELECT
	b.schema_name
	,	b.name_entity
	,	b.type_entity
	--,	b.count_column
	,	primary_key_in_table.name_column as "primary key"
FROM    
	all_entity_data as b
INNER JOIN 
		(
			select
				q.connamespace as space_id			--  oid (ссылается на pg_namespace.oid) OID пространства имён, содержащего это ограничение
		,		q.conrelid as table_id				-- вытаскиваем таблицу (ссылается на pg_class.oid) Таблица, для которой установлено это ограничение; ноль, если это не ограничение таблицы
		,		q.conname as name_column				-- Имя ограничения
				--,    q.confrelid as primary_id_in_table 	--  (ссылается на pg_class.oid)  Если это внешний ключ, таблица, на которую он ссылается; иначе ноль
		,		q.conrelid as for_table				-- Таблица, для которой установлено это ограничение; ноль, если это не ограничение таблицы
		,		case
					when q.contype in ('p') then 'primary key'				--  p = первичный ключ (primary key),
					when q.contype in ('f') then 'foreign key'					--  f = внешний ключ (foreign key)
				end as relation_types
				--, 	q.contype as relation_type
				--	,    q.conkey   --  (ссылается на pg_attribute.attnum) Для ограничений таблицы (включая внешние ключи, но не триггеры ограничений), определяет список столбцов, образующих ограничение 
			from
				pg_constraint as q
				--pg_tablespace  -- pg_attribute (анные о столбцах: имя тип и тд) -- pg_type(все о типах данны столбцов) -- pg_namespace
			where
				-- relation_types = 'primary key'
				q.contype = 'p'
		) as primary_key_in_table 
		on
			b.oid = primary_key_in_table.table_id


  -- ####################################################   Справочник связей 2		
WITH all_entity_data AS (
  SELECT
    all_entity.oid AS oid,
    nsp.nspname AS schema_name,        -- Имя схемы объекта
    all_entity.relname AS name_entity, -- Имя таблицы
    CASE
      WHEN all_entity.relkind IN ('r') THEN 'таблица'         -- r = обычная таблица
      WHEN all_entity.relkind IN ('v') THEN 'представление'   -- v = представление
      WHEN all_entity.relkind IN ('m') THEN 'мат. представление' -- m = материальное представление
    END AS type_entity,
    all_entity.relnatts AS count_column -- Количество колонок в таблице
  FROM
    pg_class AS all_entity
  INNER JOIN 
    pg_namespace AS nsp -- Джоин для получения имени схемы
  ON
    all_entity.relnamespace = nsp.oid
  WHERE
    all_entity.relkind IN ('r', 'v', 'm') -- r = обычная таблица, v = представление, m = материальное представление
    AND nsp.nspname NOT IN ('pg_catalog', 'information_schema') -- Исключение системных схем
  ORDER BY
    nsp.nspname ASC
),      
primary_key_columns AS (
  SELECT
    con.conrelid AS table_id,          -- Таблица, для которой установлено ограничение
    att.attname AS name_column         -- Имя колонки, входящей в первичный ключ
  FROM
    pg_constraint AS con
  INNER JOIN 
    pg_attribute AS att
  ON
    att.attnum = ANY(con.conkey)       -- Соответствие столбцов из conkey
    AND att.attrelid = con.conrelid    -- Таблица столбца
  WHERE
    con.contype = 'p'                  -- Только первичные ключи
),
foreign_key_columns AS (
  SELECT
    con.conrelid AS table_id,          -- Таблица, в которой определён внешний ключ
    att.attname AS name_column,        -- Имя колонки, являющейся внешним ключом
    confrelid AS ref_table_id,         -- Таблица, на которую ссылается внешний ключ
    ref_att.attname AS ref_column_name -- Имя столбца, на который ссылается внешний ключ
  FROM
    pg_constraint AS con
  INNER JOIN 
    pg_attribute AS att
  ON
    att.attnum = ANY(con.conkey)       -- Соответствие столбцов из conkey
    AND att.attrelid = con.conrelid    -- Таблица столбца
  INNER JOIN 
    pg_attribute AS ref_att
  ON
    ref_att.attnum = ANY(con.confkey)  -- Соответствие внешнего ключа к столбцу
    AND ref_att.attrelid = con.confrelid -- Таблица столбца-источника
  WHERE
    con.contype = 'f'                  -- Только внешние ключи
)
SELECT
  b.schema_name,
  b.name_entity,
  b.type_entity,
  pk.name_column AS "primary key",     -- Имя столбца первичного ключа
  fk.name_column AS "foreign key",     -- Имя столбца внешнего ключа
  fk_ref.name_entity AS ref_table,     -- Таблица, на которую ссылается внешний ключ
  fk.ref_column_name AS ref_column     -- Имя столбца, на который ссылается внешний ключ
FROM    
  all_entity_data AS b
LEFT JOIN 
  primary_key_columns AS pk
ON
  b.oid = pk.table_id
LEFT JOIN 
  foreign_key_columns AS fk
ON
  b.oid = fk.table_id
LEFT JOIN 
  all_entity_data AS fk_ref
ON
  fk.ref_table_id = fk_ref.oid
ORDER BY
  b.schema_name, b.name_entity		
			
			
  
   -- ####################################################  -- Количество таблиц
select
	count(tables.tablename) as count_table
from  
	pg_tables as tables
where 
	tables.tableowner != 'postgres' -- (postgres) Системные таблицы
	

	
-- ####################################################  -- Количество представление
select
	 count(views.viewname) as count_view
from  
	pg_views as views
where 
	views.viewowner != 'postgres' -- (postgres) Системные 	
	
	
-- ####################################################  -- Количество материальное представление
select	
	 count(mviews.matviewname) as count_matvie
from  
	pg_matviews as mviews
where 
	mviews.matviewowner != 'postgres' -- (postgres) Системные 			

	
	
--	, tables.tableowner as owner
	

-- ####################################################  -- Попытка сбора исторических даных о таблицах		
	--pg_stat_database  со статистикой на уровне базы
	--pg_stat_user_tables -со статистикой по обращениям к этой таблице
select	
	 db.oid  as oid  
,	 db.datname as db_name
from  	
	pg_database as db  
	

--	Сведения со статистикой по обращениям к этой таблице:
SELECT 
		-- Маркеры. Показатели использования таблиц в базе данных.
	 	tables.relid as oid -- oid таблицы		
	, 	tables.schemaname   -- Имя схемы
	, 	tables.relname    	-- Имя таблицы
	, 	tables.seq_scan as count_reads_table 	-- Количество последовательных чтений, произведённых в этой таблице  (количество обращений к данным, актуальность таблицы) (показатель, количества чтения таблицы при открытии в бд таблицы +1 ы) !
--	,	tables.seq_tup_read as "count_read_rows!"  -- Количество «живых» строк, прочитанных при последовательных чтениях  (при обращении к таблице сколько строк прочитанно)
--
	-- не бьются данные (не соотносятся, расные суммы)
	,	tables.n_tup_ins as "Количество вставленных строк"  -- Количество вставленных строк (показатель статичности таблицы или динамичного ее использования) !
--	,	tables.n_tup_upd as "Количество изменённых строк"  -- Количество изменённых строк (показатель статичности таблицы или динамичного ее использования) ! редко используется
--	,	tables.n_tup_del as "Количество удалённых строк"  -- Количество удалённых строк (показатель статичности таблицы или динамичного ее использования) !
--	,	tables.n_mod_since_analyze as "Всего манипуляций со стороками"  -- Всего количество изменений для таблицы (Оценочное число строк, изменённых в этой таблице с момента последнего сбора статистики (показатель статичности таблицы или динамичного ее использования) !
--
	--,	tables.n_live_tup as "?"   -- Оценочное количество «живых» строк  ! не понятно показывает 0
	--,	tables.n_dead_tup as f  -- Оценочное количество «мёртвых» строк (статистика для очистки таблицы)
	--,	tables.last_autoanalyze -- Время последнего выполнения сбора статистики для этой таблицы фоновым процессом автоочистки
FROM
	pg_stat_user_tables as tables
--LIMIT 5  всего 165	
	
	
-- Соединяем 
	
SELECT	
		tables.relid as oid -- oid таблицы		
	,	tables.schemaname   -- Имя схемы
	, 	tables.relname    	-- Имя таблицы
	, 	tables.seq_scan as count_reads_table 	-- Количество последовательных чтений, произведённых в этой таблице
	,	tables.n_tup_ins as "Количество вставленных строк" 
	--
	, all_entity.*
FROM
		pg_stat_user_tables as tables
INNER JOIN 
  --(select	tablespace.oid as oid  , tablespace.spcname  as spcname  from  pg_tablespace as tablespace) as tablespace 
	(select
	  all_entity.oid as oid --Идентификатор строки
	,	all_entity.relname as name_entity	-- Имя сущности
	,	all_entity.relowner as owner_id
	,	all_entity.relnamespace as relnamespace
	,	all_entity.relnatts as count_column  -- Количество колонок в таблице.
		-- OID пространства имён, содержащего это отношение -> pg_namespace
		--all_entity.relkind as type_all_entity
	from
		pg_class as all_entity) as all_entity
ON 	tables.relid = all_entity.oid
INNER JOIN 
	(select	db.dattablespace  as dattablespace , db.datname as db_name from  pg_database as db ) as db_list
ON 	all_entity.relnamespace = db_list.dattablespace
	

select	
	  db.dattablespace as dattablespace 
	, db.datname as db_name
	, namespace.*
from  pg_database as db 
INNER JOIN 
	(select
		  namespace.nspname as nspname
		, namespace.oid   
	 from pg_namespace as namespace 
	 ) as namespace
on db.dattablespace = namespace.nspname
