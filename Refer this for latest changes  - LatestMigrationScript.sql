--Step 1:
create function [dbo].[GetString]
(
@value as nvarchar(max)
)
returns nvarchar(max)
as
begin
return case when @value is null then 'null' when @value = '' then '""' else '"'+ replace(@value,'"','""') + '"' end
end


--Step 2:
-- SQL varbinary to bytea postgresql
-- master.dbo.fn_varbintohexstr
-- encode(decode('68656C6C6F','hex'),'escape')
--Schema script
declare @databaseName varchar(100)

set @databaseName = DB_NAME()

declare @TableToInclude table (TableName varchar(200))


select 'create schema ' + name + '; ALTER SCHEMA public OWNER TO postgres;'
from sys.schemas
where name not like 'db[_]%'
	and name <> 'sys'
	and name <> 'INFORMATION_SCHEMA'

--Table script
select cast('create table ' + (
			lower(case TABLE_SCHEMA
				when 'dbo'
					then 'public'
				else TABLE_SCHEMA
				end
			 + '.' + table_name)) + char(13) + '(' + STUFF(
			(
				select lower(', ' + char(13) + (
						case column_name
							when 'order'
								then '"order"'
							when 'default'
								then '"default"'
							when 'offset'
								then '"offset"'
							else column_name
							end
						) + (
						case 
							when DATA_TYPE in (
									'nvarchar'
									,'varchar'
									,'char'
									,'nchar'
									)
								and CHARACTER_MAXIMUM_LENGTH <> - 1
								and CHARACTER_MAXIMUM_LENGTH < 8000
								then ' varchar(' + cast(isnull(CHARACTER_MAXIMUM_LENGTH + 100, 8000) as varchar(10)) + ')'
							when DATA_TYPE in (
									'nvarchar'
									,'text'
									,'varchar'
									,'char'
									,'nchar'
									,'ntext'
									)
								or CHARACTER_MAXIMUM_LENGTH = - 1
								then ' text'
							when DATA_TYPE = 'decimal'
								then ' Numeric(' + cast(NUMERIC_PRECISION as varchar(2)) + ',' + cast(numeric_scale as varchar(2)) + ')'
							when DATA_TYPE = 'bit'
								then ' Boolean' + iif(COLUMN_default = '((0))', ' default false', iif(COLUMN_default = '((1))', ' default true', ''))
							when DATA_TYPE = 'tinyint'
								then ' smallint' + iif(COLUMN_default is not null, ' default ' + replace(replace(COLUMN_default, '((', ''), '))', ''), '')
							when DATA_TYPE = 'date'
								then ' date'
							when DATA_TYPE = 'datetime'
								or DATA_TYPE = 'datetime2'
								or DATA_TYPE = 'datetimeoffset'
								then ' timestamptz'
							when DATA_TYPE = 'timestamp'
								then ' bytea'
							when DATA_TYPE = 'uniqueidentifier'
								then ' UUID' 
							when DATA_TYPE in (
									'money'
									,'smallmoney'
									)
								then ' Numeric(8,2)'
							when DATA_TYPE in (
									'binary'
									,'varbinary'
									,'image'
									)
								then ' bytea'
							else ' ' + case COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity')
									when 1
										then 'serial'
									else DATA_TYPE
									end
							end + iif(IS_Nullable = 'NO', ' not null', ' null') + iif(COLUMN_default = '(newid())', ' default uuid_generate_v1()', ''))
						)
				from INFORMATION_SCHEMA.COLUMNS
				where (
						table_name = Results.table_name
						and TABLE_SCHEMA = Results.TABLE_SCHEMA
						)
				for xml PATH('')
					,TYPE
				).value('(./text())[1]', 'VARCHAR(MAX)'), 1, 2, '') + char(13) + ');'+ char(13) + char(13) as xml)
from INFORMATION_SCHEMA.COLUMNS Results
where OBJECT_ID(TABLE_SCHEMA + '.' + table_name) not in (
		select object_id
		from sys.views
		)
	and TABLE_SCHEMA + '.' + table_name in (
		select tablename
		from @TableToInclude
		)
	or (
		select count(1)
		from @TableToInclude
		) = 0
group by Results.TABLE_SCHEMA
	,table_name
for xml PATH('')


--Step 3:
declare @databaseName varchar(100)
declare @folderPath varchar(100)='C:\GPESDB\CRDB\'
set @databaseName = DB_NAME()

declare @TableToInclude Table
(
TableName varchar(200)
)

--Export query prepration
select ROW_NUMBER() over (
		order by (
				select 1
				)
		) rownum
	,'select * FROM ' + @databaseName + '.' + '[' + SCHEMA_NAME(schema_id) + '].[' + t.name + ']' as col1
	,'select ' + STUFF((
			select ',' + case 
					when DATA_TYPE = 'datetime'
						or DATA_TYPE = 'datetime2'
						then ' Isnull(nullif(convert(nvarchar(28),' + COLUMN_NAME + + ' ,121) ,' + char(39) + char(39) + '), ' + char(39) + 'null' + char(39) + ')'
					when DATA_TYPE = 'varchar'
						or DATA_TYPE = 'nvarchar' then @databaseName + '.dbo.GetString(' + COLUMN_NAME + ')'
						when  DATA_TYPE = 'xml'
						then @databaseName + '.dbo.GetString(' + ' Isnull(nullif(cast(' + COLUMN_NAME + + ' as nvarchar(max)) ,' + char(39) + char(39) + '), ' + char(39) + 'null' + char(39) + ')' + ')'
					when DATA_TYPE = 'binary'
						or DATA_TYPE = 'varbinary'
						or DATA_TYPE = 'image'
						or  DATA_TYPE = 'timestamp'
						then 'substring(master.dbo.fn_varbintohexstr(' + COLUMN_NAME + '), 3, len(master.dbo.fn_varbintohexstr(' + COLUMN_NAME + ')))'
					else ' Isnull(nullif(cast(' + COLUMN_NAME + + ' as nvarchar(max)) ,' + char(39) + char(39) + '), ' + char(39) + 'null' + char(39) + ')'
					end
			from INFORMATION_SCHEMA.COLUMNS
			where TABLE_NAME = t.name
				and TABLE_SCHEMA = schema_name(schema_id)
			order by table_schema
				,table_name
				,ordinal_position
			for xml PATH('')
			), 1, 1, '') + ' FROM ' + @databaseName + '.' + '[' + SCHEMA_NAME(schema_id) + '].[' + t.name + ']' as col2
	,SCHEMA_NAME(schema_id) + '."' + t.name + '"' as col3
into tempQueries
from sys.tables t
where schema_name(schema_id)+'.'+name in(select tablename from @TableToInclude)
or (select count(1) from @TableToInclude)=0

select * into  tempQueriesCopy from tempQueries

--drop table tempQueriesCopy
--select * from tempQueries
--Export with bcp script
declare @query1 varchar(MAX)
declare @query2 varchar(MAX)
declare @table varchar(MAX)
declare @row int = 0

while exists (
		select top 1 rownum
		from tempQueries
		)
begin
	select top 1 @query1 = col2
		,@query2 = col2
		,@row = rownum
		,@table = col3
	from tempQueries

	declare @sql varchar(8000) = ''

	select @sql = 'bcp "' + @query2 + '" queryout "' + @folderPath + replace( @table,'"','') + '.csv" -c -t~ -T -w -S' + @@servername

	print @sql

	exec master..xp_cmdshell @sql

	delete
	from tempQueries
	where rownum = @row
end


---- To allow advanced options to be changed.  
--EXEC sp_configure 'show advanced options', 1;  
--GO  
---- To update the currently configured value for advanced options.  
--RECONFIGURE;  
--GO  
---- To enable the feature.  
--EXEC sp_configure 'xp_cmdshell', 1;  
--GO  
---- To update the currently configured value for this feature.  
--RECONFIGURE;  
--GO  



--Step 4:
declare @folderPath varchar(100)='C:\GPESDB\CRDB\'
--Copy script
select 'Copy ' + replace(col3, '"', '') + char(13) + 'From ' + char(39) + @folderPath + replace(col3, '"', '') + '.csv' + char(39) + ' DELIMITER ' + char(39) + '~' + char(39) + ' null as ' + char(39) + 'null' + char(39) + '  encoding ' + char(39) + 'windows-1251' + char(39) + ' CSV;' + char(13) + 'select 1;' + char(13)
from tempQueriesCopy
where col3 not in (
		select TABLE_SCHEMA + '.' + char(34) + TABLE_NAME + char(34)
		from INFORMATION_SCHEMA.COLUMNS
		where DATA_TYPE = 'varbinary'
		)


drop table tempQueries
drop table tempQueriesCopy

--Step 5:
--PostgreSQL Server Query to get table and row counts
--Ref. Link https://www.periscopedata.com/blog/exact-row-counts-for-every-database-table

--Function
create or replace function 
count_rows(schema text, tablename text) returns integer
as
$body$
declare
  result integer;
  query varchar;
begin
  query := 'SELECT count(1) FROM ' || schema || '.' || tablename;
  execute query into result;
  return result;
end;
$body$
language plpgsql;


--Query
select 
  table_schema,
  table_name, 
  count_rows(table_schema, table_name)
from information_schema.tables
where 
  table_schema not in ('pg_catalog', 'information_schema') 
  and table_type='BASE TABLE'
order by 3 desc


--SQL Server Query to get table and row counts
--Ref. Link https://blog.sqlauthority.com/2017/05/24/sql-server-find-row-count-every-table-database-efficiently/

SELECT SCHEMA_NAME(schema_id) AS [SchemaName],
[Tables].name AS [TableName],
SUM([Partitions].[rows]) AS [TotalRowCount]
FROM sys.tables AS [Tables]
JOIN sys.partitions AS [Partitions]
ON [Tables].[object_id] = [Partitions].[object_id]
AND [Partitions].index_id IN ( 0, 1 )
-- WHERE [Tables].name = N'name of the table'
GROUP BY SCHEMA_NAME(schema_id), [Tables].name;

--Step 6:
--Ref. link: https://www.mssqltips.com/sqlservertip/3443/script-all-primary-keys-unique-constraints-and-foreign-keys-in-a-sql-server-database-using-tsql/
--Export all PK and unique constraints
declare @SchemaName varchar(100)
declare @TableName varchar(256)
declare @IndexName varchar(256)
declare @ColumnName varchar(100)
declare @is_unique_constraint varchar(100)
declare @IndexTypeDesc varchar(100)
declare @FileGroupName varchar(100)
declare @is_disabled varchar(100)
declare @IndexOptions varchar(max)
declare @IndexColumnId int
declare @IsDescendingKey int 
declare @IsIncludedColumn int
declare @TSQLScripCreationIndex varchar(max)
declare @TSQLScripDisableIndex varchar(max)
declare @is_primary_key varchar(100)
declare @TableToInclude Table
(
TableName varchar(200)
)


declare CursorIndex cursor for
 select schema_name(t.schema_id) [schema_name], t.name, ix.name,
 case when ix.is_unique_constraint = 1 then ' UNIQUE ' else '' END 
    ,case when ix.is_primary_key = 1 then ' PRIMARY KEY ' else '' END 
 , ix.type_desc,
  case when ix.is_padded=1 then 'PAD_INDEX = ON, ' else 'PAD_INDEX = OFF, ' end
 + case when ix.allow_page_locks=1 then 'ALLOW_PAGE_LOCKS = ON, ' else 'ALLOW_PAGE_LOCKS = OFF, ' end
 + case when ix.allow_row_locks=1 then  'ALLOW_ROW_LOCKS = ON, ' else 'ALLOW_ROW_LOCKS = OFF, ' end
 + case when INDEXPROPERTY(t.object_id, ix.name, 'IsStatistics') = 1 then 'STATISTICS_NORECOMPUTE = ON, ' else 'STATISTICS_NORECOMPUTE = OFF, ' end
 + case when ix.ignore_dup_key=1 then 'IGNORE_DUP_KEY = ON, ' else 'IGNORE_DUP_KEY = OFF, ' end
 + 'SORT_IN_TEMPDB = OFF, FILLFACTOR =' + CAST(ix.fill_factor AS VARCHAR(3)) AS IndexOptions
 , FILEGROUP_NAME(ix.data_space_id) FileGroupName
 from sys.tables t 
 inner join sys.indexes ix on t.object_id=ix.object_id
 where ix.type>0 and  (ix.is_primary_key=1 or ix.is_unique_constraint=1) --and schema_name(tb.schema_id)= @SchemaName and tb.name=@TableName
 and t.is_ms_shipped=0 and t.name<>'sysdiagrams'and
 ( schema_name(t.schema_id)+'.'+t.name in(select tablename from @TableToInclude)
or (select count(1) from @TableToInclude)=0)  
 order by schema_name(t.schema_id), t.name, ix.name
open CursorIndex
fetch next from CursorIndex into  @SchemaName, @TableName, @IndexName, @is_unique_constraint, @is_primary_key, @IndexTypeDesc, @IndexOptions, @FileGroupName
while (@@fetch_status=0)
begin
 declare @IndexColumns varchar(max)
 declare @IncludedColumns varchar(max)
 set @IndexColumns=''
 set @IncludedColumns=''
 declare CursorIndexColumn cursor for 
 select col.name, ixc.is_descending_key, ixc.is_included_column
 from sys.tables tb 
 inner join sys.indexes ix on tb.object_id=ix.object_id
 inner join sys.index_columns ixc on ix.object_id=ixc.object_id and ix.index_id= ixc.index_id
 inner join sys.columns col on ixc.object_id =col.object_id  and ixc.column_id=col.column_id
 where ix.type>0 and (ix.is_primary_key=1 or ix.is_unique_constraint=1)
 and schema_name(tb.schema_id)=@SchemaName and tb.name=@TableName and ix.name=@IndexName
 order by ixc.index_column_id
 open CursorIndexColumn 
 fetch next from CursorIndexColumn into  @ColumnName, @IsDescendingKey, @IsIncludedColumn
 while (@@fetch_status=0)
 begin
  if @IsIncludedColumn=0 
    set @IndexColumns=@IndexColumns + 	(case @ColumnName when 'order' then '"order"' when 'Default' then '"Default"'
				when 'offset' then '"offset"'
				 else @ColumnName  end)   +','  --+ case when @IsDescendingKey=1  then ' DESC, ' else  ' ASC, ' end
  else 
   set @IncludedColumns=@IncludedColumns  + (case @ColumnName when 'order' then '"order"' when 'Default' then '"Default"'
				when 'offset' then '"offset"'
				 else @ColumnName  end)    +', ' 
     
  fetch next from CursorIndexColumn into @ColumnName, @IsDescendingKey, @IsIncludedColumn
 end
 close CursorIndexColumn
 deallocate CursorIndexColumn
 set @IndexColumns = substring(@IndexColumns, 1, len(@IndexColumns)-1)
 set @IncludedColumns = case when len(@IncludedColumns) >0 then substring(@IncludedColumns, 1, len(@IncludedColumns)-1) else '' end
--  print @IndexColumns
--  print @IncludedColumns

set @TSQLScripCreationIndex =''
set @TSQLScripDisableIndex =''
set  @TSQLScripCreationIndex='ALTER TABLE '+  @SchemaName +'.'+ @TableName + ' ADD CONSTRAINT ' + replace(replace(replace(replace(@IndexName,'Registration','reg'),'Patient','pat'),'Additional','add'),'Organisation','') + @is_unique_constraint + @is_primary_key +  +  '('+@IndexColumns+') '+ 
 case when len(@IncludedColumns)>0 then CHAR(13) +'INCLUDE (' + @IncludedColumns+ ')' else '' end + '; ' + char(13) + ' select 1;' -- + CHAR(13)+'WITH (' + @IndexOptions+ ') ON ' + QUOTENAME(@FileGroupName) + ';'  

print @TSQLScripCreationIndex
print @TSQLScripDisableIndex

fetch next from CursorIndex into  @SchemaName, @TableName, @IndexName, @is_unique_constraint, @is_primary_key, @IndexTypeDesc, @IndexOptions, @FileGroupName

end
close CursorIndex
deallocate CursorIndex



--Step 7:
--Ref. link: https://www.mssqltips.com/sqlservertip/3441/script-out-all-sql-server-indexes-in-a-database-using-tsql/
--Exporting all indexes
declare @SchemaName varchar(100)declare @TableName varchar(256)
declare @IndexName varchar(256)
declare @ColumnName varchar(100)
declare @is_unique varchar(100)
declare @IndexTypeDesc varchar(100)
declare @FileGroupName varchar(100)
declare @is_disabled varchar(100)
declare @IndexOptions varchar(max)
declare @IndexColumnId int
declare @IsDescendingKey int 
declare @IsIncludedColumn int
declare @TSQLScripCreationIndex varchar(max)
declare @TSQLScripDisableIndex varchar(max)
declare @filter_definition varchar(max)

declare @BoolColumns table
(
colValue varchar(200),
colReplaceValue varchar(200)
)
insert into @BoolColumns
select distinct COLUMN_NAME+'=(0)',COLUMN_NAME+'=false' from INFORMATION_SCHEMA.COLUMNS where DATA_TYPE='bit'
union
select distinct COLUMN_NAME+'=(1)',COLUMN_NAME+'=true' from INFORMATION_SCHEMA.COLUMNS where DATA_TYPE='bit'

declare @TableToInclude Table
(
TableName varchar(200)
)


declare CursorIndex cursor for
 select schema_name(t.schema_id) [schema_name], t.name, ix.name,
 case when ix.is_unique = 1 then 'UNIQUE ' else '' END 
 , ix.type_desc,
 case when ix.is_padded=1 then 'PAD_INDEX = ON, ' else 'PAD_INDEX = OFF, ' end
 + case when ix.allow_page_locks=1 then 'ALLOW_PAGE_LOCKS = ON, ' else 'ALLOW_PAGE_LOCKS = OFF, ' end
 + case when ix.allow_row_locks=1 then  'ALLOW_ROW_LOCKS = ON, ' else 'ALLOW_ROW_LOCKS = OFF, ' end
 + case when INDEXPROPERTY(t.object_id, ix.name, 'IsStatistics') = 1 then 'STATISTICS_NORECOMPUTE = ON, ' else 'STATISTICS_NORECOMPUTE = OFF, ' end
 + case when ix.ignore_dup_key=1 then 'IGNORE_DUP_KEY = ON, ' else 'IGNORE_DUP_KEY = OFF, ' end
 + 'SORT_IN_TEMPDB = OFF, FILLFACTOR =' + CAST(ix.fill_factor AS VARCHAR(3)) AS IndexOptions
 , ix.is_disabled , FILEGROUP_NAME(ix.data_space_id) FileGroupName,
 filter_definition
 from sys.tables t 
 inner join sys.indexes ix on t.object_id=ix.object_id
 where ix.type>0 and ix.is_primary_key=0 and ix.is_unique_constraint=0 --and schema_name(tb.schema_id)= @SchemaName and tb.name=@TableName
 and t.is_ms_shipped=0 and t.name<>'sysdiagrams'
 and
 ( schema_name(t.schema_id)+'.'+t.name in(select tablename from @TableToInclude)
or (select count(1) from @TableToInclude)=0)  
 order by schema_name(t.schema_id), t.name, ix.name

open CursorIndex
fetch next from CursorIndex into  @SchemaName, @TableName, @IndexName, @is_unique, @IndexTypeDesc, @IndexOptions,@is_disabled, @FileGroupName,@filter_definition

while (@@fetch_status=0)
begin
 declare @IndexColumns varchar(max)
 declare @IncludedColumns varchar(max)
 declare @DType varchar(max)
 set @IndexColumns=''
 set @IncludedColumns=''
 
 declare CursorIndexColumn cursor for 
  select col.name, ixc.is_descending_key, ixc.is_included_column
  from sys.tables tb 
  inner join sys.indexes ix on tb.object_id=ix.object_id
  inner join sys.index_columns ixc on ix.object_id=ixc.object_id and ix.index_id= ixc.index_id
  inner join sys.columns col on ixc.object_id =col.object_id  and ixc.column_id=col.column_id
  inner JOIN sys.types AS ty ON ty.user_type_id=col.user_type_id
  where ix.type>0 and (ix.is_primary_key=0 or ix.is_unique_constraint=0)
  and schema_name(tb.schema_id)=@SchemaName and tb.name=@TableName and ix.name=@IndexName
  order by ixc.index_column_id
 
 open CursorIndexColumn 
 fetch next from CursorIndexColumn into  @ColumnName, @IsDescendingKey, @IsIncludedColumn
 
 while (@@fetch_status=0)
 begin
  if @IsIncludedColumn=0 
   set @IndexColumns=@IndexColumns + @ColumnName  + case when @IsDescendingKey=1  then ' DESC, ' else  ' ASC, ' end
  else 
   set @IncludedColumns=@IncludedColumns  + @ColumnName  +', ' 

  fetch next from CursorIndexColumn into @ColumnName, @IsDescendingKey, @IsIncludedColumn
 end

 close CursorIndexColumn
 deallocate CursorIndexColumn

 set @IndexColumns = substring(@IndexColumns, 1, len(@IndexColumns)-1)
 set @IncludedColumns = case when len(@IncludedColumns) >0 then substring(@IncludedColumns, 1, len(@IncludedColumns)-1) else '' end

 --set @filter_definition = replace(replace(replace(replace(replace(@filter_definition, '[', ''), ']', ''), '(1)', 'true'), '(0)', 'false'), '=', ' = ')
 set @filter_definition = replace(replace(@filter_definition, '[', ''), ']', '')
 select @filter_definition = replace(@filter_definition,colValue,colReplaceValue)  from @BoolColumns

 set @TSQLScripCreationIndex =''
 set @TSQLScripDisableIndex =''
 set @TSQLScripCreationIndex='CREATE '+ @is_unique  + ' INDEX idx_'  + replace(replace(replace(replace(@IndexName,'Registration','reg'),'Patient','pat'),'Additional','add'),'Organisation','org') +' ON ' + @SchemaName +'.'+ @TableName + '('+@IndexColumns+') '+ 
  case when len(@IncludedColumns)>0 then CHAR(13) +'INCLUDE (' + @IncludedColumns+ ')' else '' end + case  when @filter_definition is null then '' else ' where ' +  @filter_definition end  + ';' + char(13) + 'select 1;' + CHAR(13) --+'WITH (' + @IndexOptions+ ') ON ' + QUOTENAME(@FileGroupName) + ';'  

 print @TSQLScripCreationIndex
 print @TSQLScripDisableIndex

 fetch next from CursorIndex into  @SchemaName, @TableName, @IndexName, @is_unique, @IndexTypeDesc, @IndexOptions,@is_disabled, @FileGroupName, @filter_definition

end
close CursorIndex
deallocate CursorIndex


--Step 8:
--Ref. link: https://www.mssqltips.com/sqlservertip/3443/script-all-primary-keys-unique-constraints-and-foreign-keys-in-a-sql-server-database-using-tsql/
--Export all foreign keys
declare @ForeignKeyID int
declare @ForeignKeyName varchar(4000)
declare @ParentTableName varchar(4000)
declare @ParentColumn varchar(4000)
declare @ReferencedTable varchar(4000)
declare @ReferencedColumn varchar(4000)
declare @StrParentColumn varchar(max)
declare @StrReferencedColumn varchar(max)
declare @ParentTableSchema varchar(4000)
declare @ReferencedTableSchema varchar(4000)
declare @TSQLCreationFK varchar(max)

declare @TableToInclude Table
(
TableName varchar(200)
)



--Written by Percy Reyes www.percyreyes.com
declare CursorFK cursor for select object_id--, name, object_name( parent_object_id) 
from sys.foreign_keys
where
( schema_name(schema_id)+'.'+ object_name( parent_object_id) in(select tablename from @TableToInclude)
or (select count(1) from @TableToInclude)=0)  
open CursorFK
fetch next from CursorFK into @ForeignKeyID
while (@@FETCH_STATUS=0)
begin
 set @StrParentColumn=''
 set @StrReferencedColumn=''
 declare CursorFKDetails cursor for
  select  fk.name ForeignKeyName, schema_name(t1.schema_id) ParentTableSchema,
  object_name(fkc.parent_object_id) ParentTable, c1.name ParentColumn,schema_name(t2.schema_id) ReferencedTableSchema,
   object_name(fkc.referenced_object_id) ReferencedTable,c2.name ReferencedColumn
  from --sys.tables t inner join 
  sys.foreign_keys fk 
  inner join sys.foreign_key_columns fkc on fk.object_id=fkc.constraint_object_id
  inner join sys.columns c1 on c1.object_id=fkc.parent_object_id and c1.column_id=fkc.parent_column_id 
  inner join sys.columns c2 on c2.object_id=fkc.referenced_object_id and c2.column_id=fkc.referenced_column_id 
  inner join sys.tables t1 on t1.object_id=fkc.parent_object_id 
  inner join sys.tables t2 on t2.object_id=fkc.referenced_object_id 
  where fk.object_id=@ForeignKeyID
 
 open CursorFKDetails
 fetch next from CursorFKDetails into  @ForeignKeyName, @ParentTableSchema, @ParentTableName, @ParentColumn, @ReferencedTableSchema, @ReferencedTable, @ReferencedColumn
 while (@@FETCH_STATUS=0)
 begin    
  set @StrParentColumn=@StrParentColumn + ', ' + @ParentColumn
  set @StrReferencedColumn=@StrReferencedColumn + ', ' + @ReferencedColumn
  
     fetch next from CursorFKDetails into  @ForeignKeyName, @ParentTableSchema, @ParentTableName, @ParentColumn, @ReferencedTableSchema, @ReferencedTable, @ReferencedColumn
 end
 close CursorFKDetails
 deallocate CursorFKDetails
 --print @StrParentColumn
 --print @StrReferencedColumn
 set @StrParentColumn=substring(@StrParentColumn,2,len(@StrParentColumn))
 set @StrReferencedColumn=substring(@StrReferencedColumn,2,len(@StrReferencedColumn))
 set @TSQLCreationFK='ALTER TABLE '+ case @ParentTableSchema when 'dbo' then 'public' else @ParentTableSchema end +'.'+ @ParentTableName +'  ADD CONSTRAINT '+  replace(replace(replace(replace(@ForeignKeyName,'Registration','reg'),'Patient','pat'),'Additional','add'),'Organisation','org')  
 + ' FOREIGN KEY('+ltrim(@StrParentColumn)+') '+ char(13) +'REFERENCES '+ case @ReferencedTableSchema when 'dbo' then 'public' else @ReferencedTableSchema end +'.'+ @ReferencedTable+' ('+ltrim(@StrReferencedColumn)+');' + char(13)+'select 1;'
 
 if not exists
 (select * from @TableToInclude where tablename = @ParentTableSchema + '.' + @ParentTableName )
 or 
 not exists
 (select * from @TableToInclude where tablename = @ReferencedTableSchema + '.' + @ReferencedTable )
	print @TSQLCreationFK

fetch next from CursorFK into @ForeignKeyID 
end
close CursorFK
deallocate CursorFK
