declare @databaseName varchar(100)
declare @folderPath varchar(100)='C:\Data\'
set @databaseName = DB_NAME()


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
						or DATA_TYPE = 'nvarchar'
						then @databaseName + '.dbo.GetString(' + COLUMN_NAME + ')'
					when DATA_TYPE = 'binary'
						or DATA_TYPE = 'varbinary'
						or DATA_TYPE = 'image'
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

select * into  tempQueriesCopy from tempQueries
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

	select @sql = 'bcp "' + @query2 + '" queryout "' + @folderPath + replace( @table,'"','') + '.csv" -c -t~ -T -S' + @@servername

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