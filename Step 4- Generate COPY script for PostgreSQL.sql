declare @folderPath varchar(100)='C:\Data\'
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