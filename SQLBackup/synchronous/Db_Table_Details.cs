using Dapper;
using Microsoft.Data.SqlClient;
using SQLBackup.Model;
using System.Collections.Generic;
using System.Data;

namespace SQLBackup.synchronous
{
    public class Db_Table_Details
    {
        private readonly IDbConnection _dbConnection;

        public Db_Table_Details(IDbConnection dbConnection)
        {
            _dbConnection = dbConnection;
            int tablesrno = 0;
            var AllDBTable = from l in GetTableNames() group l by l.DatabaseName into egrp select new { Dbname=egrp.Key,Tabname=egrp.ToList(),Toataltable=egrp.Count() };

            string filePath = @"D:\Learn dotnet\SQLBackup\SQLBackup\Output\MasterDetails.txt";
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }
            File.AppendAllText(filePath, "Total Database - " + AllDBTable.Count().ToString() + "\n\n");
            foreach(var db in AllDBTable)
            {
                File.AppendAllText(filePath, "Database Name - " + db.Dbname + "  | Total Table - " + db.Toataltable + "\n");
                tablesrno = 0;
                foreach (var item in db.Tabname)
                {
                    tablesrno++;
                    File.AppendAllText(filePath, tablesrno.ToString()+"."+item.TableName + "\n");

                    // Console.WriteLine("File created successfully at: " + filePath);
                }
                File.AppendAllText(filePath, "---------------------------------------------------------------------------\n");
            }
            foreach (var db in AllDBTable)
            {
                string filePath2 = @"D:\Learn dotnet\SQLBackup\SQLBackup\Output\"+db.Dbname+".txt";
                if (File.Exists(filePath2))
                {
                    File.Delete(filePath2);
                }
                string connectionString = "Server=.;Initial Catalog="+db.Dbname+";User Id=sa;Password=456789;TrustServerCertificate=True;";
                foreach (var item in db.Tabname)
                {
                    using (var connection = new SqlConnection(connectionString))
                    {
                        connection.Open();
                        string qry = "DECLARE @TableName NVARCHAR(128) = '" + item.TableName + @"';
DECLARE @SchemaName NVARCHAR(128);
DECLARE @ObjectID INT = OBJECT_ID(@TableName);
DECLARE @CreateScript NVARCHAR(MAX) = '';
DECLARE @CRLF CHAR(2) = CHAR(13) + CHAR(10);

-- Get schema name
SELECT @SchemaName = s.name
FROM sys.objects o
JOIN sys.schemas s ON o.schema_id = s.schema_id
WHERE o.object_id = @ObjectID;

-- Start CREATE TABLE
SET @CreateScript = 'CREATE TABLE [' + @SchemaName + '].[' + @TableName + '] (' + @CRLF;

-- Columns
SELECT @CreateScript += 
    '  [' + c.name + '] ' + 
    t.name + 
    CASE 
        WHEN t.name IN ('varchar', 'char', 'nvarchar', 'nchar') THEN '(' + 
            CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR) END + ')'
        WHEN t.name IN ('decimal', 'numeric') THEN '(' + CAST(c.precision AS VARCHAR) + ',' + CAST(c.scale AS VARCHAR) + ')'
        ELSE ''
    END + 
    CASE WHEN c.is_nullable = 0 THEN ' NOT NULL' ELSE ' NULL' END +
    CASE WHEN c.is_identity = 1 THEN ' IDENTITY(' + CAST(ic.seed_value AS VARCHAR) + ',' + CAST(ic.increment_value AS VARCHAR) + ')' ELSE '' END +
    ',' + @CRLF
FROM sys.columns c
JOIN sys.types t ON c.user_type_id = t.user_type_id
LEFT JOIN sys.identity_columns ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE c.object_id = @ObjectID
ORDER BY c.column_id;

-- Remove trailing comma
SET @CreateScript = LEFT(@CreateScript, LEN(@CreateScript) - LEN(',' + @CRLF)) + @CRLF + ')' + @CRLF;

-- Primary Key
SELECT @CreateScript += 
    'ALTER TABLE [' + @SchemaName + '].[' + @TableName + '] ADD CONSTRAINT [' + i.name + '] PRIMARY KEY (' + 
    STRING_AGG('[' + c.name + ']', ', ') + ');' + @CRLF
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.is_primary_key = 1 AND i.object_id = @ObjectID GROUP BY i.name, i.is_unique;;

-- Foreign Keys
SELECT @CreateScript += 
    'ALTER TABLE [' + @SchemaName + '].[' + @TableName + '] ADD CONSTRAINT [' + fk.name + '] FOREIGN KEY (' + 
    COL_NAME(fc.parent_object_id, fc.parent_column_id) + ') REFERENCES [' + 
    OBJECT_SCHEMA_NAME(fk.referenced_object_id) + '].[' + OBJECT_NAME(fk.referenced_object_id) + '](' + 
    COL_NAME(fc.referenced_object_id, fc.referenced_column_id) + ');' + @CRLF
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fc ON fk.object_id = fc.constraint_object_id
WHERE fk.parent_object_id = @ObjectID;

-- Default Constraints
SELECT @CreateScript += 
    'ALTER TABLE [' + @SchemaName + '].[' + @TableName + '] ADD CONSTRAINT [' + dc.name + '] DEFAULT ' + dc.definition + 
    ' FOR [' + c.name + '];' + @CRLF
FROM sys.default_constraints dc
JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
WHERE dc.parent_object_id = @ObjectID;

-- Check Constraints
SELECT @CreateScript += 
    'ALTER TABLE [' + @SchemaName + '].[' + @TableName + '] ADD CONSTRAINT [' + cc.name + '] CHECK ' + cc.definition + ';' + @CRLF
FROM sys.check_constraints cc
WHERE cc.parent_object_id = @ObjectID;

-- Non-PK Indexes
SELECT @CreateScript += 
    'CREATE ' + 
    CASE WHEN i.is_unique = 1 THEN 'UNIQUE ' ELSE '' END +
    'INDEX [' + i.name + '] ON [' + @SchemaName + '].[' + @TableName + '] (' + 
    STRING_AGG('[' + c.name + ']', ', ') + ');' + @CRLF
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.is_primary_key = 0 AND i.is_unique_constraint = 0 AND i.object_id = @ObjectID
GROUP BY i.name, i.is_unique;

-- Output the full script
select @CreateScript;
";
                        List<string> users = connection.Query<string>(qry).ToList();
                      
                        File.AppendAllText(filePath2, users[0] + "\n");
                        File.AppendAllText(filePath2, "---------------------------------------------------------------------------\n");
                      //  Console.WriteLine(string.Join("\n", users));
                    }
                }

            }

        }

        public List<DBTable> GetTableNames()
        {
            return _dbConnection.Query<DBTable>(@"DECLARE @sql NVARCHAR(MAX) = '';
DECLARE @dbName NVARCHAR(128);

-- Cursor to loop through databases
DECLARE db_cursor CURSOR FOR
SELECT name FROM sys.databases
WHERE state_desc = 'ONLINE' AND name NOT IN ('master', 'model', 'msdb', 'tempdb');

OPEN db_cursor
FETCH NEXT FROM db_cursor INTO @dbName

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql += '
    SELECT 
        ''' + @dbName + ''' AS DatabaseName,
       -- s.name AS SchemaName,
        s.name+''.''+t.name AS TableName
    FROM [' + @dbName + '].sys.tables t
    JOIN [' + @dbName + '].sys.schemas s ON t.schema_id = s.schema_id
    UNION ALL
    '

    FETCH NEXT FROM db_cursor INTO @dbName
END

CLOSE db_cursor
DEALLOCATE db_cursor

-- Remove trailing UNION ALL
SET @sql = LEFT(@sql, LEN(@sql) - LEN('UNION ALL' + CHAR(13) + CHAR(10)))

-- Execute the final query
EXEC sp_executesql @sql;
").ToList();
        }
    }
}
