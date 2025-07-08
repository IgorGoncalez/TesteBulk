using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.IdentityModel.Tokens;
using System.Data;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using WebApi.Data;

namespace WebApi.Extensions
{
    public static class EfDbContextBulkExtension
    {
        public static void BulkInsert<TDbContext, TEntity>(this TDbContext dbContext, IEnumerable<TEntity> entities, int batchSize) where TDbContext : DbContext
        {
            var tempTableName = CreateTempTableName();
            var insertCommand = dbContext.GenerateInsertCommand<TDbContext, TEntity>(tempTableName, batchSize);
            dbContext.ExecuteTempTable(tempTableName, entities, false, true, insertCommand, batchSize, true);
        }

        public static void BulkUpdate<TDbContext, TEntity>(this TDbContext dbContext, IEnumerable<TEntity> entities, int batchSize, IList<string>? columnsToUpdate = null) where TDbContext : DbContext
        {
            var tempTableName = CreateTempTableName();
            var updateCommand = dbContext.GenerateUpdateCommand<TDbContext, TEntity>(tempTableName, batchSize, columnsToUpdate);
            dbContext.ExecuteTempTable(tempTableName, entities, true, true, updateCommand, batchSize, false);
        }

        private static readonly string _rowIndexColumnName = "RowIndex";

        private static void ExecuteTempTable<TDbContext, TEntity>(this TDbContext dbContext, string tableName, IEnumerable<TEntity> entities, bool addIdentityPrimaryKeys, bool createRowIndexColumn, string commandToExecute, int batchSize, bool updatePrimaryKeys) where TDbContext : DbContext
        {
            if (string.IsNullOrEmpty(tableName)) return;

            try
            {
                dbContext.ExecuteCommand(dbContext.GenerateCreateTableCommand<TDbContext, TEntity>(tableName, addIdentityPrimaryKeys, createRowIndexColumn));
                dbContext.BulkInsert(tableName, entities, addIdentityPrimaryKeys, createRowIndexColumn, batchSize);

                var primaryKeys = dbContext
                    .GetColumnsDefinition<TDbContext, TEntity>()
                    .Where(x => x.PrimaryKey && x.Identity);

                var count = 1;
                while (count > 0)
                {
                    if (updatePrimaryKeys && primaryKeys.Any())
                    {
                        var executedEntities = dbContext.ExecuteQueryDictionary(commandToExecute).ToList();
                        count = executedEntities.Count;

                        if (count == 0) break;

                        var batchEntities = entities.Take(batchSize).ToList();

                        for (int i = 0; i < batchEntities.Count && i < executedEntities.Count; i++)
                        {
                            var entity = batchEntities[i];
                            var value = executedEntities[i];

                            foreach (var primaryKey in primaryKeys)
                            {
                                var convertedValue = Convert.ChangeType(value[primaryKey.Name], primaryKey.PropertyType);
                                primaryKey.PropertyInfo.SetValue(entity, convertedValue);
                            }
                        }
                    }
                    else
                        count = dbContext.ExecuteCommand(commandToExecute);
                }                  
            }
            finally
            {
                dbContext.ExecuteCommand(dbContext.GenerateDropTableCommand(tableName));
            }
        }

        private static void BulkInsert<TDbContext, TEntity>(this TDbContext dbContext, string tableName, IEnumerable<TEntity> entities, bool addIdentityPrimaryKeys, bool createRowIndexColumn, int batchSize) where TDbContext : DbContext
        {
            var columnsDefinition = dbContext.GetColumnsDefinition<TDbContext, TEntity>()
                .Where(x => (!x.Identity && !x.Computed) || (addIdentityPrimaryKeys && x.PrimaryKey && x.Identity))
                .ToList();

            if (columnsDefinition.IsNullOrEmpty()) return;

            var dataTable = dbContext.GenerateDataTable(entities, addIdentityPrimaryKeys, createRowIndexColumn);

            var connection = (SqlConnection)dbContext.Database.GetDbConnection();
            if (connection.State == ConnectionState.Closed)
                connection.Open();

            using var bulkCopy = new SqlBulkCopy(connection)
            {
                DestinationTableName = tableName,
                BatchSize = batchSize
            };

            columnsDefinition.ToList().ForEach(x => bulkCopy.ColumnMappings.Add(x.Name, x.Name));

            if (createRowIndexColumn)
                bulkCopy.ColumnMappings.Add(_rowIndexColumnName, _rowIndexColumnName);

            bulkCopy.WriteToServer(dataTable);
        }

        private static IEntityType? GetEntityType<TDbContext, TEntity>(this TDbContext dbContext) where TDbContext : DbContext
        {
            return dbContext.Model.FindEntityType(typeof(TEntity));
        }

        private static string GetTableName<TDbContext, TEntity>(this TDbContext dbContext) where TDbContext : DbContext
        {
            return dbContext.GetEntityType<TDbContext, TEntity>()?.GetTableName() ?? "";
        }

        private static IList<ColumnDefinition> GetColumnsDefinition<TDbContext, TEntity>(this TDbContext dbContext) where TDbContext : DbContext
        {
            var tableName = dbContext.GetTableName<TDbContext, TEntity>();
            if (string.IsNullOrEmpty(tableName)) return [];

            var entityType = dbContext.GetEntityType<TDbContext, TEntity>();
            if (entityType == null) return [];

            return typeof(TEntity)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Select(prop => new 
                {
                    prop,
                    metaData = entityType.FindProperty(prop.Name)
                })
                .Where(x => x.metaData != null)
                .Select(x => new ColumnDefinition
                {
                    Name = x.metaData?.GetColumnName(StoreObjectIdentifier.Table(tableName, null)) ?? x.prop.Name,
                    DatabaseType = x.metaData?.GetColumnType()!,
                    PropertyType = x.prop.PropertyType,
                    PropertyInfo = x.prop,
                    PrimaryKey = x.metaData?.IsPrimaryKey() ?? false,
                    Identity = x.metaData?.GetValueGenerationStrategy() == SqlServerValueGenerationStrategy.IdentityColumn,
                    Computed = x.metaData?.GetComputedColumnSql() != null
                })
                .ToList();
        }

        private static DataTable GenerateDataTable<TDbContext, TEntity>(this TDbContext dbContext, IEnumerable<TEntity> entities, bool addIdentityPrimaryKeys, bool createRowIndexColumn) where TDbContext : DbContext
        {
            var response = new DataTable();

            var columnsDefinition = dbContext.GetColumnsDefinition<TDbContext, TEntity>()
                .Where(x => (!x.Identity && !x.Computed) || (addIdentityPrimaryKeys && x.PrimaryKey && x.Identity))
                .ToList();

            if (columnsDefinition.IsNullOrEmpty()) return response;

            columnsDefinition.ToList().ForEach(x =>
                response.Columns.Add(x.Name, Nullable.GetUnderlyingType(x.PropertyType) ?? x.PropertyType));

            if (createRowIndexColumn)
                response.Columns.Add(_rowIndexColumnName, typeof(Int64));

            foreach (var (entity, index) in entities.Select((value, i) => (value, i)))
            {
                var values = new object[columnsDefinition.Count + (createRowIndexColumn ? 1 : 0)];
                for (int i = 0; i < columnsDefinition.Count; i++)
                {
                    var column = columnsDefinition[i];
                    values[i] = columnsDefinition[i].PropertyInfo.GetValue(entity) ?? DBNull.Value;
                }

                if (createRowIndexColumn)
                    values[columnsDefinition.Count] = index + 1;

                response.Rows.Add(values);
            }

            return response;
        }

        private static string GenerateCreateTableCommand<TDbContext, TEntity>(this TDbContext dbContext, string tempTableName, bool addIdentityPrimaryKeys, bool createRowIndexColumn) where TDbContext : DbContext
        {
            var columnsDefinition = dbContext.GetColumnsDefinition<TDbContext, TEntity>()
                .Where(x => (!x.Identity && !x.Computed) || (addIdentityPrimaryKeys && x.PrimaryKey && x.Identity))
                .ToList();

            return dbContext.GenerateCreateTableCommand<TDbContext, TEntity>(columnsDefinition, tempTableName, createRowIndexColumn);
        }

        private static string GenerateCreateTableCommand<TDbContext, TEntity>(this TDbContext dbContext, IEnumerable<ColumnDefinition> columnsDefinition, string tempTableName, bool createRowIndexColumn) where TDbContext : DbContext
        {
            if (columnsDefinition.IsNullOrEmpty()) return string.Empty;

            if (dbContext.Database.IsSqlServer())
            {
                return @$"
                Create Table {tempTableName}
                (
                    {string.Join(",", columnsDefinition.Select(x => $"[{x.Name}] {x.DatabaseType} {(x.PrimaryKey ? "Primary Key" : "")} \n"))}
                    {(createRowIndexColumn ? $", {_rowIndexColumnName} BigInt" : "")}
                )";
            }

            return "";
        }

        private static string GenerateCreateVarTableCommand<TDbContext, TEntity>(this TDbContext dbContext, IEnumerable<ColumnDefinition> columnsDefinition, string tempTableName) where TDbContext : DbContext
        {
            if (columnsDefinition.IsNullOrEmpty()) return string.Empty;

            if (dbContext.Database.IsSqlServer())
            {
                return @$"
                Declare {tempTableName} Table 
                (
                    {string.Join(",", columnsDefinition.Select(x => $"[{x.Name}] {x.DatabaseType} {(x.PrimaryKey ? "Primary Key" : "")} \n"))}
                )";
            }

            return "";
        }

        private static string GenerateInsertCommand<TDbContext, TEntity>(this TDbContext dbContext, string tempTableName, int batchSize) where TDbContext : DbContext
        {
            var tableName = dbContext.GetTableName<TDbContext, TEntity>();
            if (string.IsNullOrEmpty(tableName)) return string.Empty;

            var columnsDefinition = dbContext
                .GetColumnsDefinition<TDbContext, TEntity>()
                .Where(x => !x.Computed && !(x.PrimaryKey && x.Identity));

            var primaryKeys = dbContext
                .GetColumnsDefinition<TDbContext, TEntity>()
                .Where(x => x.PrimaryKey && x.Identity);

            if (columnsDefinition.IsNullOrEmpty()) return string.Empty;

            if (dbContext.Database.IsSqlServer())
            {
                return $@"
                {dbContext.GenerateCreateVarTableCommand<TDbContext, TEntity>(primaryKeys, $"@{tableName}")}

                Insert Into {tableName} ({string.Join(",", columnsDefinition.Select(x => $"[{x.Name}]"))})          
                {(
                    primaryKeys.Any() ?
                    $"Output {string.Join(",", primaryKeys.Select(x => $"Inserted.[{x.Name}]"))} Into @{tableName}" : ""
                )}
                Select Top({batchSize}) {string.Join(",", columnsDefinition.Select(x => $"[{x.Name}]"))}
                From {tempTableName}
                Order By {_rowIndexColumnName};

                Delete d
                From 
                (
                    Select Top({batchSize}) temp.*
                    From {tempTableName} temp
                    Order By temp.{_rowIndexColumnName}
                ) d;

                {(
                    primaryKeys.Any() ?
                    $"Select * From @{tableName}" : ""
                )}
                ";
            }

            return string.Empty;
        }

        private static string GenerateUpdateCommand<TDbContext, TEntity>(this TDbContext dbContext, string tempTableName, int batchSize, IList<string>? columnsToUpdate = null) where TDbContext : DbContext
        {
            var tableName = dbContext.GetTableName<TDbContext, TEntity>();
            if (string.IsNullOrEmpty(tableName)) return string.Empty;

            var columnsDefinition = dbContext.GetColumnsDefinition<TDbContext, TEntity>().Where(x => !x.Computed);
            if (columnsDefinition.IsNullOrEmpty()) return string.Empty;

            if (!columnsDefinition.Any(x => x.PrimaryKey)) return string.Empty;

            if (dbContext.Database.IsSqlServer())
            {
                return $@"
                Update target
                Set {string.Join(",", columnsDefinition
                    .Where(x => !x.PrimaryKey && (columnsToUpdate.IsNullOrEmpty() || (columnsToUpdate?.Contains(x.Name) ?? false)))
                    .Select(x => $"target.[{x.Name}] = temp.[{x.Name}]\n"))}
                From {tableName} As target
                Join 
                (
                    Select Top({batchSize}) temp.*
                    From {tempTableName} temp
                    Order By temp.{_rowIndexColumnName}
                ) As temp
                    On {string.Join(" And ", columnsDefinition.Where(x => x.PrimaryKey).Select(x => $"target.[{x.Name}] = temp.[{x.Name}]\n"))};

                Delete d
                From 
                (
                    Select Top({batchSize}) temp.*
                    From {tempTableName} temp
                    Order By temp.{_rowIndexColumnName}
                ) d;";
            }

            return string.Empty;
        }

        private static string GenerateDropTableCommand<TDbContext>(this TDbContext dbContext, string tempTableName) where TDbContext : DbContext
        {
            if (dbContext.Database.IsSqlServer())
            {
                return $"Drop Table If Exists {tempTableName}";
            }

            return string.Empty;
        }

        private static int ExecuteCommand<TDbContext>(this TDbContext dbContext, string command) where TDbContext : DbContext
        {
            return dbContext.Database.ExecuteSqlRaw(command);
        }

        private static IEnumerable<Dictionary<string, object?>> ExecuteQueryDictionary<TDbContext>(this TDbContext context, string sql) where TDbContext : DbContext
        {
            var result = new List<Dictionary<string, object?>>();

            using var command = context.Database.GetDbConnection().CreateCommand();
            command.CommandText = sql;
            command.CommandType = CommandType.Text;


            if (command.Connection.State != ConnectionState.Open)
                command.Connection.Open();

            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var columnName = reader.GetName(i);
                    var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                    row[columnName] = value;
                }

                result.Add(row);
            }

            return result;
        }

        private static string CreateTempTableName()
        {
            const string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            var rng = RandomNumberGenerator.Create();

            var bytes = new byte[16];
            rng.GetBytes(bytes);

            var result = new StringBuilder(16);
            foreach (var b in bytes)
            {
                result.Append(chars[b % chars.Length]);
            }

            return $"Temp_{result}";
        }
    }
}
