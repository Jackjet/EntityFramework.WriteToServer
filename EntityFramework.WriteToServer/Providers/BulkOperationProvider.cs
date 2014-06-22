using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Reflection;
using EntityFramework.WriteToServer.Extensions;

namespace EntityFramework.WriteToServer.Providers
{
    internal class BulkOperationProvider
    {
        private readonly string _connectionString;
        private readonly DbContext _context;

        public BulkOperationProvider(DbContext context)
        {
            if (context == null)
                throw new ArgumentNullException("context");

            _context = context;

            ConnectionStringSettings contextConfig = ConfigurationManager.ConnectionStrings[context.GetType().Name];
            _connectionString = contextConfig.ConnectionString;
        }

        public void Insert<T>(IEnumerable<T> entities, int batchSize)
        {
            using (var dbConnection = new SqlConnection(_connectionString))
            {
                dbConnection.Open();

                using (SqlTransaction transaction = dbConnection.BeginTransaction())
                {
                    try
                    {
                        Insert(entities, transaction, SqlBulkCopyOptions.Default, batchSize);
                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        if (transaction.Connection != null)
                        {
                            transaction.Rollback();
                        }
                        throw ex;
                    }
                }
            }
        }

        private void Insert<T>(IEnumerable<T> entities, SqlTransaction transaction, SqlBulkCopyOptions options,
            int batchSize)
        {
            TableMapping tableMapping = DbMapper.GetDbMapping(_context)[typeof (T)];
            using (DataTable dataTable = CreateDataTable(tableMapping, entities))
            {
                using (var sqlBulkCopy = new SqlBulkCopy(transaction.Connection, options, transaction))
                {
                    sqlBulkCopy.BatchSize = batchSize;
                    sqlBulkCopy.DestinationTableName = dataTable.TableName;
                    sqlBulkCopy.WriteToServer(dataTable);
                }
            }
        }

        private static DataTable CreateDataTable<T>(TableMapping tableMapping, IEnumerable<T> entities)
        {
            DataTable dataTable = BuildDataTable<T>(tableMapping);

            foreach (T entity in entities)
            {
                DataRow row = dataTable.NewRow();

                foreach (ColumnMapping columnMapping in tableMapping.Columns)
                {
                    object @value = entity.GetPropertyValue(columnMapping.PropertyName);

                    if (columnMapping.IsIdentity) continue;

                    if (@value == null)
                    {
                        row[columnMapping.ColumnName] = DBNull.Value;
                    }
                    else
                    {
                        row[columnMapping.ColumnName] = @value;
                    }
                }

                dataTable.Rows.Add(row);
            }

            return dataTable;
        }

        private static DataTable BuildDataTable<T>(TableMapping tableMapping)
        {
            Type entityType = typeof (T);
            string tableName = string.Join(@".", tableMapping.SchemaName, tableMapping.TableName);

            var dataTable = new DataTable(tableName);
            var primaryKeys = new List<DataColumn>();

            foreach (ColumnMapping columnMapping in tableMapping.Columns)
            {
                PropertyInfo propertyInfo = entityType.GetProperty(columnMapping.PropertyName, '.');
                columnMapping.Type = propertyInfo.PropertyType;

                var dataColumn = new DataColumn(columnMapping.ColumnName);

                Type dataType;
                if (propertyInfo.PropertyType.IsNullable(out dataType))
                {
                    dataColumn.DataType = dataType;
                    dataColumn.AllowDBNull = true;
                }
                else
                {
                    dataColumn.DataType = propertyInfo.PropertyType;
                    dataColumn.AllowDBNull = columnMapping.Nullable;
                }

                if (columnMapping.IsIdentity)
                {
                    dataColumn.Unique = true;
                    if (propertyInfo.PropertyType == typeof (int)
                        || propertyInfo.PropertyType == typeof (long))
                    {
                        dataColumn.AutoIncrement = true;
                    }
                    else continue;
                }
                else
                {
                    dataColumn.DefaultValue = columnMapping.DefaultValue;
                }

                if (propertyInfo.PropertyType == typeof (string))
                {
                    dataColumn.MaxLength = columnMapping.MaxLength;
                }

                if (columnMapping.IsPk)
                {
                    primaryKeys.Add(dataColumn);
                }

                dataTable.Columns.Add(dataColumn);
            }

            dataTable.PrimaryKey = primaryKeys.ToArray();

            return dataTable;
        }
    }
}