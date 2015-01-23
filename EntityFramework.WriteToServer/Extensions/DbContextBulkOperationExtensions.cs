using System.Collections.Generic;
using System.Data.Entity;
using EntityFramework.WriteToServer.Providers;

namespace EntityFramework.WriteToServer.Extensions
{
    public static class DbContextBulkOperationExtensions
    {
        public const int DefaultBatchSize = 1000;

        public static void BulkInsert<T>(this DbContext context, IEnumerable<T> entities, int batchSize = DefaultBatchSize)
        {
            var provider = new BulkOperationProvider(context);
            provider.Insert(entities, batchSize);
        }

        public static void BulkInsertNoTransaction<T>(this DbContext context, IEnumerable<T> entities, int batchSize = DefaultBatchSize)
        {
            var provider = new BulkOperationProvider(context);
            provider.InsertNoTransaction(entities, batchSize);
        }
    }
}
