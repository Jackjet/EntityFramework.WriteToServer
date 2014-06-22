using System;
using System.Collections.Generic;
using System.Data.Entity;

namespace EntityFramework.WriteToServer
{
    internal class DbMapper
    {
        private static readonly Dictionary<Type, DbMapping> Mappings = new Dictionary<Type, DbMapping>();

        public static DbMapping GetDbMapping(DbContext context)
        {
            var contextType = context.GetType();
            if (Mappings.ContainsKey(contextType))
            {
                return Mappings[contextType];
            }

            var mapping = new DbMapping(context);
            Mappings[contextType] = mapping;

            return mapping;
        }
    }
}
