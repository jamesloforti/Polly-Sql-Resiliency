using Polly;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;

namespace MyService.Library.Database
{
    [ExcludeFromCodeCoverage]
    public static class SqlContextProvider
    {
        public static readonly string LoggerContextKey = nameof(LoggerContextKey);
        public static readonly string SqlContextKey = nameof(SqlContextKey);
        public static readonly string ParamContextKey = nameof(ParamContextKey);
        public static readonly string ConnectionContextKey = nameof(ConnectionContextKey);
        public static Context CreateContext(IDbConnection connection, ILogger logger, string sql, object param, string operationKey)
        {
            return new Context(operationKey, new Dictionary<string, object>()
            {
                { LoggerContextKey, logger },
                { SqlContextKey, sql },
                { ParamContextKey, param },
                { ConnectionContextKey, connection }
            });
        }
        public static ILogger GetLogger(this Context context)
        {
            return context[LoggerContextKey] as ILogger;
        }
        public static bool TryGetConnection(this Context context, out IDbConnection connection)
        {
            if (context[ConnectionContextKey] is IDbConnection dbConnection)
            {
                connection = dbConnection;
                return true;
            }
            connection = null;
            return false;
        }
    }
}
