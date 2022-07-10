using Polly;
using Polly.Retry;
using Polly.Timeout;
using Polly.Wrap;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace MyService.Library.Database
{
    [ExcludeFromCodeCoverage]
    public static class SqlResiliencyPolicyProvider
    {
        //https://docs.microsoft.com/en-us/sql/relational-databases/errors-events/database-engine-events-and-errors?view=sql-server-2016
        private static readonly Dictionary<string, int> TransientNumbers = new Dictionary<string, int> 
        {
            { "Deadlock", 1205 }
        };
        private static readonly Dictionary<string, int> NetworkingNumbers = new Dictionary<string, int>
        {
            { "ServerNotFound", 258 },
            { "TimeoutExpired", -2 },
            { "ConnectionFailed", 53 },
            { "ConnectionBroke", 0 },
            { "ConnectionFailedNoResponse", 10060 },
            { "PreLoginHandshakeFailed", 64 },
            { "NotFound", 26 },
            { "ConnectionAborted", 10053 },
        };
        public static AsyncPolicyWrap GetAllPolicies(int maxRetries, TimeSpan maxTimeout)
        {
            return GetSqlExceptionTimeoutPolicy(maxTimeout)
                    .WrapAsync(GetSqlExceptionTransientPolicy(maxRetries))
                    .WrapAsync(GetSqlExceptionNetworkingPolicy(maxRetries));
        }
        private static AsyncTimeoutPolicy GetSqlExceptionTimeoutPolicy(TimeSpan maxTimeout)
        {
            return Policy.TimeoutAsync(maxTimeout);
        }
        public static AsyncRetryPolicy GetSqlExceptionTransientPolicy(int maxRetries)
        {
            return Policy
                .Handle<SqlException>(ex => TransientNumbers.Values.Contains(ex.Number))
                .WaitAndRetryAsync(maxRetries, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    onRetry: (ex, attemptSeconds, context) =>
                    {
                        var exception = ex as SqlException;
                        context.GetLogger()?.Log(LogLevel.Warn, $"{nameof(GetSqlExceptionTransientPolicy)} exception, retrying:", ex, new Dictionary<string, object>
                        {
                            { nameof(attemptSeconds), attemptSeconds.ToString() },
                            { nameof(maxRetries), maxRetries.ToString() },
                            { nameof(exception.Number), exception?.Number.ToString() },
                        });
                    });
        }
        public static AsyncRetryPolicy GetSqlExceptionNetworkingPolicy(int maxRetries)
        {
            return Policy
                .Handle<SqlException>(ex => NetworkingNumbers.Values.Contains(ex.Number))
                .WaitAndRetryAsync(maxRetries, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    onRetry: (ex, attemptSeconds, context) =>
                    {
                        var exception = ex as SqlException;
                        context.GetLogger()?.Log(LogLevel.Warn, $"{nameof(GetSqlExceptionNetworkingPolicy)} exception, retrying:", ex, new Dictionary<string, object>
                        {
                            { nameof(attemptSeconds), attemptSeconds.ToString() },
                            { nameof(maxRetries), maxRetries.ToString() },
                            { nameof(exception.Number), exception?.Number.ToString() },
                        });
                    });
        }
    }
}
