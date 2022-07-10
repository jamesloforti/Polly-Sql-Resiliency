using Dapper;
using MyService.Library.Config;
using MyService.Library.Database.Interfaces;
using Polly;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace MyService.Library.Database
{
    [ExcludeFromCodeCoverage]
    public class SqlResiliencyProvider : ISqlResiliencyProvider
    {
        private readonly ILogger<SqlResiliencyProvider> _logger;
        private readonly AppSettings _appSettings;
        private readonly IDbConnectionFactory _dbConnectionFactory;
        private readonly IAsyncPolicy _resiliencyPolicy;
		
        public SqlResiliencyProvider(ILogger<SqlResiliencyProvider> logger, AppSettings appSettings, IDbConnectionFactory dbConnectionFactory)
        {
            _logger = logger;
            _appSettings = appSettings;
            _dbConnectionFactory = dbConnectionFactory;
            _resiliencyPolicy = SqlResiliencyPolicyProvider.GetAllPolicies(_appSettings.SqlDefaultRetryCount, TimeSpan.FromSeconds(_appSettings.SqlTimeoutPolicySeconds));
        }
		
        public async Task<int> ExecuteAsync(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            return await ExecuteWithResiliency((s, p, c) => c.ExecuteAsync(s, p, transaction, commandTimeout, commandType), sql, param);
        }
		
        public async Task<T> ExecuteScalarAsync<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            return await ExecuteWithResiliency((s, p, c) => c.ExecuteScalarAsync<T>(s, p, transaction, commandTimeout, commandType), sql, param);
        }
		
        public async Task<T> QueryFirstOrDefaultAsync<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            return await ExecuteWithResiliency((s, p, c) => c.QueryFirstOrDefaultAsync<T>(s, p, transaction, commandTimeout, commandType), sql, param);
        }
		
        public async Task<IEnumerable<T>> QueryAsync<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            return await ExecuteWithResiliency((s, p, c) => c.QueryAsync<T>(s, p, transaction, commandTimeout, commandType), sql, param);
        }
		
        public async Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TReturn>(string sql, Func<TFirst, TSecond, TReturn> map, object param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
        {
            return await ExecuteWithResiliency((s, p, c) => c.QueryAsync(s, map, p, transaction, buffered, splitOn, commandTimeout, commandType), sql, param);
        }
		
        public async Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TReturn>(string sql, Func<TFirst, TSecond, TThird, TReturn> map, object param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
        {
            return await ExecuteWithResiliency((s, p, c) => c.QueryAsync(s, map, p, transaction, buffered, splitOn, commandTimeout, commandType), sql, param);
        }
		
        private async Task<T> ExecuteWithResiliency<T>(Func<string, object, IDbConnection, Task<T>> connectionFunc, string sql, object param = null, [CallerMemberName] string operation = "")
        {
            using var connection = _dbConnectionFactory.CreateConnection();
            var context = SqlContextProvider.CreateContext(connection, _logger, sql, param, operation);
            return await _resiliencyPolicy.ExecuteAsync(context => connectionFunc(sql, param, connection), context);
        }
    }
}