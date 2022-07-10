public async Task<DataResponse> GetData(string id)
{
	var parms = new DynamicParameters();
	parms.Add(nameof(id), id);
	using var dbConnection = _dbConnectionFactory.CreateConnection();
	return await _sqlResiliencyProvider.QueryFirstOrDefaultAsync<DataResponse>("[dbo].[Get_Data]", parms, null, null, CommandType.StoredProcedure);
}
