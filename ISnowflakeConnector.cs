﻿using OutSystems.ExternalLibraries.SDK;

namespace OutSystems.SnowflakeConnector
{
    [OSInterface(Description = "External library that enables Snowflake integrations in ODC", IconResourceName = "OutSystems.SnowflakeConnector.resources.snowflake.png")]
    public interface ISnowflakeConnector
    {
        /// <summary>
		/// 
		/// </summary>
		/// <param name="username"></param>
		/// <param name="password"></param>
		/// <param name="schema"></param>
		/// <param name="account"></param>
		/// <param name="host"></param>
		/// <param name="port"></param>
		/// <param name="role"></param>
		/// <param name="warehouse"></param>
		/// <param name="extraParametersForConnectionString"></param>
		/// <param name="query"></param>
		/// <param name="isSuccessful"></param>
		/// <param name="resultInJSON"></param>
		/// <param name="database"></param>
        void RunQuery(string username, string password, string schema, string account, string host, string port, string role, string warehouse, string extraParametersForConnectionString, string query, out bool isSuccessful, out string resultInJSON, string database);

		/// <summary>
		/// 
		/// </summary>
		/// <param name="username"></param>
		/// <param name="privateKey"></param>
		/// <param name="privateKey"></param>
		/// <param name="schema"></param>
		/// <param name="account"></param>
		/// <param name="host"></param>
		/// <param name="port"></param>
		/// <param name="role"></param>
		/// <param name="warehouse"></param>
		/// <param name="extraParametersForConnectionString"></param>
		/// <param name="query"></param>
		/// <param name="isSuccessful"></param>
		/// <param name="resultInJSON"></param>
		/// <param name="database"></param>
        void RunQuery_JWTAuth(string username, string privateKey, string privateKeyPWD, string schema, string account, string host, string port, string role, string warehouse, string extraParametersForConnectionString, string query, out bool isSuccessful, out string resultInJSON, string database);
    }
}
