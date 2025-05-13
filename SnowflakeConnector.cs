using System.Data;
using Newtonsoft.Json;
using Snowflake.Data.Client;
using System.Data.Common;

namespace OutSystems.SnowflakeConnector
{
    public class SnowflakeConnector : ISnowflakeConnector
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="ssUsername"></param>
        /// <param name="ssPassword"></param>
        /// <param name="ssIsSuccessful"></param>
        /// <param name="ssQuery"></param>
        /// <param name="ssScheme"></param>
        /// <param name="ssAccount"></param>
        /// <param name="ssHost"></param>
        /// <param name="ssPort"></param>
        /// <param name="ssRole"></param>
        /// <param name="ssWarehouse"></param>
        /// <param name="ssExtraParametersForConnectionString"></param>
        /// <param name="ssResultInJSON"></param>
        public void RunQuery(string ssUsername, string ssPassword, string ssSchema, string ssAccount, string ssHost, string ssPort, string ssRole, string ssWarehouse, string ssExtraParametersForConnectionString, string ssQuery, out bool ssIsSuccessful, out string ssResultInJSON, string ssDatabase)
        {

            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    string connectionString =
                        "ACCOUNT=" + ssAccount + ";" +
                        "USER=" + ssUsername + ";" +
                        "PASSWORD=" + ssPassword + ";" ;

                    connectionString = AppendOptionalParametersToTheConnectionString(ssDatabase, ssHost, ssRole, ssSchema, ssWarehouse, ssPort, connectionString, ssExtraParametersForConnectionString);

                    RunQuery(ssQuery, out ssIsSuccessful, out ssResultInJSON, conn, connectionString);
                }
            }

            catch (DbException exc)
            {
                ssIsSuccessful = false;
                ssResultInJSON = CreateErrorMessage(exc);
            }


        } // MssRunQuery
    
        public void RunQuery_JWTAuth(string ssUsername, string ssPrivateKeyContent, string ssPrivateKeyPWD, string ssSchema, string ssAccount, string ssHost, string ssPort, string ssRole, string ssWarehouse, string ssExtraParametersForConnectionString, string ssQuery, out bool ssIsSuccessful, out string ssResultInJSON, string ssDatabase)
        {
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    //from Snowflake's documentation: connectionstring="account=testaccount;authenticator=snowflake_jwt;user=testuser;private_key={0};db=testdb;schema=testschema";
                    string connectionString = 
                        "ACCOUNT=" + ssAccount + ";" +
                        "USER=" + ssUsername + ";" +
                        "AUTHENTICATOR=SNOWFLAKE_JWT" + ";" +
                        "PRIVATE_KEY=" + ssPrivateKeyContent + ";";

                    if (ssPrivateKeyPWD != "")
                    {
                        connectionString = connectionString + "PRIVATE_KEY_PWD=" + ssPrivateKeyPWD + ";";
                    }

                    connectionString = AppendOptionalParametersToTheConnectionString(ssDatabase, ssHost, ssRole, ssSchema, ssWarehouse, connectionString, ssPort, ssExtraParametersForConnectionString);

                    RunQuery(ssQuery, out ssIsSuccessful, out ssResultInJSON, conn, connectionString);
                }
            }

            catch (DbException exc)
            {
                ssIsSuccessful = false;
                ssResultInJSON = CreateErrorMessage(exc);
            }

        }

        private static string AppendOptionalParametersToTheConnectionString(string ssDatabase, string ssHost, string ssRole, string ssSchema, string ssWarehouse, string ssPort, string connectionString, string ssExtraParametersForConnectionString)
        {
            if (ssDatabase != "")
            {
                connectionString = connectionString + "DB=" + ssDatabase + ";";
            }

            if (ssHost != "")
            {
                connectionString = connectionString + "HOST=" + ssHost + ";";
            }

            if (ssRole != "")
            {
                connectionString = connectionString + "ROLE=" + ssRole + ";";
            }

            if (ssSchema != "")
            {
                connectionString = connectionString + "SCHEMA=" + ssSchema + ";";
            }

            if (ssWarehouse != "")
            {
                connectionString = connectionString + "WAREHOUSE=" + ssWarehouse + ";";
            }
            if (ssPort != "")
            {
                connectionString = connectionString + "PORT=" + ssPort + ";"; ;
            }

            if (ssExtraParametersForConnectionString != "")
            {
                connectionString = connectionString + ssExtraParametersForConnectionString;
            }

            return connectionString;
        }

        private static string CreateErrorMessage(DbException exc)
        {
            return "There was an error. \n Message: " + exc.Message + "\n Stacktrace: " + exc.StackTrace + "\n Error Code: " + exc.ErrorCode + "\n Source: " + exc.Source + "\n Data: " + exc.Data + "\n InnerException:" + exc.InnerException;
        }

        private static void RunQuery(string ssQuery, out bool ssIsSuccessful, out string ssResultInJSON, SnowflakeDbConnection conn, string connectionString)
        {
            conn.ConnectionString = connectionString;
            conn.Open();
            var cmd = conn.CreateCommand();
            cmd.CommandText = ssQuery;
            ssResultInJSON = "";

            var reader = cmd.ExecuteReader();
            if (reader.HasRows)
            {
                DataTable dataTable = new DataTable();
                dataTable.Load(reader);
                ssResultInJSON = JsonConvert.SerializeObject(dataTable);
            }
            else
            {
                ssResultInJSON = "No rows found.";
            }
            conn.Close();
            ssIsSuccessful = true;
        }

    }
}