// -----------------------------------------------------------------------
// <copyright file="AdoDBAccesor.cs" company="SAPIENT">
// TODO: Update copyright text.
// </copyright>
// -----------------------------------------------------------------------

namespace TransformTransform.DataBase
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Data.SqlClient;
    using System.Data;
    using System.Threading;
    using System.Configuration;
    using TransformTransform;

    public static class AdoDbAccessor
    {
        private const int CommandTimeoutInSecs = 240;
        
        private static readonly AutoResetEvent DataReadLock = new AutoResetEvent(true);

        static AdoDbAccessor()
        {
            try
            {
                if (ConfigurationManager.ConnectionStrings == null
                    || ConfigurationManager.ConnectionStrings.Count < 1)
                {
                    throw new Exception();
                }
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("ConnectionState String setting [{0}] or [{1}] must be available in configuration App.config/Web.config.", "Master_connection_String", "CMRS_MASTER"), ex);
            }
        }
        
        public static DataTable ExecuteStoredProcedureAsDataTable(string storedProcName, string conString, SqlParameter[] parameters, bool isLongRunningTransaction = false)
        {
            var dataTable = new DataTable();

            using (SqlConnection con = new SqlConnection(conString))
            {
                con.Open();

                using (SqlCommand command = new SqlCommand(storedProcName, con))
                {
                    command.CommandType = CommandType.StoredProcedure;

                    if (parameters != null)
                    {
                        command.Parameters.AddRange(parameters);
                    }

                    using (SqlDataAdapter adapter = new SqlDataAdapter(command))
                    {
                        adapter.Fill(dataTable);
                    }
                }
            }

            return dataTable;
        }

        ////public static int ExecuteNonQuery(string commandText,string conString)
        ////{
        ////    IDbConnection idbConnection = new SqlConnection();
        ////    IDbCommand idbCommand = new SqlCommand();

        ////    var cnb = new SqlConnectionStringBuilder(conString);
        ////    //idbConnection.ConnectionString = cnb.ConnectionString;

        ////    using (var con = new SqlConnection(cnb.ConnectionString) )
        ////    if (idbConnection.State != ConnectionState.Open)
        ////    {
        ////        idbConnection.Open();
        ////    }

        ////    idbCommand.Parameters.Clear();

        ////    idbCommand.Connection = idbConnection;
        ////    idbCommand.CommandType = CommandType.Text;
        ////    idbCommand.CommandText = commandText;
        ////    idbCommand.CommandTimeout = Convert.ToInt32(ConfigurationManager.AppSettings["sql_query_timeout"] ?? "100000");

        ////    var returnValue = idbCommand.ExecuteNonQuery();
        ////    return returnValue;
        ////}

        public static int ExecuteNonQuery(string commandText, string conString, SqlParameter[] parameters)
        {
            var cnb = new SqlConnectionStringBuilder(conString);
            using (var con = new SqlConnection(cnb.ConnectionString))
            {
                con.Open();
                using (var command = new SqlCommand(commandText, con))
                {
                    command.CommandType = CommandType.Text;
                    if (parameters != null)
                    {
                        command.Parameters.AddRange(parameters);
                    }

                    return command.ExecuteNonQuery();

                }
            }
        }

        //public static DataTable ExecuteQuery(string query, string conString)
        //{
        //    var resultTable = new DataTable();

        //    using (var con = new SqlConnection(conString))
        //    {
        //        con.Open();

        //        using (var adapter = new SqlDataAdapter(query, con))
        //        {
        //            adapter.Fill(resultTable);
        //        }
        //    }

        //    return resultTable;
        //}



        public static void BulkInsert(DataTable dataTable, string destinationTableName, string connectionString)
        {
            if (dataTable != null && !string.IsNullOrEmpty(destinationTableName) && !string.IsNullOrEmpty(connectionString))
            {
                using (var bulkCopy = new SqlBulkCopy(connectionString))
                {
                    bulkCopy.DestinationTableName = destinationTableName;
                    bulkCopy.WriteToServer(dataTable);
                }
            }
        }

        public static DataTable ExecuteStoredProcedureAsDataTable(string storedProcName, SqlParameter[] parameters, string connectionString, bool isLongRunningTransaction = false)
        {
            var dataTable = new DataTable();

            using (var con = new SqlConnection(connectionString))
            {
                con.Open();

                using (var command = new SqlCommand(storedProcName, con))
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandTimeout = GetConnectionTimeOut(isLongRunningTransaction);
                    if (parameters != null)
                    {
                        command.Parameters.AddRange(parameters);
                    }

                    using (var adapter = new SqlDataAdapter(command))
                    {
                        adapter.Fill(dataTable);
                    }
                }
            }

            return dataTable;
        }

        public static DataSet ExecuteStoredProcedureAsDataSet(string storedProcName, SqlParameter[] parameters, string connectionString, bool isLongRunningTransaction = false)
        {
            var dataSet = new DataSet();

            using (var con = new SqlConnection(connectionString))
            {
                con.Open();

                using (var command = new SqlCommand(storedProcName, con))
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandTimeout = GetConnectionTimeOut(isLongRunningTransaction);
                    if (parameters != null)
                    {
                        command.Parameters.AddRange(parameters);
                    }

                    using (var adapter = new SqlDataAdapter(command))
                    {
                        adapter.Fill(dataSet);
                    }
                }
            }

            return dataSet;
        }

        public static object ExecuteStoredProcedureAsScalar(string storedProcName, SqlParameter[] parameters, string connectionString, bool isLongRunningTransaction = false)
        {
            object data = null;

            using (var con = new SqlConnection(connectionString))
            {
                con.Open();

                using (var command = new SqlCommand(storedProcName, con))
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandTimeout = GetConnectionTimeOut(isLongRunningTransaction);

                    if (parameters != null)
                    {
                        command.Parameters.AddRange(parameters);
                    }

                    data = command.ExecuteScalar();
                }
            }

            return data;
        }

        public static void ExecuteStoredProcAsNonQuery(string storedProcName, SqlParameter[][] parametersList, string conString)
        {
            var cnb = new SqlConnectionStringBuilder(conString);
            using (var con = new SqlConnection(cnb.ConnectionString))
            {
                con.Open();

                SqlCommand command = null;

                try
                {
                    command = new SqlCommand(storedProcName, con)
                    {
                        CommandType = CommandType.StoredProcedure,
                        CommandTimeout = CommandTimeoutInSecs
                    };

                    if (parametersList != null)
                    {
                        foreach (var parameters in parametersList)
                        {
                            if (parameters != null)
                            {
                                command.Parameters.Clear();
                                command.Parameters.AddRange(parameters);
                            }


                        }
                    }
                    command.ExecuteNonQuery();
                }
                finally
                {
                    if (command != null)
                    {
                        command.Dispose();
                    }
                }
            }
        }

        public static DataTable ExecuteQuery(string query, string connectionString)
        {
            var resultTable = new DataTable();

            using (var con = new SqlConnection(connectionString))
            {
                con.Open();

                using (var adapter = new SqlDataAdapter(query, con))
                {
                    adapter.Fill(resultTable);
                }
            }

            return resultTable;
        }

        public static object ExecuteScalarQuery(string query, SqlParameter[] parameters, string connectionString)
        {
            using (var con = new SqlConnection(connectionString))
            {
                con.Open();
                using (var command = new SqlCommand(query, con))
                {
                    command.CommandType = CommandType.Text;
                    if (parameters != null)
                    {
                        command.Parameters.AddRange(parameters);
                    }

                    return command.ExecuteScalar();

                }
            }

        }

        public static bool HasConnection(string connectionString)
        {
            using (var con = new SqlConnection(connectionString))
            {
                con.Open();
                con.Close();
            }

            return true;
        }

        #region Transaction

        public static SqlConnection GetConnectionForTransaction(string connectionString)
        {
            var con = new SqlConnection(connectionString);
            return con;
        }

        public static void BulkInsert(DataTable dataTable, string destinationTableName, SqlConnection connection, SqlTransaction transaction)
        {
            if (dataTable != null && !string.IsNullOrEmpty(destinationTableName))
            {
                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = destinationTableName;
                    bulkCopy.BulkCopyTimeout = GetConnectionTimeOut(true);
                    bulkCopy.WriteToServer(dataTable);
                }
            }
        }

        public static void ExecuteStoredProcAsNonQuery(string storedProcName, SqlParameter[][] parametersList, SqlConnection connection, SqlTransaction transaction)
        {
            SqlCommand command = null;

            command = new SqlCommand(storedProcName, connection, transaction)
            {
                CommandType = CommandType.Text,
                CommandTimeout = CommandTimeoutInSecs
            };

            foreach (var parameters in parametersList)
            {
                if (parameters != null)
                {
                    command.Parameters.Clear();
                    command.Parameters.AddRange(parameters);
                }

                command.ExecuteNonQuery();
            }
        }

        public static void ExecuteQuery(string query, SqlConnection connection, SqlTransaction transaction)
        {
            var command = new SqlCommand(query, connection, transaction);
            command.ExecuteNonQuery();
        }
        #endregion

        /// <summary>
        /// Returns the connection timeout
        /// </summary>
        /// <param name="isLongRunningTransaction">flag to tell if it is long running transaction</param>
        /// <returns>connection timeout</returns>
        private static int GetConnectionTimeOut(bool isLongRunningTransaction)
        {
            if (isLongRunningTransaction)
            {
                return ApplicationConfigurationReader.LongRunningCommandTimeoutInSecs;
            }

            return CommandTimeoutInSecs;
        }
    }


}
