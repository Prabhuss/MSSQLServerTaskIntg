using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using System.Threading;


namespace MSSQLServer_Local_TaskIntg
{
    class Program
    {
        static string logFileName = "";
        static string logFileDir = "";
        static string logFilePathName = "";
        static void Main(string[] args)
        {
            try
            {
                string tableParam = ConfigurationManager.AppSettings["tablename"];
                string runningParam = ConfigurationManager.AppSettings["runningindex"];
                string indexValues = ConfigurationManager.AppSettings["indexVal"];
                string dirToStoreLastIndex = ConfigurationManager.AppSettings["dirToStoreLastIndex"];
                string fileToStoreLastIndex = ConfigurationManager.AppSettings["fileToStoreLastIndex"];
                string filePathToStoreLastIndex = Path.Combine(dirToStoreLastIndex, fileToStoreLastIndex);
                string notSentDir = ConfigurationManager.AppSettings["NotSentDir"];
                string waitTime = ConfigurationManager.AppSettings["WaitTime"];
                string maxRowStr = ConfigurationManager.AppSettings["maxRowInOneShot"];
                string NotSentDir = ConfigurationManager.AppSettings["NotSentDir"];
                string ExponentialFactor = ConfigurationManager.AppSettings["ExponentialFactor"];

                logFileName = ConfigurationManager.AppSettings["logfilename"];
                logFileDir = ConfigurationManager.AppSettings["logFileDir"];
                logFilePathName = Path.Combine(logFileDir, logFileName);
                if (!Directory.Exists(logFileDir))
                {
                    Directory.CreateDirectory(logFileDir);
                }

                // Copy the old Log file which will be transferred to clould
                if (File.Exists(logFilePathName))
                {
                    string logFileFullPathName = logFilePathName;
                    // Get Log file name to transfer to cloud
                    string logFileWithDate = fileNameCreator.logFileNameCreatetor(logFileName);
                    string logFilePathNameForNotSent = Path.Combine(NotSentDir, logFileWithDate);

                    File.Copy(logFileFullPathName, logFilePathNameForNotSent, true);
                    File.Delete(logFileFullPathName);
                }

                int waitTimeInSecondsBeforeNextSync = 0;
                try
                {
                    waitTimeInSecondsBeforeNextSync = int.Parse(waitTime);
                    Log("Configured WaitTime is = " + waitTime);
                }
                catch
                {
                    Log("Wait Time is not configured propely");
                    // Defalut Value of Wait Time is 30 Seconds
                    waitTimeInSecondsBeforeNextSync = 30000;
                }
                int maxRowsInOneFetch = 0;
                try
                {
                    maxRowsInOneFetch = int.Parse(maxRowStr);
                    Log("Configured Max Rows is = " + maxRowsInOneFetch);
                }
                catch
                {
                    Log("Max Rows In One Fetch configured propely");
                    // Defalut Value of Wait Time is 30 Seconds
                    maxRowsInOneFetch = 8192;
                }

                SqlConnection conn;
                int i = 1;
                int WritingFactor = 1;
                while (true)
                {




                    try
                    {
                        // Prepare for Making the connection
                        conn = new SqlConnection();
                        string ConnectionString = ConfigurationManager.ConnectionStrings["MSSQLSERVERDB"].ConnectionString;
                        conn.ConnectionString = ConnectionString;
                        conn.Open();
                        break;
                    }
                    catch (Exception ex)
                    {
                        if (i / WritingFactor == 1)
                        {
                            Log(" Looks like MSAccess DataSyncer is not started "+ex.ToString());
                            // follow exponnetial increase to avoid system load 
                            WritingFactor = i * (Convert.ToInt32(ExponentialFactor));
                        }
                        i++;
                    }

                }
                string[] tableNames = tableParam.Split(',');
                string[] runningNames = runningParam.Split(',');
                string[] indexVal = indexValues.Split(',');



                // Exit if Number of whereCluase list does not match with
                // Number of Tables

                if (tableNames.Length != runningNames.Length)
                {
                    Log(" Oops! Mismatch Between Number Of WhereClause and Number Of Tables");
                    return;
                }

                IDictionary<string, string> tableIndexDict = new Dictionary<string, string>();
                try
                {
                    // Make Table Name and Index Name Mapping
                    int runningIndex = 0;
                    foreach (string tblName in tableNames)
                    {
                        tableIndexDict.Add(tblName, (runningNames[runningIndex]));
                        runningIndex++;
                    }
                }
                catch
                {
                    Log(" Counld not Construct Table Index Dictonary");
                }
                // Initialze the initial indicies 
                IDictionary<string, int> indicesDict = new Dictionary<string, int>();
                IDictionary<string, int> tempIndicesDict = new Dictionary<string, int>();

                if (!File.Exists(filePathToStoreLastIndex))
                {
                    // for each table fill 0
                    int index = 0;
                    foreach (string table in tableNames)
                    {
                        indicesDict.Add(table, int.Parse(indexVal[index]));
                        index++;
                    }
                }
                else
                {
                    // Read Last indices from file
                    string readIndiecsFile = File.ReadAllText(filePathToStoreLastIndex);
                    string[] tableNIndexstrings = readIndiecsFile.Split('\n');
                    foreach (string tblNIndex in tableNIndexstrings)
                    {
                        string[] splitTblNindex = tblNIndex.Split(',');
                        if (splitTblNindex.Length > 1)
                        {
                            indicesDict.Add(splitTblNindex[0], int.Parse(splitTblNindex[1]));
                        }
                    }

                    // Adjsut the table name which exists in configuration file but does not exist
                    // in dictionary
                    string tableNamesFromFile = " ";
                    foreach (string tblNIndex in tableNIndexstrings)
                    {
                        string[] splitTblNindex = tblNIndex.Split(',');
                        tableNamesFromFile = tableNamesFromFile + splitTblNindex[0] + ",";
                    }

                    foreach (string tableNamesFromConfig in tableNames)
                    {
                        if (tableNamesFromFile.Contains(tableNamesFromConfig))
                        {
                            continue;
                        }
                        else
                        {
                            Log("Entry:   " + tableNamesFromConfig + "  Not found");
                            indicesDict.Add(tableNamesFromConfig, 0);
                        }
                    }
                }
                int whereClauseMaxIndex;
                //int i;
                while (true)
                {
                    //i =0;
                    foreach (KeyValuePair<string, int> item in indicesDict)
                    {
                        whereClauseMaxIndex = pollAndFetchDeltaEntriesFromDB(conn, item.Key, item.Value,
                            tableIndexDict[item.Key], maxRowsInOneFetch);
                        //indicesDict.Remove(item.Key);
                        if (whereClauseMaxIndex != 0)
                        {
                            tempIndicesDict.Add(item.Key, whereClauseMaxIndex);
                        }
                        else
                        {
                            // Let us have original value only
                            tempIndicesDict.Add(item.Key, item.Value);
                        }
                    }
                    indicesDict.Clear();
                    // update indicesDict with latest indices and store it in file
                    foreach (KeyValuePair<string, int> item in tempIndicesDict)
                    {
                        indicesDict.Add(item.Key, item.Value);
                    }
                    tempIndicesDict.Clear();
                    // Update the txt file with new indices

                    StringBuilder lastIndicSb = new StringBuilder();
                    foreach (var entry in indicesDict)
                    {
                        lastIndicSb.AppendLine(entry.Key + "," + entry.Value);
                    }
                    System.IO.File.WriteAllText(filePathToStoreLastIndex, lastIndicSb.ToString());
                    Thread.Sleep(waitTimeInSecondsBeforeNextSync);
                }
            }
            catch (Exception ex)
            {
                Log(ex.Message);
            }
        }
        private static int pollAndFetchDeltaEntriesFromDB(SqlConnection conn, string tableName,
             int index, string whereClause, int maxRowsInOneFetch)
        {
            try
            {
                string cmdStr = "select top " + maxRowsInOneFetch + " *  from " + tableName + " where " +
                    whereClause + "+0 > " + index + "    Order by " + whereClause + "+0  ";
                SqlCommand cmd = new SqlCommand(cmdStr, conn);
                cmd.CommandTimeout = 0;
                var result = cmd.ExecuteReader();
                result.Close();
                int whereClauseMaxIndex = copyValuesFromReaderNReturnValueForWhereClause(tableName, cmd, whereClause);
                return whereClauseMaxIndex;
            }
            catch (Exception ex)
            {
                Log(ex.Message);
                return 0;
            }
        }
        private static int copyValuesFromReaderNReturnValueForWhereClause(string table,SqlCommand cmd, string whereClause)
        {
            try
            {
                SqlDataReader dataReader = cmd.ExecuteReader();
                string outputStr = "";
                int maxValueForWhereCaluse = 0;
                int minValueForWhereCaluse = 0;
                int i = 0;
                //First Get Header info of Table
                int whereClausePosition = 0;

                while (i < dataReader.FieldCount)
                {
                    if (i != 0)
                    {
                        outputStr = outputStr + ",";

                    }

                    var output = dataReader.GetName(i);
                    if (output == whereClause)
                    {
                        whereClausePosition = i;
                    }
                    outputStr = outputStr + output.ToString();
                    i++;
                }
                // New line 
                outputStr = outputStr + '\n';
                while (dataReader.Read())
                {
                    i = 0;
                    while (i < dataReader.FieldCount)
                    {
                        if (i != 0)
                        {
                            outputStr = outputStr + ",";

                        }
                        var output = dataReader.GetValue(i);
                        outputStr = outputStr + output.ToString();
                        i++;
                    }
                    outputStr = outputStr + '\n';
                }
                dataReader.Close();
                string[] splitOutputStr = outputStr.Split('\n');

                string[] splitOutputStrOnCommaForMaxIndex = splitOutputStr[splitOutputStr.Length - 2].Split(',');
                // get the max value of where clause position
                maxValueForWhereCaluse = int.Parse(splitOutputStrOnCommaForMaxIndex[whereClausePosition]);

                string[] splitOutputStrOnCommaForMinIndex = splitOutputStr[1].Split(',');
                // get the max value of where clause position
                minValueForWhereCaluse = int.Parse(splitOutputStrOnCommaForMinIndex[whereClausePosition]);

                // Get  the appropirate file name
                string csvFileName = fileNameCreator.csvFileNameCreatetor(table);
                System.IO.File.WriteAllText(csvFileName, outputStr);
                Log("For Table: " + table +
                    " Min/Max Value for where Clause is " + minValueForWhereCaluse + "/" + maxValueForWhereCaluse);

                return maxValueForWhereCaluse;
            }
            catch (Exception )
            {
                // Log(" Looks like Same data fectch and hence maxValueForWhereClause failed ");
                // let us return zero, since exception has occured csv file 
                //also will not be created and transferred
                return 0;
            }
        }

        private static void LogParam(string message, string param)
        {
            string finalLogMsg = string.Format("{0}  {1} ", message, param);
            Log(finalLogMsg);
        }

        public static void Log(string msg)
        {
            try
            {
                using (var sw = new StreamWriter(logFilePathName, true))
                {

                    sw.Write(string.Format("{0} - {1}\n", DateTime.Now, msg));
                    sw.Flush();
                }
            }
            catch (Exception ex)
            {
                Log(ex.ToString());
            }
        }
    }
}
