using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data.OleDb;

namespace AccessListTables
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("Usage AccessListTables <input access files path>");
                return;
            }

            if (!Directory.Exists(args[0])) {
                Console.WriteLine("Directory not found");
                return;
            }

            string[] filePaths = Directory.GetFiles(args[0], "*.mdb");
            for (var c = 0; c < filePaths.Count(); c++) {
                string file = filePaths[c];
                string dir = file.Substring(0, file.Length - 4);
                Directory.CreateDirectory(dir);
                string conStr = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + file;
                using (var connection = new OleDbConnection(conStr))
                {
                    DataTable userTables = null;
                    try
                    {
                        // We only want user tables, not system tables
                        string[] restrictions = new string[4];
                        restrictions[3] = "Table";
                        connection.Open();
                        // Get list of user tables
                        userTables = connection.GetSchema("Tables", restrictions);
                        for (int i = 0; i < userTables.Rows.Count; i++)
                        {
                            string tableName = userTables.Rows[i][2].ToString();
                            if (TableExists(connection, tableName))
                            {
                                string selectString = "SELECT * FROM " + tableName;
                                string path = dir + "\\" + tableName + ".csv";
                                TextWriter tw = new StreamWriter(path);
                                DataSet myDataSet = new DataSet();

                                try
                                {
                                    OleDbCommand command = new OleDbCommand(selectString, connection);
                                    OleDbDataAdapter adapter = new OleDbDataAdapter(command);
                                    adapter.Fill(myDataSet, tableName);

                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("Error: Failed to retrieve the required data from the Database.\n{0}", ex.Message);
                                    throw ex;
                                }

                                DataTableCollection dta = myDataSet.Tables;
                                foreach (DataTable dt in dta)
                                {
                                    Console.WriteLine("found data table {0}", dt.TableName);
                                }
                                Console.WriteLine("{0} columns in {1} table", myDataSet.Tables[0].Columns.Count, myDataSet.Tables[0].TableName);
                                DataColumnCollection drc = myDataSet.Tables[0].Columns;
                                string headerString = "";
                                bool writeComma = false;
                                foreach (DataColumn dc in drc) {
                                    if (writeComma) {
                                        headerString += ",  ";
                                    }
                                    else
                                    {
                                        writeComma = true;
                                    }
                                    headerString += "\"" + dc.ColumnName + "\"";
                                }
                                tw.WriteLine(headerString);
                                DataRowCollection dra = myDataSet.Tables[0].Rows;
                                foreach (DataRow dr in dra)
                                {
                                    string rowString = "";
                                    writeComma = false;
                                    foreach (DataColumn dc in myDataSet.Tables[0].Columns) {
                                        if (writeComma)
                                        {
                                            rowString += ", ";
                                        }
                                        else
                                        {
                                            writeComma = true;
                                        }
                                        rowString += "\"" + dr[dc] + "\"";
                                    }
                                    tw.WriteLine(rowString);
                                }
                                tw.Close();
                            }
                        }
                    }
                    catch (OleDbException ex)
                    {
                        Console.WriteLine("ERROR: [file:" + file + "] " + ex.Message);
                    }
                    finally {
                        connection.Close();
                    }
                }
            }
        }

        private static bool TableExists(OleDbConnection connection, string tableName)
        {
            var tables = connection.GetSchema("Tables");
            var tableExists = false;
            for (var i = 0; i < tables.Rows.Count; i++)
            {
                tableExists = String.Equals(tables.Rows[i][2].ToString(),
                                        tableName,
                                        StringComparison.CurrentCultureIgnoreCase);
                if (tableExists)
                    break;
            }
            return tableExists;
        }
    }
}
