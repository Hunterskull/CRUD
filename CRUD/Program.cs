using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Sql;
using System.Data.SqlClient;
using Newtonsoft.Json;

namespace CRUD
{
    class Program
    {
        static void Main(string[] args)
        {
            string connectionstring = "Data Source=.\\LUCASSQLSERVER;Initial Catalog=master;Integrated Security=True";
            string connectionstringDB = "Data Source=.\\LUCASSQLSERVER;Initial Catalog=CSharpTest;Integrated Security=True";
            
            string sql = "CREATE DATABASE CSharpTest";
            string sqlCreateContinents = "CREATE TABLE dbo.Continents(ContinentName nvarchar(100), CountryId bigint, Population bigint)";
            string sqlCreateJson = "CREATE TABLE ContJson(Info nvarchar(max))";
            string insertContinents1 = "INSERT INTO Continents(ContinentName, CountryId, Population) VALUES('Europe', 32, 10400000)";
            string insertContinents2 = "INSERT INTO Continents(ContinentName, CountryId, Population) VALUES('Europe', 31, 17180000)";
            string updateContinentsQuery = "UPDATE Continents SET Population = 11400000 where CountryId = 32";
            string deleteFromQuery1 = "DELETE FROM Continents";
            string deleteFromQuery2 = "DELETE FROM ContJson";
            string drop1 = "DROP TABLE Continents";
            string drop2 = "DROP TABLE ContJson";
            


            SqlConnection conn = new SqlConnection(connectionstring);
            SqlConnection connCSharp = new SqlConnection(connectionstringDB);
            SqlCommand createDB = new SqlCommand(sql, conn);
            SqlCommand createContinets = new SqlCommand(sqlCreateContinents, connCSharp);
            SqlCommand createJson = new SqlCommand(sqlCreateJson, connCSharp);
            SqlCommand fillContinents1 = new SqlCommand(insertContinents1, connCSharp);
            SqlCommand fillContinents2 = new SqlCommand(insertContinents2, connCSharp);
            SqlCommand updatecontinets = new SqlCommand(updateContinentsQuery, connCSharp);
            SqlCommand deleteFrom1 = new SqlCommand(deleteFromQuery1, connCSharp);
            SqlCommand deleteFrom2 = new SqlCommand(deleteFromQuery2, connCSharp);
            SqlCommand dropTable1 = new SqlCommand(drop1, connCSharp);
            SqlCommand dropTable2 = new SqlCommand(drop2, connCSharp);
            
            try
            {

                conn.Open();
                createDB.ExecuteNonQuery();
                conn.Close();
                connCSharp.Open();
                createContinets.ExecuteNonQuery();
                createJson.ExecuteNonQuery();
                fillContinents1.ExecuteNonQuery();
                fillContinents2.ExecuteNonQuery();
                updatecontinets.ExecuteNonQuery();
                SaveRecordsToJsonTable();
                deleteFrom1.ExecuteNonQuery();
                deleteFrom2.ExecuteNonQuery();
                dropTable1.ExecuteNonQuery();
                dropTable2.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                
            }
            finally
            {
                connCSharp.Close();
            }

        }
        
        public static void SaveRecordsToJsonTable()
        {
            DataTable dt = new DataTable();
            using (SqlConnection con = new SqlConnection("Data Source=.\\LUCASSQLSERVER;Initial Catalog=CSharpTest;Integrated Security=True"))
            {
                string insertJsonQuery = "Insert into ContJson(Info) values (@Jsonstring)";
                using (SqlCommand cmd = new SqlCommand(@"SELECT [ContinentName],[CountryId],[Population]
                FROM[Continents]", con))
                {
                    con.Open();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(dt);
                    
                    List<Dictionary<string, object>> rows = new List<Dictionary<string, object>>();
                    Dictionary<string, object> row;
                    foreach (DataRow dr in dt.Rows)
                    {
                        row = new Dictionary<string, object>();
                        foreach (DataColumn col in dt.Columns)
                        {
                            row.Add(col.ColumnName, dr[col]);
                        }
                        var jsonString = JsonConvert.SerializeObject(row);
                        using (SqlCommand insertCommand = new SqlCommand(insertJsonQuery, con))
                        {
                            var jsonPar = new SqlParameter("Jsonstring", SqlDbType.NVarChar);
                            jsonPar.Value = jsonString;
                            insertCommand.Parameters.Add(jsonPar);
                            insertCommand.ExecuteNonQuery();
                        }
                    }
                }
            }
        }
    }
}


