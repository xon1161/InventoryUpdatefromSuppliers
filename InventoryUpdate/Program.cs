using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace InventoryUpdate
{
    class Program
    {
        static void Main(string[] args)
        {
            while (1 == 1)
            {
                try
                {
                    X: bool flag = false;
                    //string countRows = "";
                    int counter = 1;
                    string ord = ConfigurationManager.AppSettings["MIIDorder"].ToString();
                    int ik = 1;
                    ClsSqlServer cls = new ClsSqlServer();
                    List<string> MIIDs = new List<string>();
                    //string count = "SELECT COUNT(distinct I.MIID) from tbl_Inventory I inner join tbl_InventorySuppliers S on I.MIID = S.MIID where MONTH(S.Date_Update)<> MONTH(I.Date_Update) and S.updaterStatus in (0, 1, 3) ";
                    //cls.connect_SQLServer();
                    //SqlDataReader counts=null;
                    //try
                    //{
                    //    counts = cls.execute_ReturnRecord(count, 3000);

                    //    while (counts != null && counts.Read())
                    //    {
                    //        countRows = counts["counts"].ToString();
                    //    }
                    //}
                    //catch (Exception w)
                    //{
                    //    counts.Close();
                    //    cls.close_SQLServer();
                    //    goto X;
                    //}
                    string sql = "select MIID from tbl_Inventory where MIID in(select MIID from tbl_InventorySuppliers where updaterStatus in (0,1,3) and Availability=0 " +
                        "except select MIID from tbl_InventorySuppliers where updaterStatus in (0,1,3) and Availability<>0) and Availability<>0";
                    cls.close_SQLServer();
                    cls.connect_SQLServer();
                    SqlDataReader reader = cls.execute_ReturnRecord(sql, 1000);
                    while (reader != null && reader.Read())
                    {
                        try
                        {
                            string MIID = reader["MIID"].ToString();
                            MIIDs.Add(MIID);
                            if (counter == 500)
                            {
                                List<SqlParameter> param = new List<SqlParameter>();
                                param.Add(new SqlParameter("@MIIDs", string.Join(",", MIIDs)));
                                DataSet s = new DataSet();
                                //object data = null;
                                //data = cls.exceuteSP_ReturnScalar("sp_Update_Inventory_Tables_Updaters", param, 600);
                                //if(data.ToString()=="1")
                                //    Console.WriteLine(ik + ". SCANNED 500 MIIDs");
                                //else
                                //    Console.WriteLine("Error occured");
                                s = cls.exceuteSP_ReturnDataSet("sp_Update_Inventory_Tables_Updaters_debug", param, 600);
                                MIIDs.Clear();
                                counter = 0;
                                if (s.Tables[5].Rows[0][0].ToString()=="1")
                                    Console.WriteLine(ik + ". SCANNED 500 MIIDs");
                                else
                                    Console.WriteLine("Error occured");
                                ik++;
                            }
                        }
                        catch (Exception s)
                        {
                            reader.Close();
                            flag = true;
                            cls.close_SQLServer();
                            counter = 0;
                            break;
                        }
                        counter++;
                    }
                    if (flag)
                    {
                        goto X;
                    }
                    if (!reader.IsClosed)
                        reader.Close();
                    cls.close_SQLServer();
                    Thread.Sleep(600000);
                }
                catch (Exception w)
                { }
            }
        }
    }
}
