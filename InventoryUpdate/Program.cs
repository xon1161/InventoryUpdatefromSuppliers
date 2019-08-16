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
            int iteration = 1;
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
                    if (iteration % 3 == 0)
                    {
                        Console.WriteLine("Now starting IDs whose InventorySuppliers AVAILABILITY IS ZERO BUT Inventory is NON-ZERO");
                        string sql = "select Distinct I.MIID from tbl_Inventory I inner join tbl_InventorySuppliers S on I.MIID=S.MIID where S.Availability=0 and S.updaterStatus<>2 and I.Availability>0";//and updaterStatus in (2)
                        if (cls.connStr != null)
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
                                    //param.Add(new SqlParameter("@executeBy", 1));
                                    //DataSet s = new DataSet();
                                    object data = null;
                                    data = cls.exceuteSP_ReturnScalar("sp_Update_Inventory_Tables_Updaters", param, 600);
                                    if (data.ToString() == "1")
                                        Console.WriteLine(ik + ". SCANNED 500 MIIDs");
                                    else
                                        Console.WriteLine("Error occured");
                                    //s = cls.exceuteSP_ReturnDataSet("sp_Update_Inventory_Tables_Updaters_debug", param, 600);
                                    MIIDs.Clear();
                                    counter = 0;
                                    //if (s.Tables[1].Rows[0][0].ToString() == "1")
                                    //    Console.WriteLine(ik + ". SCANNED 500 MIIDs");
                                    //else
                                    //    Console.WriteLine("Error occured");
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
                    else if (iteration % 3 == 1) 
                    {
                        Console.WriteLine("Now starting IDs whose Inventory AVAILABILITY IS ZERO BUT InventorySuppliers is NON-ZERO");
                        string sql = "select distinct I.MIID from tbl_Inventory I inner join tbl_InventorySuppliers S on I.MIID=S.MIID where S.updaterStatus in (0,1,3) and S.Availability<>0 and I.Availability=0";
                        if(cls.connStr!=null)
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
                                    //param.Add(new SqlParameter("@executeBy", 1));
                                    //DataSet s = new DataSet();
                                    object data = null;
                                    data = cls.exceuteSP_ReturnScalar("sp_Update_Inventory_Tables_Updaters", param, 600);
                                    if (data.ToString() == "1")
                                        Console.WriteLine(ik + ". SCANNED 500 MIIDs");
                                    else
                                        Console.WriteLine("Error occured");
                                    //s = cls.exceuteSP_ReturnDataSet("sp_Update_Inventory_Tables_Updaters_debug", param, 600);
                                    MIIDs.Clear();
                                    counter = 0;
                                    //if (s.Tables[1].Rows[0][0].ToString() == "1")
                                    //    Console.WriteLine(ik + ". SCANNED 500 MIIDs");
                                    //else
                                    //    Console.WriteLine("Error occured");
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
                    else if (iteration % 3 == 2)
                    {
                        Console.WriteLine("Now starting IDs whose Inventory Date is less than 7 days old but not in suppliers");
                        string sql = "select DISTINCT I.MIID from tbl_Inventory I inner join tbl_InventorySuppliers S on I.MIID=S.MIID where S.updaterStatus in (0,1,3) and CAST(S.Date_Update as DATE)>=CAST(DATEADD(dd,-7,getdate()) as DATE) and  CAST(I.Date_Update as DATE)<CAST(DATEADD(dd,-7,getdate()) as DATE)";
                        if (cls.connStr != null)
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
                                    //param.Add(new SqlParameter("@executeBy", 1));
                                    //DataSet s = new DataSet();
                                    object data = null;
                                    data = cls.exceuteSP_ReturnScalar("sp_Update_Inventory_Tables_Updaters", param, 600);
                                    if (data.ToString() == "1")
                                        Console.WriteLine(ik + ". SCANNED 500 MIIDs");
                                    else
                                        Console.WriteLine("Error occured");
                                    //s = cls.exceuteSP_ReturnDataSet("sp_Update_Inventory_Tables_Updaters_debug", param, 600);
                                    MIIDs.Clear();
                                    counter = 0;
                                    //if (s.Tables[1].Rows[0][0].ToString() == "1")
                                    //    Console.WriteLine(ik + ". SCANNED 500 MIIDs");
                                    //else
                                    //    Console.WriteLine("Error occured");
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
                }
                catch (Exception w)
                { }
                iteration++;
            }
        }
    }
}
