using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

public class ClsSqlServer
{
    public string connStr;
    private SqlConnection sqlConn;
    private SqlTransaction sqlTran;

    public void BeginTransaction()
    {
        this.sqlTran = this.sqlConn.BeginTransaction();
    }

    public bool close_SQLServer()
    {
        try
        {
            this.sqlConn.Close();
            return true;
        }
        catch (Exception exception)
        {
            Console.WriteLine(exception.ToString());
            return false;
        }
    }

    public void CompleteTransaction()
    {
        this.sqlTran.Commit();
    }

    public bool connect_SQLServer()
    {
        try
        {
            this.connStr = @"server=xoncust.database.windows.net;uid=xonuser;pwd=_xon1161;database=CustomerSite;MultipleActiveResultSets=True";
            this.sqlConn = new SqlConnection(this.connStr);
            this.sqlConn.Open();
            return true;
        }
        catch (Exception exception)
        {
            Console.WriteLine(exception.ToString());
            return false;
        }
    }
    public bool connect_SQLServer_Remote()
    {
        try
        {
            this.connStr = @"server=203.142.136.58;uid=xonuser;pwd=_xon1161;database=ClearVision;MultipleActiveResultSets=True";
            this.sqlConn = new SqlConnection(this.connStr);
            this.sqlConn.Open();
            return true;
        }
        catch (Exception exception)
        {
            Console.WriteLine(exception.ToString());
            return false;
        }
    }
    public bool execute_DML(string sql_In)
    {
        bool flag;
        try
        {
            new SqlCommand(sql_In, this.sqlConn) { Transaction = this.sqlTran }.ExecuteNonQuery();
            flag = true;
        }
        catch (Exception exception)
        {
            Console.WriteLine("Ex: " + exception.ToString());
            flag = false;
        }
        return flag;
    }

    public DataTable execute_DT(string sql, int timeout = 0)
    {
        DataTable dt = new DataTable();
        SqlCommand command;
        SqlDataAdapter sqlAd;
        try
        {
            command = new SqlCommand(sql, this.sqlConn);
            command.CommandTimeout = timeout;
            sqlAd = new SqlDataAdapter(command);
            sqlAd.Fill(dt);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
            sqlAd = null;
        }
        sqlAd = null;
        return dt;
    }

    public SqlDataReader execute_ReturnRecord(string sql_In,int timeout=0)
    {
        SqlCommand command;
        SqlDataReader reader;
        try
        {
            if (timeout > 0)
            {
                command = new SqlCommand(sql_In, this.sqlConn);
                command.CommandTimeout = timeout;
                if (this.sqlTran != null)
                {
                    command.Transaction = this.sqlTran;
                }
                reader = command.ExecuteReader();
            }
            else
            {
                command = new SqlCommand(sql_In, this.sqlConn);
                if (this.sqlTran != null)
                {
                    command.Transaction = this.sqlTran;
                }
                reader = command.ExecuteReader();
            }
        }
        catch (Exception exception)
        {
            Console.WriteLine(exception.ToString());
            reader = null;

        }
        command = null;
        return reader;
    }
    public object exceuteSP_ReturnScalar(string procedureName, List<SqlParameter> parameters, int timeout = 0)
    {
        var data = new object();
        using (SqlCommand sqlCommand = new SqlCommand(procedureName, sqlConn))
        {
            sqlCommand.CommandType = CommandType.StoredProcedure;
            if (timeout > 0)
                sqlCommand.CommandTimeout = timeout;
            if (parameters != null)
            {
                sqlCommand.Parameters.AddRange(parameters.ToArray());
                data = sqlCommand.ExecuteScalar();
            }
        }
        return data;

    }

    public DataSet exceuteSP_ReturnDataSet(string procedureName, List<SqlParameter> parameters, int timeout = 0)
    {
        DataSet data = new DataSet();
        using (SqlCommand sqlCommand = new SqlCommand(procedureName, sqlConn))
        {
            sqlCommand.CommandType = CommandType.StoredProcedure;
            if (timeout > 0)
                sqlCommand.CommandTimeout = timeout;
            if (parameters != null)
            {
                sqlCommand.Parameters.AddRange(parameters.ToArray());
                SqlDataAdapter adapter = new SqlDataAdapter(sqlCommand);
                adapter.Fill(data);
                adapter = null;
            }
        }
        return data;

    }
}

