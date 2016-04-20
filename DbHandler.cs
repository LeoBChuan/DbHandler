using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace COMS.Data
{
    public class DbHandler
    {
        #region 查询

        /// <summary>
        /// 根据连接字符串和SQL语句获取表数据
        /// </summary>
        /// <param name="connStr">连接字符串</param>
        /// <param name="sqlStr">SQL语句</param>
        /// <returns></returns>
        public static DataTable GetDataTable(string connStr, string sqlStr)
        {
            DataTable dt = new DataTable();

            using (SqlConnection con = new SqlConnection(connStr))
            {
                try
                {
                    con.Open();
                    SqlDataAdapter sda = new SqlDataAdapter(sqlStr, connStr);
                    DataSet ds = new DataSet();
                    sda.Fill(ds);
                    dt = ds.Tables[0];
                    con.Close();
                }
                catch (Exception ex)
                {
                    ex.ToString();
                    return dt;
                }
            }

            return dt;
        }

        /// <summary>
        /// 获取第n到m条数据
        /// </summary>
        /// <param name="connStr">连接字符串</param>
        /// <param name="start">起始条数</param>
        /// <param name="length">需要查询的条数</param>
        /// <param name="tableName">数据库表名</param>
        /// <param name="strSelect">需要查询的字段</param>
        /// <param name="strWhere">查询条件</param>
        /// <param name="strOrder">排序</param>
        /// <returns></returns>
        public static DataTable GetDataTable(string connStr, int start, int length, string tableName, string strSelect, string strWhere, string strOrder)
        {
            DataTable dt = new DataTable();
            start = start < 0 ? 0 : start;
            length = length < 1 ? 10 : length;

            using (SqlConnection con = new SqlConnection(connStr))
            {
                try
                {
                    con.Open();
                    int maxNum = start + length;
                    int minNUm = start;
                    string subSqlStr = strSelect + " from " + tableName;
                    if (!string.IsNullOrEmpty(strWhere))
                        subSqlStr += " where " + strWhere;
                    string sqlStr = "select TOP(" + maxNum + ") " + subSqlStr + " except select TOP(" + minNUm + ") " + subSqlStr
                        + " order by " + strOrder;

                    SqlDataAdapter sda = new SqlDataAdapter(sqlStr, connStr);
                    DataSet ds = new DataSet();
                    sda.Fill(ds);
                    dt = ds.Tables[0];
                    con.Close();
                }
                catch (Exception ex)
                {
                    ex.ToString();
                    return dt;
                }
            }

            return dt;
        }

        /// <summary>
        /// 获取记录条数
        /// </summary>
        /// <param name="connStr">连接字符串</param>
        /// <param name="tableName">数据库表名</param>
        /// <param name="strWhere">查询条件</param>
        /// <returns></returns>
        public static int GetCount(string connStr, string tableName, string strWhere)
        {
            int rowNum = 0;

            using (SqlConnection con = new SqlConnection(connStr))
            {
                try
                {
                    con.Open();
                    string sqlStr = "select COUNT(ID) from " + tableName;
                    if (!string.IsNullOrEmpty(strWhere))
                        sqlStr += " where " + strWhere;
                    SqlCommand cmd = new SqlCommand(sqlStr, con);
                    rowNum = Int32.Parse(cmd.ExecuteScalar().ToString());
                    con.Close();
                }
                catch (Exception ex)
                {
                    ex.ToString();
                    return 0;
                }
            }

            return rowNum;
        }

        #endregion 查询


        #region 增删改

        /// <summary>
        /// 直接执行SQL语句，返回受影响的条数 
        /// </summary>
        /// <param name="connStr">连接字符串</param>
        /// <param name="sqlStr">SQL语句</param>
        /// <returns></returns>
        public static int ExecuteSql(string connStr, string sqlStr)
        {
            int rowNum = 0;
            using (SqlConnection con = new SqlConnection(connStr))
            {
                using (SqlCommand cmd = new SqlCommand(sqlStr, con))
                {
                    try
                    {
                        con.Open();
                        rowNum = cmd.ExecuteNonQuery();
                        con.Close();
                    }
                    catch (Exception ex)
                    {
                        ex.ToString();
                        return 0;
                    }
                }
            }

            return rowNum;
        }

        /// <summary>
        /// 增加记录
        /// </summary>
        /// <param name="connStr">连接字符串</param>
        /// <param name="tableName">数据库表名</param>
        /// <param name="columns">字段名</param>
        /// <param name="values">值 </param>
        /// <returns></returns>
        public static int AddItems(string connStr, string tableName, string columns, string values)
        {
            int rowNum = 0;
            using (SqlConnection con = new SqlConnection(connStr))
            {
                try
                {
                    con.Open();
                    string sqlStr = "insert into " + tableName + " (" + columns + ") values (" + values + ")";
                    SqlCommand cmd = new SqlCommand(sqlStr, con);
                    rowNum = cmd.ExecuteNonQuery();
                    con.Close();
                }
                catch (Exception ex)
                {
                    ex.ToString();
                    return 0;
                }
            }

            return rowNum;
        }

        /// <summary>
        /// 执行更新记录
        /// </summary>
        /// <param name="connStr">连接字符串</param>
        /// <param name="tableName">数据库表名</param>
        /// <param name="strUpdate">更新语句</param>
        /// <param name="strWhere">更新条件</param>
        /// <returns></returns>
        public static int UpdateItems(string connStr, string tableName, string strUpdate, string strWhere)
        {
            int rowNum = 0;
            using (SqlConnection con = new SqlConnection(connStr))
            {
                try
                {
                    con.Open();
                    string sqlStr = "update " + tableName + " set " + strUpdate;
                    if (!string.IsNullOrEmpty(strWhere))
                        sqlStr += " where " + strWhere;
                    SqlCommand cmd = new SqlCommand(sqlStr, con);
                    rowNum = cmd.ExecuteNonQuery();
                    con.Close();
                }
                catch (Exception ex)
                {
                    ex.ToString();
                    return 0;
                }
            }

            return rowNum;
        }

        /// <summary>
        /// 删除记录
        /// </summary>
        /// <param name="connStr">连接字符串</param>
        /// <param name="tableName">数据库表名</param>
        /// <param name="strWhere">条件字符串</param>
        /// <param name="delFlag">删除标记(true为真删除，false为假删除)</param>
        /// <returns>收影响的条数</returns>
        public static int DeleteItems(string connStr, string tableName, string strWhere, bool delFlag)
        {
            int rowNum = 0;
            using (SqlConnection con = new SqlConnection(connStr))
            {
                try
                {
                    con.Open();
                    string sqlStr = string.Empty;
                    if (delFlag)
                    {
                        sqlStr = "delete from " + tableName;
                    }
                    else
                    {
                        sqlStr = "update " + tableName + " set DelFlag = 1 ";
                    }
                    if (!string.IsNullOrEmpty(strWhere))
                        sqlStr += " where " + strWhere;
                    SqlCommand cmd = new SqlCommand(sqlStr, con);
                    rowNum = cmd.ExecuteNonQuery();
                    con.Close();
                }
                catch (Exception ex)
                {
                    ex.ToString();
                    return 0;
                }
            }

            return rowNum;
        }

        #endregion 增删改


        #region 存储过程

        /// <summary>
        /// 执行存储过程
        /// </summary>
        /// <param name="connStr">连接字符串</param>
        /// <param name="procName">存储过程名</param>
        /// <param name="values">参数</param>
        /// <returns>执行结果DataTable</returns>
        public static DataTable GetDatatable(string connStr, string procName, params SqlParameter[] values)
        {
            DataTable dt = new DataTable();
            using (SqlConnection con = new SqlConnection(connStr))
            {
                try
                {
                    con.Open();
                    using (SqlCommand cmd = new SqlCommand())
                    {
                        cmd.Connection = con;
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandText = procName;
                        cmd.Parameters.AddRange(values);
                        SqlDataAdapter sda = new SqlDataAdapter(cmd);
                        DataSet ds = new DataSet();
                        sda.Fill(ds);
                        dt = ds.Tables[0];
                    }
                    con.Close();
                }
                catch (Exception ex)
                {
                    ex.ToString();
                    return dt;
                }
            }
            return dt;
        }

        /// <summary>
        /// 执行存储过程
        /// </summary>
        /// <param name="connStr">连接字符串</param>
        /// <param name="procName">存储过程名</param>
        /// <param name="values">参数</param>
        /// <returns>存储过程执行结果对象</returns>
        public static object GetObject(string connStr, string procName, params SqlParameter[] values)
        {
            object obj = new object();
            using (SqlConnection con = new SqlConnection(connStr))
            {
                try
                {
                    con.Open();
                    using (SqlCommand cmd = new SqlCommand())
                    {
                        cmd.Connection = con;
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandText = procName;
                        cmd.Parameters.AddRange(values);
                        obj = cmd.ExecuteScalar();
                    }
                    con.Close();
                }
                catch (Exception ex)
                {
                    ex.ToString();
                    return null;
                }
            }
            return obj;
        }

        /// <summary>
        /// 执行存储过程
        /// </summary>
        /// <param name="connStr">连接字符串</param>
        /// <param name="procName">存储过程名</param>
        /// <param name="values">参数</param>
        /// <returns>存储过程执行结果对象</returns>
        public static int GetCount(string connStr, string procName, params SqlParameter[] values)
        {
            int rowNum = 0;
            using (SqlConnection con = new SqlConnection(connStr))
            {
                try
                {
                    con.Open();
                    using (SqlCommand cmd = new SqlCommand())
                    {
                        cmd.Connection = con;
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandText = procName;
                        cmd.Parameters.AddRange(values);
                        rowNum = cmd.ExecuteNonQuery();
                    }
                    con.Close();
                }
                catch (Exception ex)
                {
                    ex.ToString();
                    return 0;
                }
            }
            return rowNum;
        }

        #endregion 存储过程
    }
}
