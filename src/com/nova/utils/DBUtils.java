package com.nova.utils;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class DBUtils {
/*    private static String url = "jdbc:mysql://localhost:3306/mydb";
    private static String user = "root";
    private static String psw = "root";*/

    private static Connection conn;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private DBUtils() {

    }

    /**
     * 获取数据库的连接
     *
     * @return conn
     */
    public static Connection getConnection(String url, String username, String password) {
        if (null == conn) {
            try {
                conn = DriverManager.getConnection(url, username, password);
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        return conn;
    }

    /**
     * 释放资源
     *
     * @param conn
     * @param pstmt
     * @param rs
     */
    public static void closeResources(Connection conn, Statement pstmt, ResultSet rs) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                if (null != pstmt) {
                    try {
                        pstmt.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    } finally {
                        if (null != conn) {
                            try {
                                conn.close();
                            } catch (SQLException e) {
                                e.printStackTrace();
                                throw new RuntimeException(e);
                            }
                        }
                    }
                }
            }
        }
    }

    public static Map<Object, Object> get_parafile(String url, String username, String password, String select_sql) {


        Statement statement = null;
        Connection conn = null;
        ResultSet rs = null;

        try {
             conn = DBUtils.getConnection(url, username, password);
            if (!conn.isClosed()) {
                System.out.println("Succeeded connecting to the Database!");
                 statement = conn.createStatement();
                String selectsql = select_sql;
                 rs = statement.executeQuery(selectsql);
                ResultSetMetaData rData = rs.getMetaData();
                List<Object> list = new ArrayList<Object>();
                Map<Object, Object> obj = new HashMap<Object, Object>();
                while (rs.next()) {
                    for (int i = 1; i <= rData.getColumnCount(); i++) {
                        obj.put(rData.getColumnName(i).toLowerCase(), rs.getObject(i));
                        //List<String> result = new ArrayList<String>(obj.values());
                    }
                    //list.add(obj.values());
                    //list.add(obj);
                }
                return obj;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try{rs.close();}catch(SQLException ex){}
            try{statement.close();}catch(SQLException ex){}
            try{conn.close();}catch(SQLException ex){}
        }
        return null;
    }

    public static Map<Object, Object> insert(String url, String username, String password, String insert_sql) {

        Statement statement = null;
        Connection conn = null;
        ResultSet rs = null;

        try {
             conn = DBUtils.getConnection(url, username, password);
            if (!conn.isClosed()) {
                System.out.println("Succeeded connecting to the Database!");
                 statement = conn.createStatement();
                statement.execute(insert_sql);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try{statement.close();}catch(SQLException ex){}
            try{conn.close();}catch(SQLException ex){}
        }
        return null;
    }

}
