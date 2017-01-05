package tools;

/**
 * Created by yunchen on 2017/1/4.
 */

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class HiveUtils {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void AddPartition(String jdbc_hive,String TableName,String HdfsFilePath,String date,
                                    String partition_name) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        //Connection con = DriverManager.getConnection("jdbc:hive2://10.10.11.201:10000/default", "", "");
        Connection con = DriverManager.getConnection(jdbc_hive, "", "");
        Statement stmt = con.createStatement();
        String tableName = TableName;
        String filepath = HdfsFilePath;
        ResultSet res;

        //String sql = "load data local inpath '" + filepath + "' into table " + tableName;
/*        Date d = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        System.out.println("当前时间：" + sdf.format(d));*/

        String showsql = "show partitions " + partition_name;

        res = stmt.executeQuery(showsql);

        while (res.next()) {
            System.out.println(res.getString(1));
        }

        //alter table "$table_name3"_full drop partition \(dt=\"$3\"\)\
        //取消删除操作，因为这会同时删除hdfs上的目录，貌似删除操作也没有太多的作用
        //String sql1 = "alter table "+tableName+" drop partition"+" (dt=\""+sdf.format(d)+"\")";

        //如果分区存在，就不会增加分区，正常情况也应该是这样的，如果分区就应该指向了正确的目录
        //alter table "$table_name3"_full add partition \(dt=\""$3"\"\) location \"$HDFS_ADDRESS/$ROOT_DIRECTORY/$2/"$3"_full\"\
        String sql2 = "alter table " + partition_name + " add IF NOT EXISTS partition" +
                " (dt=\"" + date + "\")" + " location \"" + filepath + "\"";

        //System.out.println("Running: " + sql1);
        System.out.println("Running: " + sql2);
        //stmt.execute(sql1);
        stmt.execute(sql2);

        //查询测试语句
/*        String sql = "select * from " + tableName + " where dt='" + date + "'";
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }*/

    }


}
