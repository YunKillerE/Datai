package main;

import tools.DBUtils;
import tools.PropertiesUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by yunchen on 2016/12/27.
 * <p>
 * 功能：
 * 1，mysql连接函数
 * 2，环境检查函数
 * 3，全量抽取函数
 * 4，增量抽取函数
 * 5，合并函数
 * <p>
 * 思路：
 * 1，判断数据链接是否正常？这里判断主机是否可通，账号密码是否对，是否存在给定的表，直接发送一次查询请求就可以了，
 * 如果失败就直接退出，记录报错
 * 2，如果是全量抽取，直接执行就好，不过这里注意，是否需要先删除以当天日期命名的目录
 * 3，如果是增量抽取，首次抽取需要获取最大值和最小值，并把最大值记录到一个文件，第二次开始需要从上一个文件里面获取
 * 上一次的最大值作为此次的最小值，再通过sql去查询最大值，再执行增量抽取任务
 * 4，全量
 * <p>
 * 想法：
 * 1，每个函数都包含列式存储Parquet与压缩lzo的选择
 * 2，尽量分层，借鉴spring的架构
 */
public class sqoop_main {

    public static void main(String[] args) {

        //传入的两个变量，一个properties文件位置，一个是需要抽取的表名
        String path = args[1];
        String table_name = args[2];

        //获取数据库所需的连接的变量
        String username = PropertiesUtils.Get_Properties(path, "username");
        String url = PropertiesUtils.Get_Properties(path, "url");
        String password = PropertiesUtils.Get_Properties(path, "password");
        String selectsql = PropertiesUtils.Get_Properties(path, "selectsql")+"\""+table_name+"\"";
        String columnname = PropertiesUtils.Get_Properties(path, "columnname");//暂时没什么用

        //hdfs集群地址
        String hdfs_address = PropertiesUtils.Get_Properties(path,"hdfs_address");
        System.out.println(hdfs_address);

        //List<String> columnname_list = Arrays.asList(columnname.split(" "));
       /* for(String s:columnname_list)
            System.out.println(s);*/

        //所有的值返回再这个map里面，需要什么就取什么
        Map ss = DBUtils.get_parafile(url,username,password,selectsql);
        System.out.print(ss.get("database_link"));


    }
}
