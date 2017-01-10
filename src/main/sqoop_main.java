package main;

import net.neoremind.sshxcute.core.Result;
import net.neoremind.sshxcute.exception.TaskExecFailException;
import org.apache.hadoop.hbase.util.RegionSplitter;
import tools.*;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
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
 *
 * 5,运行
 * java -Djava.ext.dirs=/opt/cloudera/parcels/CDH/jars:/usr/java/latest/jre/lib/ext/ -jar sqoop1_import.jar main.sqoop_main /root/config.properties YUNCHEN.MYTABLE
 *需要将oracle和mysql得jar包放到jars目录中或者将sqoop的lib目录加入到上面ext中
 *
 * 6，jar
 *
 * 1，sqoop jar包
 * 2，hadoop-common jar包
 * 3，hadoop-hdfs jar包
 * 4，hadoop-common lib jar包
 * 5，hadoop mapreduce jar包
 * 6，mysql jar包
 * 7，hive lib目录jar包
 * 8，sshxcute jar https://storage.googleapis.com/google-code-archive-downloads/v2/code.google.com/sshxcute/sshxcute-1.0.jar
 *
 */
public class sqoop_main {

    public static void main(String[] args) throws SQLException, TaskExecFailException {

        //1,传入的两个变量，一个properties文件位置，一个是需要抽取的表名
        //注意：实际运行时第一个参数时main类名
        String path = args[1];
        String table_name = args[2];

        //获取mysql元数据库所需的连接的变量
        String username = PropertiesUtils.Get_Properties(path, "username");
        String url = PropertiesUtils.Get_Properties(path, "url");
        String password = PropertiesUtils.Get_Properties(path, "password");
        String selectsql = PropertiesUtils.Get_Properties(path, "selectsql") + "\"" + table_name + "\"";
        String columnname = PropertiesUtils.Get_Properties(path, "columnname");//暂时没什么用
        String targetdir = PropertiesUtils.Get_Properties(path,"targetdir");
        String jdbc_hive = PropertiesUtils.Get_Properties(path,"jdbc_hive");
        String sqoop_server_ip = PropertiesUtils.Get_Properties(path,"sqoop_server_ip");
        String sqoop_server_user = PropertiesUtils.Get_Properties(path,"sqoop_server_user");

        //hdfs集群地址
        String hdfs_address = PropertiesUtils.Get_Properties(path, "hdfs_address");

        //List<String> columnname_list = Arrays.asList(columnname.split(" "));
       /* for(String s:columnname_list)
            System.out.println(s);*/

        //所有的值返回再这个map里面，需要什么就取什么
        Map parafile = DBUtils.get_parafile(url, username, password, selectsql);
        System.out.println(parafile.get("database_link"));

        //开始取值
        String parafile_url = (String) parafile.get("database_link");
        String parafile_username = (String) parafile.get("database_username");
        String parafile_password = (String) parafile.get("database_pwd");
        String parafile_table = (String) parafile.get("table_name");
        String parafile_splitby = (String) parafile.get("sqoop_pri_key");
        String parafile_full_delta = (String) parafile.get("sqoop_delta_full");
        System.out.println("================================"+parafile_full_delta+"===========================");

        //确定表的存储路径，全量和增量
        String hdfs_address_ods = null;
        String partition_name = null;
        if (parafile_full_delta.equals("full")) {
            hdfs_address_ods = hdfs_address + targetdir + table_name.replace(".","_")+"_full";
            System.out.println("表存储路径： "+hdfs_address_ods);
            partition_name = table_name.replace(".","_")+"_full";
            System.out.println("partition的名称： "+partition_name);
        }else if (parafile_full_delta.equals("delta")){
            hdfs_address_ods = hdfs_address + targetdir + table_name.replace(".","_")+"_delta";
            System.out.println(hdfs_address_ods);
            partition_name = table_name.replace(".","_")+"_delta";
            System.out.println("partition的名称： "+partition_name);
        }else{
            System.out.println("增量或者全量名称录入错误，mysql中sqoop_delta_full字段只能为full或者delta");
            System.exit(1);
        }

        //当前时间
        Date d = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String date = sdf.format(d);
        System.out.println("当前时间：" + sdf.format(d));

        //全量抽取

        //暂时放弃这种方法，出现一个未知错误无法解决
        //Sqoop_Full.full_import(parafile_url, parafile_username, parafile_password,
         //       parafile_table, parafile_splitby,hdfs_address_ods, hdfs_address);

        String sqoop_command = "sqoop import --connect jdbc:oracle:thin:@192.168.1.28:1521:xe --username yunchen --password root --table MYTABLE -m 1 --target-dir /33";
        SqoopUtils.importDataUseSSH(sqoop_server_ip,sqoop_server_user,sqoop_command);

        //获取最大值并插入数据库
        String sqoop_max = "sqoop eval --connect jdbc:oracle:thin:@192.168.1.28:1521:xe -" +
                "-username yunchen --password root --query \"select max(INC_DATETIME) from "+table_name+"\"";
        String max = SqoopUtils.SelectMaxUseSSH(sqoop_server_ip,sqoop_server_user,sqoop_max);
        System.out.println("max="+max);
        String insertsql = "INSERT INTO sqoop.table_timestamp VALUES('MYTABLE', '0000-00-00 00:00:00',\""+max+"\")";
        System.out.println("max="+insertsql);
        DBUtils.insert(url,username,password,insertsql);
        //增量抽取

        //导入hvie
        //HiveUtils.AddPartition(jdbc_hive,table_name,hdfs_address_ods,date,partition_name);

    }
}
