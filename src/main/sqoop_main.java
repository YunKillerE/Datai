package main;

import net.neoremind.sshxcute.exception.TaskExecFailException;
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
 *
 */

/**
 * 抽取入口 全量/增量
 */
public class sqoop_main {

    public static void main(String[] args) throws SQLException, TaskExecFailException {

        //当前时间
        Date d = new Date();
        Date yd = new Date(d.getTime() - 86400000L);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        SimpleDateFormat sdf4 = new SimpleDateFormat("yyyyMMddhhmmss");
        String date = sdf.format(d);
        String date2 = sdf2.format(d);
        String date3 = sdf3.format(d);
        String date4 = sdf4.format(d);
        String yesterday = sdf.format(yd);
        String yesterday2 = sdf2.format(yd);
        String yesterday4 = sdf4.format(yd);
        System.out.println("当前时间：" + sdf.format(d));
        System.out.println("昨天时间：" + yesterday);
        System.out.println("date3-------------->："+date3);
        System.out.println("date4-------------->："+date4);

        //1,传入的两个变量，一个properties文件位置，一个是需要抽取的表名
        //注意：实际运行时第一个参数时main类名
        String path = args[1];
        String table_name = args[2];

        //获取mysql元数据库所需的连接的变量
        String username = PropertiesUtils.Get_Properties(path, "username");
        String url = PropertiesUtils.Get_Properties(path, "url");
        String password = PropertiesUtils.Get_Properties(path, "password");
        String selectsql = PropertiesUtils.Get_Properties(path, "selectsql") + "\"" + table_name + "\"";
        String selectlasttime = PropertiesUtils.Get_Properties(path, "selectlasttime") + "\"" + table_name + "\"" ;
        String columnname = PropertiesUtils.Get_Properties(path, "columnname");//暂时没什么用
        String targetdir = PropertiesUtils.Get_Properties(path,"targetdir");
        String jdbc_hive = PropertiesUtils.Get_Properties(path,"jdbc_hive");
        String sqoop_server_ip = PropertiesUtils.Get_Properties(path,"sqoop_server_ip");
        String sqoop_server_user = PropertiesUtils.Get_Properties(path,"sqoop_server_user");
        String sqoop_param_file = PropertiesUtils.Get_Properties(path,"sqoop_param_file");//取消TO_date函数参数文件路径

        //hdfs集群地址
        String hdfs_address = PropertiesUtils.Get_Properties(path, "hdfs_address");

        //List<String> columnname_list = Arrays.asList(columnname.split(" "));
       /* for(String s:columnname_list)
            System.out.println(s);*/

        //所有的值返回再这个map里面，需要什么就取什么
        System.out.println(selectsql);
        System.out.println(url);
        Map parafile = DBUtils.get_parafile(url, username, password, selectsql);
        System.out.println(parafile.get("database_link"));

        //开始取值
        String parafile_url = (String) parafile.get("database_link");
        String parafile_username = (String) parafile.get("database_username");
        String parafile_password = (String) parafile.get("database_pwd");
        String parafile_table = (String) parafile.get("table_name");
        String parafile_splitby = (String) parafile.get("sqoop_pri_key");
        String parafile_full_delta = (String) parafile.get("sqoop_delta_full");
//        String parafile_param_file = (String) parafile.get("sqoop_param_file");//参数文件路径
        String parafile_compress_format = (String) parafile.get("sqoop_compress_format");//压缩格式
        String parafile_storage_format = (String) parafile.get("sqoop_storage_format");//存储格式
        String parafile_map_count = (String) parafile.get("sqoop_map_count");//并行度
        String parafile_map_column_java = (String) parafile.get("sqoop_map_column_java");//需要映射的字段
        String parafile_timestamp = (String) parafile.get("sqoop_timestamp");//时间戳字段
        String parafile_sqoop_time_varchar = (String) parafile.get("sqoop_time_varchar");//是否为varchar类型
        System.out.println("================================"+parafile_full_delta+"===========================");


        //确定表的存储路径，全量和增量
        String hdfs_address_ods = null;
        String partition_name = null;
        if (parafile_full_delta.equals("full")) {
//            hdfs_address_ods = hdfs_address + targetdir + table_name.replace(".","_")+"_full";
            hdfs_address_ods = targetdir + table_name +"/"+date+"_full";
            System.out.println("表存储路径： "+hdfs_address_ods);
            partition_name = "ods."+table_name.replace(".","_")+"_full";
//            partition_name = date;
            System.out.println("partition的名称： "+partition_name);
        }else if (parafile_full_delta.equals("delta")){
//            hdfs_address_ods = hdfs_address + targetdir + table_name.replace(".","_")+"_delta";
            hdfs_address_ods = targetdir + table_name +"/"+date+"_delta";
            System.out.println(hdfs_address_ods);
            partition_name = "ods."+table_name.replace(".","_")+"_delta";
//            partition_name = date;
            System.out.println("partition的名称： "+partition_name);
        }else{
            System.out.println("增量或者全量名称录入错误，mysql中sqoop_delta_full字段只能为full或者delta");
            System.exit(1);
        }

        HdfsUtils.deleteFile(hdfs_address_ods);
//        String sqoop_command = "source /etc/profile;sqoop import --connect jdbc:oracle:thin:@10.120.119.83:1521:police1 --username sjcq_za_szpt --password szpt_za_sjcq --table XXHSZPT.RYJD_YJ_PETON_PROPERSONNEL -m 1 --target-dir /TEST/XXHSZPT.RYJD_YJ_PETON_PROPERSONNEL";
//        String sqoop_command = "sqoop import --connect jdbc:oracle:thin:@192.168.1.28:1521:xe --username sjcq_za_szpt --password szpt_za_sjcq --table XXHSZPT.RYJD_YJ_PETON_PROPERSONNEL -m 1 --target-dir /TEST/XXHSZPT.RYJD_YJ_PETON_PROPERSONNEL";
//        SqoopUtils.importDataUseSSH(sqoop_server_ip,sqoop_server_user,sqoop_command);

        //获取最大值并插入数据库
        String sqoop_max;
        if(parafile_sqoop_time_varchar.equals("yes")){
            sqoop_max = "source /etc/profile;sqoop eval --connect "+ parafile_url
                    +" --username "+parafile_username+" --password "+parafile_password+" --query \"select max("+parafile_timestamp+") from "
                    +table_name+" where "+parafile_timestamp+" < "+date4+" and "+parafile_timestamp+" >= "+yesterday+"000000\"";
        }
        else{
            sqoop_max = "source /etc/profile;sqoop eval --connect "+ parafile_url
                    +" --username "+parafile_username+" --password "+parafile_password+" --query \"select max("+parafile_timestamp+") from "
                    +table_name+" where "+parafile_timestamp+" < TO_DATE('"+date3+"','yyyy-mm-dd hh24:mi:ss') and "+parafile_timestamp+" >= TO_DATE('"+yesterday2+" 00:00:00','yyyy-mm-dd hh24:mi:ss')\"";
        }

        System.out.println(sqoop_max);
        String max = SqoopUtils.SelectMaxUseSSH(sqoop_server_ip,sqoop_server_user,sqoop_max);
        System.out.println("max="+max);
        System.out.println(selectlasttime);
        Map lasttimeMap = DBUtils.get_parafile(url, username, password, selectlasttime);
        String lasttime = (String) lasttimeMap.get("maxtime");
        String insertsql = "UPDATE table_timestamp set maxtime='"+max+"' where table_name = \""+table_name+"\"";
        System.out.println("max="+insertsql);
        DBUtils.insert(url,username,password,insertsql);

        System.out.println(parafile_compress_format);
        System.out.println(parafile_storage_format);

        switch (parafile_full_delta) {
            case "full": {
                //全量抽取
                if (parafile_compress_format.equals("no") && parafile_storage_format.equals("no")) {//压缩方式为no，文件格式为no

                    Sqoop_Full.full_import(parafile_url, parafile_username, parafile_password, parafile_table, parafile_map_count,
                            hdfs_address_ods, sqoop_param_file, parafile_map_column_java,sqoop_server_ip,sqoop_server_user);

                } else if (!parafile_compress_format.equals("no") && parafile_storage_format.equals("no")) { //压缩方式不为no，文件格式为no

                    Sqoop_Full.full_import(parafile_url, parafile_username, parafile_password, parafile_table, parafile_map_count,
                            hdfs_address_ods, parafile_compress_format, sqoop_param_file, parafile_map_column_java, sqoop_server_ip,sqoop_server_user);

                } else if (!parafile_compress_format.equals("no") && !parafile_storage_format.equals("no")) { //压缩方式不为no，文件格式也为no

                    Sqoop_Full.full_import(parafile_url, parafile_username, parafile_password, parafile_table, parafile_map_count,
                            hdfs_address_ods, parafile_compress_format, parafile_storage_format, sqoop_param_file, parafile_map_column_java, sqoop_server_ip,sqoop_server_user);

                }
                //导入hvie
                HiveUtils.AddPartition(jdbc_hive,table_name,hdfs_address_ods,date,partition_name);
            }
            break;
            case "delta":{
                //增量抽取
                if(parafile_sqoop_time_varchar.equals("yes")){

                    Sqoop_Delta.delta_import_varchar(parafile_url,parafile_username,parafile_password,parafile_table,parafile_timestamp,
                            parafile_map_count,hdfs_address_ods,sqoop_param_file,parafile_map_column_java,parafile_splitby,sqoop_server_ip,sqoop_server_user,date,yesterday);

                }else if (parafile_compress_format.equals("no") && parafile_storage_format.equals("no")) {//压缩方式为no，文件格式为no

                    Sqoop_Delta.delta_import(parafile_url,parafile_username,parafile_password,parafile_table,parafile_map_count,
                            hdfs_address_ods,parafile_timestamp,lasttime,sqoop_param_file,parafile_map_column_java,sqoop_server_ip,sqoop_server_user);

                } else if (!parafile_compress_format.equals("no") && parafile_storage_format.equals("no")) { //压缩方式不为no，文件格式为no

                    Sqoop_Delta.delta_import(parafile_url,parafile_username,parafile_password,parafile_table,parafile_map_count,
                            hdfs_address_ods,parafile_timestamp,lasttime,parafile_compress_format,sqoop_param_file,parafile_map_column_java,sqoop_server_ip,sqoop_server_ip);

                } else if (!parafile_compress_format.equals("no") && !parafile_storage_format.equals("no")) { //压缩方式不为no，文件格式也为no

                    Sqoop_Delta.delta_import(parafile_url,parafile_username,parafile_password,parafile_table,parafile_map_count,
                            hdfs_address_ods,parafile_timestamp,lasttime,parafile_compress_format,parafile_storage_format,sqoop_param_file,parafile_map_column_java,sqoop_server_ip,sqoop_server_user);
                }
                HiveUtils.AddPartition(jdbc_hive,table_name,hdfs_address_ods,date,partition_name);
            }
            break;
            default:
                System.out.println("compress or parquert args input error");
                break;
        }

        //

        //导入hvie
//        HiveUtils.AddPartition(jdbc_hive,table_name,hdfs_address_ods,date,partition_name);

    }
}
