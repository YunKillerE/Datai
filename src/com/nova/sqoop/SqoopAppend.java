package com.nova.sqoop;

import net.neoremind.sshxcute.exception.TaskExecFailException;
import com.nova.utils.*;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by shenly on 2017/3/3.
 */
public class SqoopAppend {
    public void append(String path , String tableName , Map<String , String> commMap, Map tableMap){
        //当前时间
        Date d = new Date();
        Date yd = new Date(d.getTime() - 86400000L);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
        String date = sdf.format(d);
        String yesterday = sdf.format(yd);
        String date2 = sdf2.format(d);
        String yesterday2 = sdf2.format(yd);
        System.out.println("当前时间：" + sdf.format(d));
        System.out.println("昨天时间：" + yesterday);

        //获取mysql元数据库所需的连接的变量
        String username = PropertiesUtils.Get_Properties(path, "username");
        String url = PropertiesUtils.Get_Properties(path, "url");
        String password = PropertiesUtils.Get_Properties(path, "password");
        String selectsql = PropertiesUtils.Get_Properties(path, "selectsql") + "\"" + tableName + "\"";
        String selectlasttime = PropertiesUtils.Get_Properties(path, "selectlasttime") + "\"" + tableName + "\"";
        String columnname = PropertiesUtils.Get_Properties(path, "columnname");//暂时没什么用
        String targetdir = PropertiesUtils.Get_Properties(path,"targetdir");
        String jdbc_hive = PropertiesUtils.Get_Properties(path,"jdbc_hive");
        String sqoop_server_ip = PropertiesUtils.Get_Properties(path,"sqoop_server_ip");
        String sqoop_server_user = PropertiesUtils.Get_Properties(path,"sqoop_server_user");
        String sqoop_server_pwd = PropertiesUtils.Get_Properties(path,"sqoop_server_pwd");
        String sqoop_param_file = PropertiesUtils.Get_Properties(path,"sqoop_param_file");//取消TO_date函数参数文件路径

        //hdfs集群地址
        String hdfs_address = PropertiesUtils.Get_Properties(path, "hdfs_address");

        //List<String> columnname_list = Arrays.asList(columnname.split(" "));
       /* for(String s:columnname_list)
            System.out.println(s);*/

        //所有的值返回再这个map里面，需要什么就取什么
        System.out.println(selectsql);
        Map parafile = DBUtils.get_parafile(url, username, password, selectsql);
        System.out.println(parafile.get("database_link"));

        //开始取值
        String parafile_url = (String) parafile.get("database_link");
        String parafile_username = (String) parafile.get("database_username");
        String parafile_password = (String) parafile.get("database_pwd");
        String parafile_table = (String) parafile.get("table_name");
        String parafile_splitby = (String) parafile.get("sqoop_pri_key");//主键
        String parafile_full_delta = (String) parafile.get("sqoop_delta_full");
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
        String hdfs_address_ods_full = null;
        String partition_name_full = null;

        //这部分有变动，带检验
        hdfs_address_ods = targetdir + tableName +"/"+date+"_delta";
        System.out.println(hdfs_address_ods);
        partition_name = "ods."+tableName.replace(".","_")+"_delta";
        System.out.println("partition的名称： "+partition_name);
        hdfs_address_ods_full =  targetdir + tableName +"/"+date+"_full";
        partition_name_full = "ods."+tableName.replace(".","_")+"_full";
        System.out.println(hdfs_address_ods_full);
        System.out.println("partition的名称： "+partition_name_full);



        HdfsUtils.deleteFile(targetdir + tableName +"/"+date+"_delta");
        String sqoop_append;
        if(parafile_sqoop_time_varchar.equals("no"))
        {
            if (parafile_compress_format.equals("no") && parafile_storage_format.equals("no")){
                if(parafile_map_column_java.equals("no")) {
                    sqoop_append = "source /etc/profile;sqoop import --connect " + parafile_url + " --username " + parafile_username
                            + " --password " + parafile_password + " --query \"SELECT * from " + parafile_table + " where " + parafile_timestamp
                            + " >= TO_DATE('" + yesterday2 + " 00:00:00','yyyy-mm-dd hh24:mi:ss') and " + parafile_timestamp + " < TO_DATE('" + date2 + " 00:00:00','yyyy-mm-dd hh24:mi:ss') and \\$CONDITIONS\" -m "
                            + parafile_map_count + " --target-dir " + hdfs_address_ods + " --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file "
                            + sqoop_param_file  + " --split-by " + parafile_splitby;
                }else{
                    sqoop_append = "source /etc/profile;sqoop import --connect " + parafile_url + " --username " + parafile_username
                            + " --password " + parafile_password + " --query \"SELECT * from " + parafile_table + " where " + parafile_timestamp
                            + " >= TO_DATE('" + yesterday2 + " 00:00:00','yyyy-mm-dd hh24:mi:ss') and " + parafile_timestamp + " < TO_DATE('" + date2 + " 00:00:00','yyyy-mm-dd hh24:mi:ss') and \\$CONDITIONS\" -m "
                            + parafile_map_count + " --target-dir " + hdfs_address_ods + " --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file "
                            + sqoop_param_file + " --map-column-java " + parafile_map_column_java + " --split-by " + parafile_splitby;
                }

            }else if (!parafile_compress_format.equals("no") && parafile_storage_format.equals("no")) { //压缩方式不为no，文件格式为no
                if(parafile_map_column_java.equals("no")) {
                    sqoop_append = "source /etc/profile;sqoop import --connect "+parafile_url+" --username "+parafile_username
                            +" --password "+parafile_password+" --query \"SELECT * from "+parafile_table+" where "+parafile_timestamp
                            +" >= TO_DATE('"+yesterday2+" 00:00:00','yyyy-mm-dd hh24:mi:ss') and "+parafile_timestamp+ " < TO_DATE('"+date2+" 00:00:00','yyyy-mm-dd hh24:mi:ss') and \\$CONDITIONS\" -m "
                            +parafile_map_count+" --target-dir "+hdfs_address_ods +" -z --compression-codec "+parafile_compress_format+" --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file "
                            +sqoop_param_file+" --split-by "+parafile_splitby;

                }else{
                    sqoop_append = "source /etc/profile;sqoop import --connect "+parafile_url+" --username "+parafile_username
                            +" --password "+parafile_password+" --query \"SELECT * from "+parafile_table+" where "+parafile_timestamp
                            +" >= TO_DATE('"+yesterday2+" 00:00:00','yyyy-mm-dd hh24:mi:ss') and "+parafile_timestamp+ " < TO_DATE('"+date2+" 00:00:00','yyyy-mm-dd hh24:mi:ss') and \\$CONDITIONS\" -m "
                            +parafile_map_count+" --target-dir "+hdfs_address_ods +" -z --compression-codec "+parafile_compress_format+" --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file "
                            +sqoop_param_file+" --map-column-java "+parafile_map_column_java+" --split-by "+parafile_splitby;
                }

            } else { //压缩方式不为no
                if(parafile_map_column_java.equals("no")) {
                    sqoop_append = "source /etc/profile;sqoop import --connect "+parafile_url+" --username "+parafile_username
                            +" --password "+parafile_password+" --query \"SELECT * from "+parafile_table+" where "+parafile_timestamp
                            +" >= TO_DATE('"+yesterday2+" 00:00:00','yyyy-mm-dd hh24:mi:ss') and "+parafile_timestamp+ " < TO_DATE('"+date2+" 00:00:00','yyyy-mm-dd hh24:mi:ss') and \\$CONDITIONS\" -m "
                            +parafile_map_count+" --target-dir "+hdfs_address_ods +" -z --compression-codec "+parafile_compress_format+" --as- "
                            +parafile_storage_format+" --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file "
                            +sqoop_param_file+" --split-by "+parafile_splitby;
                }else{
                    sqoop_append = "source /etc/profile;sqoop import --connect "+parafile_url+" --username "+parafile_username
                            +" --password "+parafile_password+" --query \"SELECT * from "+parafile_table+" where "+parafile_timestamp
                            +" >= TO_DATE('"+yesterday2+" 00:00:00','yyyy-mm-dd hh24:mi:ss') and "+parafile_timestamp+ " < TO_DATE('"+date2+" 00:00:00','yyyy-mm-dd hh24:mi:ss') and \\$CONDITIONS\" -m "
                            +parafile_map_count+" --target-dir "+hdfs_address_ods +" -z --compression-codec "+parafile_compress_format+" --as- "
                            +parafile_storage_format+" --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file "
                            +sqoop_param_file+" --map-column-java "+parafile_map_column_java+" --split-by "+parafile_splitby;
                }

            }
        }
        else {

            if (parafile_compress_format.equals("no") && parafile_storage_format.equals("no")){
                if(parafile_map_column_java.equals("no"))
                {
                    sqoop_append = "source /etc/profile;sqoop import --connect "+parafile_url+" --username "+parafile_username
                            +" --password "+parafile_password+" --query \"SELECT * from "+parafile_table+" where "+parafile_timestamp
                            +" >= '"+yesterday+"000000' and "+parafile_timestamp+ " < '"+date+"000000' and \\$CONDITIONS\" -m "
                            +parafile_map_count+" --target-dir "+hdfs_address_ods+" --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file "
                            +sqoop_param_file+" --split-by "+parafile_splitby;
                }else {

                    sqoop_append = "source /etc/profile;sqoop import --connect " + parafile_url + " --username " + parafile_username
                            + " --password " + parafile_password + " --query \"SELECT * from " + parafile_table + " where " + parafile_timestamp
                            + " >= '" + yesterday + "000000' and " + parafile_timestamp + " < '" + date + "000000' and \\$CONDITIONS\" -m "
                            + parafile_map_count + " --target-dir " + hdfs_address_ods + " --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file "
                            + sqoop_param_file + " --map-column-java " + parafile_map_column_java + " --split-by " + parafile_splitby;
                }

            }else if (!parafile_compress_format.equals("no") && parafile_storage_format.equals("no")) { //压缩方式不为no，文件格式为no
                if(parafile_map_column_java.equals("no")){
                    sqoop_append = "source /etc/profile;sqoop import --connect "+parafile_url+" --username "+parafile_username
                            +" --password "+parafile_password+" --query \"SELECT * from "+parafile_table+" where "+parafile_timestamp
                            +" >= '"+yesterday+"000000' and "+parafile_timestamp+ " < '"+date+"000000' and \\$CONDITIONS\" -m "
                            +parafile_map_count+" --target-dir "+hdfs_address_ods+" -z --compression-codec "+parafile_compress_format+
                            " --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file "
                            +sqoop_param_file+" --split-by "+parafile_splitby;
                }else{

                    sqoop_append = "source /etc/profile;sqoop import --connect "+parafile_url+" --username "+parafile_username
                            +" --password "+parafile_password+" --query \"SELECT * from "+parafile_table+" where "+parafile_timestamp
                            +" >= '"+yesterday+"000000' and "+parafile_timestamp+ " < '"+date+"000000' and \\$CONDITIONS\" -m "
                            +parafile_map_count+" --target-dir "+hdfs_address_ods+" -z --compression-codec "+parafile_compress_format+
                            " --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file "
                            +sqoop_param_file+ " --map-column-java " + parafile_map_column_java+" --split-by "+parafile_splitby;

                }


            } else { //压缩方式不为no
                if(parafile_map_column_java.equals("no")) {
                    sqoop_append = "source /etc/profile;sqoop import --connect " + parafile_url + " --username " + parafile_username
                            + " --password " + parafile_password + " --query \"SELECT * from " + parafile_table + " where " + parafile_timestamp
                            + " >= '" + yesterday + "000000' and " + parafile_timestamp + " < '" + date + "000000' and \\$CONDITIONS\" -m "
                            + parafile_map_count + " --target-dir " + hdfs_address_ods + " --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file "
                            + sqoop_param_file +  " --split-by " + parafile_splitby;
                }else{
                    sqoop_append = "source /etc/profile;sqoop import --connect " + parafile_url + " --username " + parafile_username
                            + " --password " + parafile_password + " --query \"SELECT * from " + parafile_table + " where " + parafile_timestamp
                            + " >= '" + yesterday + "000000' and " + parafile_timestamp + " < '" + date + "000000' and \\$CONDITIONS\" -m "
                            + parafile_map_count + " --target-dir " + hdfs_address_ods + " --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file "
                            + sqoop_param_file + " --map-column-java " + parafile_map_column_java + " --split-by " + parafile_splitby;
                }

            }

        }
        try {
            SqoopUtils.importDataUseSSH(sqoop_server_ip,sqoop_server_user,sqoop_server_pwd,sqoop_append);
        } catch (TaskExecFailException e) {
            e.printStackTrace();
        }

        //导入hvie
        try {
            HiveUtils.AddPartition(jdbc_hive,tableName,hdfs_address_ods,date,partition_name);
        } catch (SQLException e) {
            e.printStackTrace();
        }
//        HiveUtils.AddPartition(jdbc_hive,table_name,hdfs_address_ods_full,date,partition_name_full);
    }
}

