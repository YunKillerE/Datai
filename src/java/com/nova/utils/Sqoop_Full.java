package java.com.nova.utils;

import net.neoremind.sshxcute.exception.TaskExecFailException;

import java.sql.SQLException;

/**
 *
 * 1，sqoop jar包
 * 2，hadoop-common jar包
 * 3，hadoop-hdfs jar包
 * 4，hadoop-common lib jar包
 * 5，hadoop mapreduce jar包
 * 6，mysql jar包
 * 7,hive lib目录jar包
 *
 * 完成后打jar包，不需要吧依赖的jar包打进去
 *
 * 运行命令：java -Djava.ext.dirs=/opt/cloudera/parcels/CDH/jars:/usr/java/latest/jre/lib/ext/ -jar sqoop1.jar
 *
 * 需求：
 * 1，源数据库可自定义   dmp
 * 2，源表可自定义       dmp_tag
 * 3，源数据库类型可自定义 mysql or oracle
 * 4，hdfs集群地址可自定义
 * 5，输出路径可自定义
 * 6，增量全量自定义、增量字段、增量
 * 7，压缩格式、存储格式自定义
 *
 */

public class Sqoop_Full {

    /**
     * @param url 数据库链接
     * @param username 用户名
     * @param password 数据库密码
     * @param table 表名
     * @param map_count 并发量
     * @param target_dir 目标文件夹
     * @param sqoop_server_ip sqoop服务器ip
     * @param sqoop_server_user sqoop服务器用户名
     * */
    public static void full_importtest (String url, String username, String password, String table, String map_count,
                                        String target_dir, String sqoop_server_ip, String sqoop_server_user) throws SQLException, TaskExecFailException
    {
        String sqoop_command = "source /etc/profile;sqoop import --connect "+url+" --username "+username+" --password "+password+" --table "+table+" -m "+map_count+" --target-dir "+target_dir;
        SqoopUtils.importDataUseSSH(sqoop_server_ip,sqoop_server_user,sqoop_command);
    }

    /**
     * @param url 数据库链接
     * @param username 用户名
     * @param password 数据库密码
     * @param table 表名
     * @param map_count 并发量
     * @param target_dir 目标文件夹
     * @param param_file 取消TO_DATE函数的参数文件
     * @param map_column_java 类型隐射参数
     * @param sqoop_server_ip sqoop服务器ip
     * @param sqoop_server_user sqoop服务器用户名
     * */
    public static void full_import(String url,String username,String password,String table,String map_count,
                                       String target_dir,String param_file,String map_column_java,String sqoop_server_ip, String sqoop_server_user) throws SQLException, TaskExecFailException
    {
        String sqoop_command;
        if(map_column_java.equals("no")){
            sqoop_command = "source /etc/profile;sqoop import --connect "+url+" --username "+username+" --password "+password+" --table "+table+" -m "+map_count+" --target-dir "+target_dir
                    + " --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file "+param_file;
        }else{
            sqoop_command = "source /etc/profile;sqoop import --connect "+url+" --username "+username+" --password "+password+" --table "+table+" -m "+map_count+" --target-dir "+target_dir
                    + " --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file "+param_file+" --map-column-java "+map_column_java;
        }
        System.out.print(sqoop_command);
        SqoopUtils.importDataUseSSH(sqoop_server_ip,sqoop_server_user,sqoop_command);
    }

    /**
     * @param url 数据库链接
     * @param username 用户名
     * @param password 数据库密码
     * @param table 表名
     * @param map_count 并发量
     * @param target_dir 目标文件夹
     * @param compression 压缩方式
     * @param param_file 取消TO_DATE函数的参数文件
     * @param map_column_java 类型隐射参数
     * @param sqoop_server_ip sqoop服务器ip
     * @param sqoop_server_user sqoop服务器用户名
     * */
    public static void full_import(String url,String username, String password, String table, String map_count,
                                   String target_dir,String compression,String param_file,String map_column_java,String sqoop_server_ip, String sqoop_server_user)throws SQLException, TaskExecFailException
    {
        String sqoop_command;
        if(map_column_java.equals("no")){
            sqoop_command = "source /etc/profile;sqoop import --connect "+url+" --username "+username+" --password "+password+" --table "+table+" -m "+map_count+" --target-dir "+target_dir
                    + " -z --compression-codec "+ compression +" --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file "+param_file;
        }else{
            sqoop_command = "source /etc/profile;sqoop import --connect "+url+" --username "+username+" --password "+password+" --table "+table+" -m "+map_count+" --target-dir "+target_dir
                    + " -z --compression-codec "+ compression +" --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file "+param_file
                    +" --map-column-java "+map_column_java;
        }


        System.out.print(sqoop_command);
        SqoopUtils.importDataUseSSH(sqoop_server_ip,sqoop_server_user,sqoop_command);

    };

    /**
     * @param url 数据库链接
     * @param username 用户名
     * @param password 数据库密码
     * @param table 表名
     * @param map_count 并发量
     * @param target_dir 目标文件夹
     * @param compression 压缩方式
     * @param format 存储格式
     * @param param_file 取消TO_DATE函数的参数文件
     * @param map_column_java 类型隐射参数
     * @param sqoop_server_ip sqoop服务器ip
     * @param sqoop_server_user sqoop服务器用户名
     * */
    public static void full_import(String url,String username, String password, String table, String map_count,
                                   String target_dir,String compression,String format,String param_file,String map_column_java,String sqoop_server_ip,String sqoop_server_user)throws SQLException, TaskExecFailException
    {
        String sqoop_command;
        if(map_column_java.equals("no")){
            sqoop_command = "source /etc/profile;sqoop import --connect "+url+" --username "+username+" --password "+password+" --table "+table+" -m "+map_count+" --target-dir "+target_dir
                    + " -z --compression-codec "+ compression +"--as-"+format+" --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file "+param_file;
        }else{
            sqoop_command = "source /etc/profile;sqoop import --connect "+url+" --username "+username+" --password "+password+" --table "+table+" -m "+map_count+" --target-dir "+target_dir
                    + " -z --compression-codec "+ compression +"--as-"+format+" --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file "+param_file
                    +" --map-column-java "+map_column_java;
        }
        System.out.print(sqoop_command);
        SqoopUtils.importDataUseSSH(sqoop_server_ip,sqoop_server_user,sqoop_command);
    };


}
