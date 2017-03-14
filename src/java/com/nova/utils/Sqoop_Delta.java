package java.com.nova.utils;

import net.neoremind.sshxcute.exception.TaskExecFailException;

import java.sql.SQLException;

/**
 * Created by yunchen on 2016/12/27.
 */
public class Sqoop_Delta {

    /**
     * 用于增量抽取时间戳为varchar类型的表
     *
     * @param url 数据库链接
     * @param username 用户名
     * @param password 密码
     * @param table 表名
     * @param timestamp 时间戳
     * @param map_count 并行度
     * @param target_dir 目标文件夹
     * @param param_file 取消TO_DATE函数参数文件
     * @param map_column_java 字段类型映射
     * @param split_key 主键
     * @param sqoop_server_ip sqoop服务器IP
     * @param sqoop_server_user sqoop服务器用户名
     * @param today 今天日期
     * @param yestday 昨天日期
     * */
    public static void delta_import_varchar(String url,String username, String password,String table, String timestamp,
                                            String map_count,String target_dir,String param_file, String map_column_java, String split_key,String sqoop_server_ip,String sqoop_server_user,String today ,String yestday)throws SQLException,TaskExecFailException
    {
        String sqoop_command;
        if (!map_column_java.equals("no")) {
            sqoop_command = "source /etc/profile;sqoop import --connect " + url + " --username " + username + " --password " + password +
                    " --query " + "\"select * from " + table + " where " + timestamp + " < '" + today + "' and " + timestamp + " > '" + yestday + "' and \\$CONDITIONS\" -m" + map_count + " --target-dir " + target_dir
                    + " --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file " + param_file + " --map-column-java " + map_column_java + " --split-by " + split_key;
        }else{
            sqoop_command = "source /etc/profile;sqoop import --connect " + url + " --username " + username + " --password " + password +
                    " --query " + "\"select * from " + table + " where " + timestamp + " < '" + today + "' and " + timestamp + " > '" + yestday + "' and \\$CONDITIONS\" -m" + map_count + " --target-dir " + target_dir
                    + " --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file " + param_file + " --split-by " + split_key;
        }
        SqoopUtils.importDataUseSSH(sqoop_server_ip, sqoop_server_user, sqoop_command);
    }

    /**
     * 用于增量抽取时间戳为varchar类型的表
     *
     * @param url 数据库链接
     * @param username 用户名
     * @param password 密码
     * @param table 表名
     * @param map_count 并行度
     * @param target_dir 目标文件夹
     * @param timestamp 时间戳
     * @param last_timestamp 上次抽取时间
     * @param param_file 取消TO_DATE函数参数文件
     * @param map_column_java 字段类型映射
     * @param sqoop_server_ip sqoop服务器IP
     * @param sqoop_server_user sqoop服务器用户名
     *
     * */
    public static void delta_import(String url,String username, String password,String table, String map_count, String target_dir,
                                   String timestamp,String last_timestamp,String param_file,String map_column_java,String sqoop_server_ip,String sqoop_server_user) throws SQLException,TaskExecFailException
    {
        String sqoop_command;
        if (map_column_java.equals("no")) {
            sqoop_command = "source /etc/profile;sqoop import --connect " + url + " --username " + username + " --password " + password + " --table " + table + " -m " + map_count + " --target-dir " + target_dir
                    +" --check-column "+timestamp+" --incremental lastmodified --last-value '"+ last_timestamp
                        + "' --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file " + param_file;
        } else {
            sqoop_command = "source /etc/profile;sqoop import --connect " + url + " --username " + username + " --password " + password + " --table " + table + " -m " + map_count + " --target-dir " + target_dir
            +" --check-column "+timestamp+" --incremental lastmodified --last-value '"+ last_timestamp
                        + "' --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file " + param_file + " --map-column-java " + map_column_java;
        }
        SqoopUtils.importDataUseSSH(sqoop_server_ip, sqoop_server_user, sqoop_command);
    }

    /**
     * 用于增量抽取时间戳为varchar类型的表
     *
     * @param url 数据库链接
     * @param username 用户名
     * @param password 密码
     * @param table 表名
     * @param map_count 并行度
     * @param target_dir 目标文件夹
     * @param timestamp 时间戳
     * @param last_timestamp 上次抽取时间
     * @param compression 压缩格式
     * @param param_file 取消TO_DATE函数参数文件
     * @param map_column_java 字段类型映射
     * @param sqoop_server_ip sqoop服务器IP
     * @param sqoop_server_user sqoop服务器用户名
     * */
    public static void delta_import(String url,String username, String password,String table, String map_count, String target_dir,
                                    String timestamp,String last_timestamp,String compression,String param_file,String map_column_java,String sqoop_server_ip,String sqoop_server_user) throws SQLException,TaskExecFailException
    {
        String sqoop_command;
        if (map_column_java.equals("no")) {
            sqoop_command = "source /etc/profile;sqoop import --connect " + url + " --username " + username + " --password " + password + " --table " + table + " -m " + map_count + " --target-dir " + target_dir
                    + " -z --compression-codec "+ compression +" --check-column "+timestamp+" --incremental lastmodified --last-value '"+last_timestamp
                    + "' --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file " + param_file;
        } else {
            sqoop_command = "source /etc/profile;sqoop import --connect " + url + " --username " + username + " --password " + password + " --table " + table + " -m " + map_count + " --target-dir " + target_dir
                    + " -z --compression-codec "+ compression +" --check-column "+timestamp+" --incremental lastmodified --last-value '"+last_timestamp
                    + "' --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file " + param_file + " --map-column-java " + map_column_java;
        }
        SqoopUtils.importDataUseSSH(sqoop_server_ip, sqoop_server_user, sqoop_command);
    }

    /**
     * 用于增量抽取时间戳为varchar类型的表
     *
     * @param url 数据库链接
     * @param username 用户名
     * @param password 密码
     * @param table 表名
     * @param map_count 并行度
     * @param target_dir 目标文件夹
     * @param timestamp 时间戳
     * @param last_timestamp 上次抽取时间
     * @param compression 压缩格式
     * @param format 储存格式
     * @param param_file 取消TO_DATE函数参数文件
     * @param map_column_java 字段类型映射
     * @param sqoop_server_ip sqoop服务器IP
     * @param sqoop_server_user sqoop服务器用户名
     * */
    public static void delta_import(String url,String username, String password,String table, String map_count, String target_dir,
                                    String timestamp,String last_timestamp,String compression, String format,String param_file,String map_column_java,String sqoop_server_ip,String sqoop_server_user) throws SQLException,TaskExecFailException
    {
        String sqoop_command;
        if (map_column_java.equals("no")) {
            sqoop_command = "source /etc/profile;sqoop import --connect " + url + " --username " + username + " --password " + password + " --table " + table + " -m " + map_count + " --target-dir " + target_dir
                    + " -z --compression-codec "+ compression +" --as-"+format+" --check-column "+timestamp+" --incremental lastmodified --last-value '"+last_timestamp
                    + "' --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file " + param_file;
        } else {
            sqoop_command = "source /etc/profile;sqoop import --connect " + url + " --username " + username + " --password " + password + " --table " + table + " -m " + map_count + " --target-dir " + target_dir
                    + " -z --compression-codec "+ compression +" --as-"+format+" --check-column "+timestamp+" --incremental lastmodified --last-value '"+last_timestamp
                    + "' --null-non-string '\\\\N' --null-string '\\\\N' --fields-terminated-by '\\001' --hive-drop-import-delims --connection-param-file " + param_file + " --map-column-java " + map_column_java;
        }
        SqoopUtils.importDataUseSSH(sqoop_server_ip, sqoop_server_user, sqoop_command);
    }
}
