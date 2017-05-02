package com.nova.main;

import com.nova.utils.DBUtils;
import com.nova.utils.HdfsUtils;
import com.nova.utils.PropertiesUtils;
import com.nova.utils.SqoopUtils;
import net.neoremind.sshxcute.exception.TaskExecFailException;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yunchen on 2017/4/6.
 */
public class HdfsExportMain {

    public static void main(String[] args) throws TaskExecFailException, IOException {

        //sqoop export --connect jdbc:oracle:thin:@192.168.1.226:1521:xe --table TT --username ROOT --password root --export-dir /dd
        // --columns 'ID,NAME' --input-fields-terminated-by ',' --input-lines-terminated-by '\n' -m 1 --update-key ID --update-mode allowinsert
        /*String path = "C:\\Users\\yunchen\\Desktop\\yunchen\\Datai\\src\\config.properties";//properties文件位置
        String tableName = "YUNCHEN.GA_DFK_BARXX";//当前需要操作的表名*/
        String path = args[0];
        String tableName = args[1];

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String today = sdf.format(new Date());

        //获取mysql元数据库所需的连接的变量
        Map<String ,String> commonMap = new HashMap<String ,String>();
        commonMap.put("username", PropertiesUtils.Get_Properties(path, "username"));
        commonMap.put("url", PropertiesUtils.Get_Properties(path, "url"));
        commonMap.put("password", PropertiesUtils.Get_Properties(path, "password"));
        commonMap.put("selectsql",PropertiesUtils.Get_Properties(path, "selectsql") + "\"" + tableName + "\"");
        commonMap.put("selectlasttime",PropertiesUtils.Get_Properties(path, "selectlasttime") + "\"" + tableName + "\"");
        commonMap.put("columnname",PropertiesUtils.Get_Properties(path, "columnname"));//暂时没什么用
        commonMap.put("targetdir",PropertiesUtils.Get_Properties(path,"targetdir"));
        commonMap.put("jdbc_hive",PropertiesUtils.Get_Properties(path,"jdbc_hive"));
        commonMap.put("sqoop_server_ip",PropertiesUtils.Get_Properties(path,"sqoop_server_ip"));
        commonMap.put("sqoop_server_user",PropertiesUtils.Get_Properties(path,"sqoop_server_user"));
        commonMap.put("para_path",PropertiesUtils.Get_Properties(path,"PARA_PATH"));
        commonMap.put("hdfs_address",PropertiesUtils.Get_Properties(path,"hdfs_address"));//hdfs集群地址
        commonMap.put("bdsdir",PropertiesUtils.Get_Properties(path,"bdsdir"));

        commonMap.put("hdfs2oraclesql",PropertiesUtils.Get_Properties(path,"hdfs2oraclesql")+ "\"" + tableName  + "\"");//关于hive2oracle配置信息的查询sql

        Map tableMap = DBUtils.get_parafile(commonMap.get("url"), commonMap.get("username"), commonMap.get("password"), commonMap.get("hdfs2oraclesql"));
        System.out.println("待写入表"+tableName+"信息如下：");
        System.out.println(tableMap);
        //Map tableMaptmp = DBUtils.get_parafile(commonMap.get("url"), commonMap.get("username"), commonMap.get("password"), commonMap.get("selectsql"));

/*
        String sqoop_command = "source /etc/profile;sqoop export --connect "+tableMap.get("database_link")+" --table "+
                tableMap.get("table_name")+" --username "+tableMap.get("database_username")+" --password "+tableMap.get("database_password")
                +" --export-dir "+tableMap.get("export_dir")+" --columns '"+tableMap.get("table_columns")+"' --input-fields-terminated-by  ',' --input-lines-terminated-by '\\n' --null-non-string '\\\\N' --null-string '\\\\N' " +
                "-m 1 --update-key "+tableMap.get("update_key")+" --update-mode allowinsert"; //测试，正式时\\n需要改为\\001
*/

        String export_dir = null;
        String sqoop_command;

        /*String tablenametmp = tableName.split(".")[1];
        System.out.println("tablenametmp is : "+tablenametmp);*/

        //这里设计到sqoop_info这张表中table_name字段，所以要采用三表联合模糊查询，这里会导致一个问题，如果两个库中的表相似，就会报错
        //这方便缺陷太明显了，还是用一个字段来存储源表的名称，也就是hdfs上面的二级目录名称
        if(tableMap.get("export_dir").equals("flow")){
            export_dir = commonMap.get("bdsdir")+tableMap.get("source_table_name")+"/"+today+"_delta";//目前增量流水表抽取还没有flow这个值，后续等那边改了，这里的判断就可以取消
        }else if (tableMap.get("export_dir").equals("full") || tableMap.get("export_dir").equals("delta")){
            export_dir = commonMap.get("bdsdir")+tableMap.get("source_table_name")+"/"+today+"_"+tableMap.get("export_dir");
        }else{
            System.out.println(tableMap.get("export_dir")+"    export_dir字段的值错误！");
            System.exit(1);
        }


        String sqoop_command_delete = "source /etc/profile;sqoop eval --connect "+tableMap.get("database_link")+" --username "+tableMap.get("database_username")
                +" --password "+tableMap.get("database_password")+" --query \"delete from "+tableMap.get("table_name")+"\"";

        //1,如果没有主键或者唯一值的情况，这清空表，再插入，不过这里要判断hdfs上是否有数据再清空以及插入
        if(tableMap.get("update_key") == null && tableMap.get("map_column_java") == null) {
            sqoop_command = "source /etc/profile;sqoop export --connect " + tableMap.get("database_link") + " --table " +
                    tableMap.get("table_name") + " --username " + tableMap.get("database_username") + " --password " + tableMap.get("database_password")
                    + " --export-dir " + export_dir + " --columns '" + tableMap.get("table_columns") + "' --input-fields-terminated-by  '\\001' --input-lines-terminated-by '\\n' --input-null-string '\\\\N'  --input-null-non-string '\\\\N' " +
                    "-m 1";  //测试，正式时将\\n需要改为\\001,将NULL改为对应的值，还有分隔符逗号也要改为相应的值
            if(HdfsUtils.isDirectoryEmety(export_dir)) {
                SqoopUtils.importDataUseSSH(commonMap.get("sqoop_server_ip"), commonMap.get("sqoop_server_user"), sqoop_command_delete);
            }else {
                System.out.println("待导出目录为空");
                System.exit(1);
            }

            //没有主键，有映射的情况
        }else if(tableMap.get("update_key") == null && tableMap.get("map_column_java") != null){
            sqoop_command = "source /etc/profile;sqoop export --connect " + tableMap.get("database_link") + " --table " +
                    tableMap.get("table_name") + " --username " + tableMap.get("database_username") + " --password " + tableMap.get("database_password")
                    + " --export-dir " + export_dir + " --columns '" + tableMap.get("table_columns") + "' --input-fields-terminated-by  '\\001' --input-lines-terminated-by '\\n' --input-null-string '\\\\N'  --input-null-non-string '\\\\N' " +
                    "-m 1" + " --map-column-java " + tableMap.get("map_column_java");  //测试，正式时将\\n需要改为\\001,将NULL改为对应的值，还有分隔符逗号也要改为相应的值

            if(HdfsUtils.isDirectoryEmety(export_dir)) {
                SqoopUtils.importDataUseSSH(commonMap.get("sqoop_server_ip"), commonMap.get("sqoop_server_user"), sqoop_command_delete);
            }else {
                System.out.println("待导出目录为空");
                System.exit(1);
            }
        }
        //3,有主键或者唯一值情况
        else if(tableMap.get("update_key") != null && tableMap.get("map_column_java") == null) {
            sqoop_command = "source /etc/profile;sqoop export --connect " + tableMap.get("database_link") + " --table " +
                    tableMap.get("table_name") + " --username " + tableMap.get("database_username") + " --password " + tableMap.get("database_password")
                    + " --export-dir " + export_dir + " --columns '" + tableMap.get("table_columns") + "' --input-fields-terminated-by  '\\001' --input-lines-terminated-by '\\n' --input-null-string '\\\\N'  --input-null-non-string '\\\\N' " +
                    "-m 1 --update-key " + tableMap.get("update_key") + " --update-mode allowinsert";  //测试，正式时将\\n需要改为\\001,将NULL改为对应的值，还有分隔符逗号也要改为相应的值
        } else{
            sqoop_command = "source /etc/profile;sqoop export --connect " + tableMap.get("database_link") + " --table " +
                    tableMap.get("table_name") + " --username " + tableMap.get("database_username") + " --password " + tableMap.get("database_password")
                    + " --export-dir " + export_dir + " --columns '" + tableMap.get("table_columns") + "' --input-fields-terminated-by  '\\001' --input-lines-terminated-by '\\n' --input-null-string '\\\\N'  --input-null-non-string '\\\\N' " +
                    "-m 1 --update-key " + tableMap.get("update_key") + " --update-mode allowinsert" + " --map-column-java " + tableMap.get("map_column_java");  //测试，正式时\\n需要改为\\001
        }

        System.out.println(sqoop_command);
        System.out.println(sqoop_command_delete);

        SqoopUtils.importDataUseSSH(commonMap.get("sqoop_server_ip"), commonMap.get("sqoop_server_user"),sqoop_command);

    }

}
