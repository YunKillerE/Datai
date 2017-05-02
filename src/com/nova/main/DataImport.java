package com.nova.main;

import com.nova.sqoop.SqoopAppend;
import com.nova.sqoop.SqoopExtraction;
import com.nova.sqoop.SqoopMerge;
import com.nova.utils.DBUtils;
import com.nova.utils.PropertiesUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by shenly on 2017/3/1.
 */
public class DataImport {
    public static void main(String[] args){

        //String path = "D:/IDEAworkspace/sqoop1_import/src/config.properties";//properties文件位置
        //String tableName = "YUNCHEN.JQJD_EXTRACT_INFO";//当前需要操作的表名

        String path = args[0];//properties文件位置
        System.out.println("配置文件目录"+path);
        String tableName = args[1];//当前需要操作的表名
        System.out.println("当前操作表名"+tableName);

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
        commonMap.put("sqoop_server_pwd",PropertiesUtils.Get_Properties(path,"sqoop_server_pwd"));
        commonMap.put("para_path",PropertiesUtils.Get_Properties(path,"PARA_PATH"));
        commonMap.put("hdfs_address",PropertiesUtils.Get_Properties(path,"hdfs_address"));//hdfs集群地址



        Map tableMap = DBUtils.get_parafile(commonMap.get("url"), commonMap.get("username"), commonMap.get("password"), commonMap.get("selectsql"));
        System.out.println("待抽取表"+tableName+"信息如下：");
        System.out.println(tableMap);




        String tableMap_full_delta = (String) tableMap.get("sqoop_delta_full");
        switch (tableMap_full_delta){
            case "full":
                System.out.println("表"+tableName+"开始全量抽取");
                    new SqoopExtraction().extraction(path,tableName,commonMap,tableMap);break;
            case "delta":
                System.out.println("表"+tableName+"开始增量抽取并合并");
                new SqoopExtraction().extraction(path,tableName,commonMap,tableMap);
                System.out.println("表"+tableName+"开始增量抽取成功，开始合并");
                new SqoopMerge().merge(path,tableName,commonMap,tableMap);
                break;
            case "append":
                System.out.println("表"+tableName+"开始分区数据抽取");
                new SqoopAppend().append(path,tableName,commonMap,tableMap);
                break;
            default:
                System.out.println("数据库中"+tableName+"信息有误");break;

        }
    }
}
