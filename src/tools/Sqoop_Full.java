package tools;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.Sqoop;
import org.apache.sqoop.tool.SqoopTool;
import org.apache.sqoop.util.OptionsFileUtil;

import static tools.SqoopUtils.importDataFrom;

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

    public static void full_import(String url,String username,String password,String table,
                                   String splitby,String target_dir,String hdfs_address) {
        String[] sqoopargs = new String[]{
                "--connect", url,
                "--username", username,
                "--password", password,
                "--table", table,
                "--split-by", splitby,
                "--target-dir", target_dir
        };
        try {
            importDataFrom(sqoopargs,hdfs_address);
        } catch (Exception e){
            e.printStackTrace();
        }
    }


}
