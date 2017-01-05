package tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.Sqoop;
import org.apache.sqoop.tool.SqoopTool;
import org.apache.sqoop.util.OptionsFileUtil;

/**
 * Created by yunchen on 2016/12/28.
 */
public class SqoopUtils {
    public static int importDataFrom(String[] args,String hdfs_address) {

        String[] expandedArgs = null;
        try {
            expandedArgs = OptionsFileUtil.expandArguments(args);
        } catch (Exception  ex) {
            System.err.println(ex.getMessage());
            System.err.println("Try 'sqoop help' for usage.");
        }

        com.cloudera.sqoop.tool.SqoopTool tool = (com.cloudera.sqoop.tool.SqoopTool) SqoopTool.getTool("import");
        //com.cloudera.sqoop.tool.SqoopTool tool = new ImportTool();

        Configuration conf = new Configuration() ;
        //conf.set("fs.defaultFS", hdfs_address);//设置hadoop服务地址
        conf.set("fs.default.name", hdfs_address);//设置hadoop服务地址
        Configuration pluginConf = tool.loadPlugins(conf);

        Sqoop sqoop = new Sqoop(tool, pluginConf);
        return Sqoop.runSqoop(sqoop, expandedArgs);
    }

}
