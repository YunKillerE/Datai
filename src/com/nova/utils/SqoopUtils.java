package com.nova.utils;

import net.neoremind.sshxcute.core.ConnBean;
import net.neoremind.sshxcute.core.Result;
import net.neoremind.sshxcute.core.SSHExec;
import net.neoremind.sshxcute.exception.TaskExecFailException;
import net.neoremind.sshxcute.task.CustomTask;
import net.neoremind.sshxcute.task.impl.ExecCommand;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.Sqoop;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.tool.ImportTool;
import org.apache.sqoop.tool.SqoopTool;
import org.apache.sqoop.util.OptionsFileUtil;


/**
 * Created by yunchen on 2016/12/28.
 */

public class SqoopUtils {
    public static int importDataUseArgs(String[] args, String hdfs_address) {

        String[] expandedArgs = null;
        try {
            expandedArgs = OptionsFileUtil.expandArguments(args);
        } catch (Exception  ex) {
            System.err.println(ex.getMessage());
            System.err.println("Try 'sqoop help' for usage.");
        }

        //com.cloudera.sqoop.tool.SqoopTool tool = (com.cloudera.sqoop.tool.SqoopTool) SqoopTool.getTool("import");
        //com.cloudera.sqoop.tool.SqoopTool tool = new ImportTool();
        //SqoopTool tool = new ImportTool();

        SqoopTool tool = SqoopTool.getTool("import");

        Configuration conf = new Configuration() ;
        conf.set("fs.defaultFS", hdfs_address);//设置hadoop服务地址
        //conf.set("fs.default.name", hdfs_address);//设置hadoop服务地址
        Configuration pluginConf = tool.loadPlugins(conf);

        /*Sqoop sqoop = new Sqoop(tool, pluginConf);*/
        // Sqoop sqoop = new Sqoop(tool, pluginConf);
        Sqoop sqoop = new Sqoop((com.cloudera.sqoop.tool.SqoopTool) tool,pluginConf);

        return Sqoop.runSqoop(sqoop, expandedArgs);
    }

    public static int importDataUseOptions(String[] args, String hdfs_address) {

        SqoopOptions options = new SqoopOptions();
        options.setConnectString("jdbc:mysql://HOSTNAME:PORT/DATABASE_NAME");
        //options.setTableName("TABLE_NAME");
        //options.setWhereClause("id>10");     // this where clause works when importing whole table, ie when setTableName() is used
        options.setUsername("USERNAME");
        options.setPassword("PASSWORD");
        //options.setDirectMode(true);    // Make sure the direct mode is off when importing data to HBase
        options.setNumMappers(8);         // Default value is 4
        options.setSqlQuery("SELECT * FROM user_logs WHERE $CONDITIONS limit 10");
        options.setSplitByCol("log_id");

        //HDFS
        options.setTargetDir("/ooo");

        int ret = new ImportTool().run((com.cloudera.sqoop.SqoopOptions) options);

        return  ret;

    }

    public static void importDataUseSSH(String sqoop_server_ip, String sqoop_server_user, String sqoop_command) throws TaskExecFailException {

        // Initialize a ConnBean object, parameter list is ip, username, password

        ConnBean cb = new ConnBean(sqoop_server_ip, sqoop_server_user,"0okm(IJN");

        // Put the ConnBean instance as parameter for SSHExec static method getInstance(ConnBean) to retrieve a singleton SSHExec instance
        SSHExec ssh = SSHExec.getInstance(cb);
        // Connect to server
        ssh.connect();
        CustomTask sampleTask1 = new ExecCommand("echo $SSH_CLIENT"); // Print Your Client IP By which you connected to ssh server on Horton Sandbox
        System.out.println(ssh.exec(sampleTask1));
        //CustomTask sampleTask2 = new ExecCommand("sqoop import --connect jdbc:oracle:thin:@192.168.1.28:1521:xe --username yunchen --password root --table MYTABLE -m 1 --target-dir /ooii");
        CustomTask sampleTask2 = new ExecCommand(sqoop_command);
        ssh.exec(sampleTask2);
        ssh.disconnect();

    }

    public static String SelectMaxUseSSH(String sqoop_server_ip, String sqoop_server_user, String sqoop_command) throws TaskExecFailException {

        ConnBean cb = new ConnBean(sqoop_server_ip, sqoop_server_user,"0okm(IJN");

        // Put the ConnBean instance as parameter for SSHExec static method getInstance(ConnBean) to retrieve a singleton SSHExec instance
        SSHExec ssh = SSHExec.getInstance(cb);
        // Connect to server
        ssh.connect();
        CustomTask sampleTask1 = new ExecCommand("echo $SSH_CLIENT"); // Print Your Client IP By which you connected to ssh server on Horton Sandbox
        System.out.println(ssh.exec(sampleTask1));
        CustomTask sampleTask2 = new ExecCommand(sqoop_command);
        Result result = ssh.exec(sampleTask2);
        ssh.disconnect();

        String[] max = result.sysout.toString().split("\n");
 /*       for (int i = 0; i < max.length; i++) {
            System.out.println(i+"==========="+max[i]);
        }*/

        return max[max.length-2].replace("|","").trim();
    }

}
