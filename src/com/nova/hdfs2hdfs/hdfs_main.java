package com.nova.hdfs2hdfs;

import com.nova.utils.HdfsUtils;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * 場景：
 * 1，源集群写入时按天写入，每天一个文件夹 （已完成）
 *      直接组合，将原路径写入目的路径的当天日期下面即可
 * 2，源集群时按单一目录写入，所有数据都写入一个目录里面
 *      相当于将源文件一个一个拷贝过来
 *
 * 抽取前需要满足的条件：
 * 1，如果集群启用了高可用，则要传入主namenode的地址，暂不支持nameservice的名称
 * 2，存储集群需要能够访问源集群的所有datanode节点以及主namenode节点
 *
 * Created by yunchen on 2017/3/1.
 */
public class hdfs_main {

    public static void main(String[] args) throws Exception {

        Date d = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String date = sdf.format(d);

        String hdfs_address = "hdfs://cmserver:8020";

        HdfsUtils hu = new HdfsUtils(hdfs_address);

        String sourceNameNode = args[1]; //"hdfs://cmname1:8020/ods";
        String destNameNode = args[2] ;//"hdfs://cmserver:8020/ods";

        String i = args[3];

        if(i.equals("1")) {
            /**
             * 1.
             * Distcp 将整个目录抽取，包含子目录，可以控制是否删除目的路径
             *
             */
            String[] dislist = new String[2];
            dislist[0] = sourceNameNode;
            dislist[1] = destNameNode + "/" + date;
            hu.distCopyDirectory(dislist, true);

        }else if(i.equals("2")) {
            /**
             *2.
             * 通过文件修改时间进行增量抽取，一个一个文件去抽取
             *
             * 遗留问题：
             *      相同文件名无法自动覆盖
             *
             */

            String time = "2017-03-03 10:11:11";

            Timestamp maxtime = hu.getMaxTime(sourceNameNode);
            System.out.println("maxtime==============" + maxtime);

            List<String> strings = hu.getNewFileList(sourceNameNode, time);

             hu.distCopyFile(destNameNode,strings);
        }else{
            System.out.println(i+"args[3] input error");
        }
    }

}
