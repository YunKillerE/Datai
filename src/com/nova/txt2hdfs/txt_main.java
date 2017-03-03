package com.nova.txt2hdfs;
import com.nova.utils.*;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Created by yunchen on 2017/2/28.
 */

/**
 * 抽取的数据类型：
* 1，结构化数据
 * 2，非结构化数据（半结构化）
 *      1）可以移动数据或者删除数据的场景（已完成）,借用了flume的抽取思想
 *      2）不能移动数据场景，也就是说要保持源数据不变，需要去判断文件的修改时间 (已完成) 借鉴了sqoop的关系型数据库抽取的思想
 *
 * 结构化数据抽取思路：
 * 1，如果不打算考虑数据的结构文件和导入hive的报错问题，则直接用非结构化抽取的方式
 * 2，如果要严格控制数据结构，保证导入hive不会出错，则指定结构，借鉴datax，将datax的核心框架分离出来，
 * 合并到项目中就可以使用了
* */
public class txt_main {

    /**
     * 注意：暫未考慮抽取目錄中的子目錄的情況,也就是說子目錄會忽略
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException, ParseException {

        String path = "/opt/data";    //抽取目录
        //String path = "C:\\Users\\yunchen\\Desktop\\file";

        Date d = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat sdt = new SimpleDateFormat("yyyyMMdd hh:mm:ss");
        String dt = sdt.format(d);

        String date = sdf.format(d);    //日期
        String deleteornot = "false";   //是否删除源数据，true删除，false移动
        String dstpath = "/ods/yunchen/"+date+"/";  //构造hdfs路径
        String hdfs_address = "hdfs://cmserver";    //hdfs存储地址

        String time = "2017-01-02 11:11:11";    //上一次抽取的最大文件修改时间,需要存储到数据库中

        HdfsUtils hu = new HdfsUtils(hdfs_address);

        /**
         *
         * 借用flume抽取思想的方式
         *
         */
        if(deleteornot.equals("false")) {
            //TODO 加入hdfs的上传函数,false,不删除源数据，將源數據move到tmp目錄下面
            hu.uploadFileToFlume(path,dstpath,false,date);
            System.out.println("==========抽取成功==========");
            System.out.println();
        }else{
            //TODO 加入hdfs的上传函数,true,删除源数据
            hu.uploadFileToFlume(path,dstpath,true,date);
            System.out.println("==========抽取失敗==========");

        }


        /**
         *
         * 通过时间来判断，进行增量抽取
         *
         */

     /*   Timestamp maxtime = TxtUtils.getMaxModifyTime(path); //本次抽取的最大时间

        System.out.println("1==========================================");

        System.out.println("maxtime======="+maxtime);

        List<String>  filelist = TxtUtils.getNewFileList(path,time); //获取本次抽取的文件列表

        System.out.println("2==========================================");

        hu.uploadFileToNotMove(filelist,dstpath,hdfs_address,false,date);*/

        //TODO 调用mysql函数将最大值写入mysql中

    }

}