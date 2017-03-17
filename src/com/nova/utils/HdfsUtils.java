package com.nova.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by yunchen on 2017/1/5.
 */

public class HdfsUtils {

    final static Configuration conf = new Configuration();

    public HdfsUtils(String hdfs_address) throws IOException {
        conf.set("fs.defaultFS", hdfs_address);
    }

    public static void deleteFile(String file) {
        //Configuration conf = new Configuration();
        FileSystem fs;
        try {
            fs= FileSystem.get(conf);
            Path path = new Path(file);
            if (!fs.exists(path)) {
                System.out.println("File " + file + " does not exists");
                return;
            }

            fs.delete(new Path(file), true);
            fs.close();
        }catch (IOException e) {
            System.out.println("deleteFile Exception caught! :" + e);
            new RuntimeException(e);
        }

    }


    /**
     *上传本地文件，主要針對非結構化、半結構化數據
     * @param src   抽取目錄所有文件的源路徑
     * @param dst   hdfs存儲的目的路徑
     * @param deleteornot   是否刪除源數據
     * @throws IOException
     */
    public static void uploadFileToFlume(String src, String dst,boolean deleteornot,String date) throws IOException {
        //Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", hdfs_address);
        TxtUtils.isExistDirectory(src);

        FileSystem fs = FileSystem.get(conf);

        try {
            File file = new File(src);
            List fileName = TxtUtils.getAllFileName(src);
            Path srcPath = new Path(src); //原路径
            Path dstPath = new Path(dst); //目标路径
            //调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
            //fs.copyFromLocalFile(deleteornot, srcPath, dstPath);

            if(file.isDirectory()) {
                File[] files = file.listFiles();
                if(fileName.size() > 0) {
                    //fs.copyFromLocalFile(deleteornot, srcPath, dstPath);
                    if(fs.exists(dstPath)) {
                        fs.delete(dstPath,true);
                    }
                    fs.copyFromLocalFile(deleteornot,true,srcPath,dstPath);
                    for(Object name:fileName)
                    {
                        TxtUtils.moveFileToTmpDirectory(src, (String) name, date);
                    }
                    //TODO 這裏目前只支持單目錄傳輸，也可以多目錄的情況，後續加上
                    //TODO 合并源文件。結構化數據考慮
                }
                /*else if(file.isFile()){
                    fs.copyFromLocalFile(deleteornot,true,srcPath,dstPath);
                } */
                else{
                    System.out.println("==========抽取目錄偉空=================");
                }
            }
            /*//打印文件路径
            System.out.println("Upload to " + conf.get("fs.default.name"));
            System.out.println("------------list files------------" + "\n");
            FileStatus[] fileStatus = fs.listStatus(dstPath);
            for (FileStatus file : fileStatus) {
                System.out.println(file.getPath());
            }*/
            fs.close();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void uploadFileToNotMove(List<String> list, String dst, boolean deleteornot, String date) throws IOException {

        //Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", hdfs_address);
        FileSystem fs = FileSystem.get(conf);

        for(String src:list) {
            TxtUtils.isExistDirectory(src);
            Path srcPath = new Path(src); //原路径

            String[] temp = src.split("/");

            Path dstPath = new Path(dst+temp[temp.length-1]); //目标路径
            fs.copyFromLocalFile(deleteornot,true,srcPath,dstPath);
        }
    }

    /**
     * @param fileList [hdfs://cmname1:8020/ods, hdfs://cmserver:8020/ods/20170301]
     * @param delDstPath    是否删除目的路径
     * @throws Exception
     */
    //TODO 后续可以加入多源路径的支持
    public static void distCopyDirectory(String[] fileList, boolean delDstPath ) throws Exception {
        System.out.println("In dist copy............");

       /* StringTokenizer tokenizer = new StringTokenizer(fileList,",");
        ArrayList<String> list = new ArrayList<>();

        while ( tokenizer.hasMoreTokens() ){
            String file = sourceNameNode + "/" + tokenizer.nextToken();
            list.add( file );
        }

        String[] args = new String[list.size() + 1];
        int count = 0;
        for ( String filename : list ){
            args[count++] = filename;
        }

        args[count] = destNameNode;*/

        //System.out.println("args------>"+ Arrays.toString(fileList));
        //long st = System.currentTimeMillis();

        if(delDstPath){
            deleteFile(fileList[1]);
        }

        Path sourcePath=new Path(fileList[0]);
        Path targetPath = new Path(fileList[1]);
        /*if(extractWay == 2) {
            String[] temp = fileList[1].split("/");
            Path targetPath = new Path(fileList[1] + "/" + temp[temp.length - 1]); //目标路径
        }else {
            Path targetPath = new Path(fileList[1]);
        }*/

        DistCpOptions inputOptions = new DistCpOptions(sourcePath,targetPath);

        inputOptions.setOverwrite(true);

        DistCp distCp=new DistCp(conf,inputOptions);
        distCp.run(fileList);

        //System.out.println(System.currentTimeMillis() - st);

    }

    public static void distCopyFile(String destNameNode, List<String> list ) throws Exception {
        /*System.out.println("In dist copy");

        StringTokenizer tokenizer = new StringTokenizer(fileList,",");
        ArrayList<String> list = new ArrayList<>();

        while ( tokenizer.hasMoreTokens() ){
            String file = sourceNameNode + "/" + tokenizer.nextToken();
            list.add( file );
        }

        String[] args = new String[list.size() + 1];
        int count = 0;
        for ( String filename : list ){
            args[count++] = filename;
        }

        args[count] = destNameNode;

        System.out.println("args------>"+Arrays.toString(args));*/

        /*String stringd[]=new String[list.size()];
        for(int i=0,j=list.size();i<j;i++){
            stringd[i]=list.get(i);
        }*/

        list.add(destNameNode);
        String strings[]=new String[list.size()];
        for(int i=0,j=list.size();i<j;i++){
            strings[i]=list.get(i);
        }


        //这api真tmd坑，加个overwrite参数死活加不进去

        /*Path src = new Path(stringd.toString().trim(),DistCpOptionSwitch.OVERWRITE.getSwitch());
        Path dst = new Path(destNameNode);

        DistCpOptions inputOptions = new DistCpOptions(src,dst);
        inputOptions.setOverwrite(true);
        inputOptions.setMapBandwidth(80);*/

        //long st = System.currentTimeMillis();
        DistCp distCp=new DistCp(conf,null);
        distCp.run(strings);
        //return System.currentTimeMillis() - st;

    }

    /**
     *
     * @param file
     * @return
     * @throws IOException
     */
    public static long fileMetaQuery(String file) throws IOException {
        //读取hadoop文件系统的配置
        //Configuration conf = new Configuration();
        //conf.set("hadoop.job.ugi", "hadoop-user,hadoop-user");

        //实验1:查看HDFS中某文件的元信息
        FileSystem fileFS = FileSystem.get(URI.create(file) ,conf);
        FileStatus fileStatus = fileFS.getFileStatus(new Path(file));
        //获取这个文件的基本信息
        /*if(!fileStatus.isDirectory()){
            System.out.println("这是个文件");
        }*/
        /*System.out.println("文件路径: "+fileStatus.getPath());
        System.out.println("文件长度: "+fileStatus.getLen());
        System.out.printf("文件修改日期： %s%n", new Timestamp(fileStatus.getModificationTime()).toString());
        System.out.println("文件上次访问日期： "+new Timestamp(fileStatus.getAccessTime()).toString());
        System.out.println("文件备份数： "+fileStatus.getReplication());
        System.out.println("文件的块大小： "+fileStatus.getBlockSize());
        System.out.println("文件所有者：  "+fileStatus.getOwner());
        System.out.println("文件所在的分组： "+fileStatus.getGroup());
        System.out.println("文件的 权限： "+fileStatus.getPermission().toString());
        System.out.println();*/

        long modtime = new Timestamp(fileStatus.getModificationTime()).getTime();

        System.out.printf("文件修改日期： %s%n",modtime );

        return modtime;

    }

    /**
     * 获取文件列表
     * @param path
     * @return
     * @throws IOException
     */
    public static List<Path> getAllFileName(String path) throws IOException {

        FileSystem hdfs = FileSystem.get(URI.create(path),conf);
        FileStatus[] fs = hdfs.listStatus(new Path(path));
        Path[] listPath = FileUtil.stat2Paths(fs);
        List<Path> list = new ArrayList<>();
        for(Path p : listPath) {
            System.out.println(p);
            if(isDirectory(p.toString()) == false) {
                list.add(p);
            }
        }
        return list;
    }

    /**
     * 过滤掉目录
     * @param path
     * @return
     * @throws IOException
     */
    public static boolean isDirectory(String path) throws IOException {

        //Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create(path), conf);
        FileStatus[] fs = hdfs.listStatus(new Path(path));

        Path[] paths = FileUtil.stat2Paths(fs);

        boolean bool = false;

        for (Path p : paths){
            FileSystem fileFS = FileSystem.get(URI.create(p.toString()) ,conf);
            FileStatus fileStatus = fileFS.getFileStatus(p);
            bool = fileStatus.isDirectory();
        }
        return bool;
    }


    /**
     * 获取文件时间
     * @param path
     * @return
     * @throws IOException
     */
    public static Timestamp getFileTime(String path) throws IOException {

        //Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create(path), conf);
        FileStatus[] fs = hdfs.listStatus(new Path(path));

        Path[] paths = FileUtil.stat2Paths(fs);

        Timestamp time = null;

        for (Path p : paths){
            FileSystem fileFS = FileSystem.get(URI.create(p.toString()) ,conf);
            FileStatus fileStatus = fileFS.getFileStatus(p);
            time = new Timestamp(fileStatus.getModificationTime());
        }
        return time;
    }

    /**
     * 获取大于指定修改时间的文件列表
     * @param path
     * @param mintime
     * @return
     * @throws IOException
     */
    public static List<String> getNewFileList(String path, String mintime ) throws IOException, ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date d;
        d = sdf.parse(mintime);

        List<String> list = new ArrayList<>();
        //StringBuilder sb = new StringBuilder();


        List<Path> filelist =  getAllFileName(path);

        for(Path p : filelist) {
            if(getFileTime(p.toString()).getTime() > d.getTime()){
                //String[] temp = p.toString().split("/");
                list.add(p.toString());
                //list.add(temp[temp.length - 1]);

            }
        }

        /*String strings[]=new String[list.size()];
        for(int i=0,j=list.size();i<j;i++){
            strings[i]=list.get(i);
        }*/


        /*if (list != null && list.size() > 0) {
            for (int i = 0; i < list.size(); i++) {
                if (i < list.size() - 1) {
                    sb.append(list.get(i) + ",");
                } else {
                    sb.append(list.get(i));
                }
            }
        }

        return sb.toString();*/

        return list;

    }

    public static Timestamp getMaxTime(String path) throws IOException, ParseException {

        List<Path> filelist =  getAllFileName(path);

        Timestamp maxtime = null;

        for(Path p : filelist) {
            if(null == maxtime){
                maxtime = getFileTime(p.toString());
            }else{
                if(maxtime.getTime() < getFileTime(p.toString()).getTime()){
                    maxtime = getFileTime(p.toString());
                }
            }
        }
        return  maxtime;

    }


    public boolean uploadStructFileToHdfs(String path,String localfile){
        File file=new File(localfile);
        if (!file.isFile()) {
            System.out.println(file.getName());
            return false;
        }

        try {
            FileSystem localFS =FileSystem.getLocal(conf);
            FileSystem hadoopFS =FileSystem.get(conf);
            Path hadPath=new Path(path);
            FSDataOutputStream fsOut=hadoopFS.create(new Path(path+"/"+file.getName()));
            FSDataInputStream fsIn=localFS.open(new Path(localfile));
            byte[] buf =new byte[1024];
            int readbytes=0;
            while ((readbytes=fsIn.read(buf))>0){
                fsOut.write(buf,0,readbytes);
            }
            fsIn.close();
            fsOut.close();
            FileStatus[] hadfiles= hadoopFS.listStatus(hadPath);
            for(FileStatus fs :hadfiles){
                System.out.println(fs.toString());
            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }


}
