package com.nova.utils;

import java.io.File;
import java.text.ParseException;
import java.util.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yunchen on 2017/2/28.
 */
public class TxtUtils {

    /**
     *  获取指定目录下的所有文件
     *
     * @param path  抽取目录
     * @return  返回目录下的所有文件的list，不包括目录
     */
    public static List<String> getAllFileName(String path)
    {
        File file = new File(path);
        File[] fileName = file.listFiles();
        List<String> list = new ArrayList<>();
        if(file.isDirectory()) {
            for (File string : fileName)
                if (new File(string.getPath()).isFile()) {
                    System.out.println(string.getPath());
                    list.add(string.getPath());
                }
        }else {
            System.out.println("it's a file.");
            //System.exit(1);
        }
        return list;
    }

    /**
     *  @param path  抽取根目录
     * @param filepath  已经抽取完毕的文件全路径
     * @param date  当天时间
     *
     */
    public static void moveFileToTmpDirectory(String path, String filepath, String date){

        String tmppath = path+"tmp/"+date;
        File file = new File(tmppath);
        if(!file.exists()){
            file.mkdirs();
        }
        //传入filepath，已经传输完成的文件全路径
        try {
        File file1 = new File(filepath);
        if(file1.renameTo(new File(tmppath+"/"+file1.getName()))) {
            System.out.println(filepath+" : File is moved successful!");
        } else {
            System.out.println(filepath+" : File is failed to move!");
        }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    //判斷文件是否存在
    public static void isExistDirectory(String path) {
        File file = new File(path);
        if (!file.exists()) {
          /*  String[] files = file.list();
            if (!(files.length > 0)) {
                System.out.print("=========抽取目錄不存在==========");
                System.exit(1);
            }*/
            System.out.println("=========抽取目錄不存在==========");
            System.exit(1);
        }
    }

    /**
     *  找出大于上一次最大修改时间的文件，返回一个文件列表的list
     *
     * @param path  抽取路径
     * @param mintime   上一次所有文件的最大修改时间
     * @return  List
     * @throws ParseException
     */
    public static List getNewFileList(String path, String mintime) throws ParseException {

        List list = new ArrayList();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date d;
        d = sdf.parse(mintime);

        List<String> filelist = getAllFileName(path);
        for(String file:filelist){
            File f = new File(file);
            //Date filetime = new Date(f.lastModified());
            Timestamp time = new Timestamp(f.lastModified());
            //SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            //String dt = sdt.format(filetime);
            if(d.getTime() < time.getTime()) {
                /*//System.out.println(mintime + "======" + mintime);
                System.out.println("--------------------------------------");
                System.out.println(file + "======" + time);*/
                list.add(file);
            }
        }
        return list;
    }

    /**
     *  获取一个目录中所有文件最大的修改时间
     *
     * @param path
     * @return
     */
    public static Timestamp getMaxModifyTime(String path){
        List<String> filelist = getAllFileName(path);
        Timestamp time = null;

        for(String s:filelist){
            File f = new File(s);
            if(time == null){
                time = new Timestamp(f.lastModified());
            }else {
                if(time.getTime() < new Timestamp(f.lastModified()).getTime()){
                    time = new Timestamp(f.lastModified());
                }
            }
        }
        return time;

    }

  /*  public static void main(String[] args)
    {
        List fileName = getAllFileName("C:\\Users\\yunchen\\Desktop\\file");
        for(Object name:fileName)
        {
            System.out.println(name);
        }

        System.out.println("--------------------------------");

    }*/

}
