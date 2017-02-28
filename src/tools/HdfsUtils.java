package tools;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Created by yunchen on 2017/1/5.
 */

public class HdfsUtils {

    public static void deleteFile(String file) {
        Configuration conf = new Configuration();
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

    public static FileSystem getConf(String hdfs_address) throws IOException {
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://cmserver:8020");
        conf.set("fs.defaultFS",hdfs_address);
        FileSystem fs = FileSystem.get(conf);
        return fs;
    }

    //上传本地文件
    public static void uploadFile(String src, String dst,String hdfs_address) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfs_address);
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(src); //原路径
        Path dstPath = new Path(dst); //目标路径
        //调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
        fs.copyFromLocalFile(false, srcPath, dstPath);

        //打印文件路径
        System.out.println("Upload to " + conf.get("fs.default.name"));
        System.out.println("------------list files------------" + "\n");
        FileStatus[] fileStatus = fs.listStatus(dstPath);
        for (FileStatus file : fileStatus) {
            System.out.println(file.getPath());
        }
        fs.close();
    }

    //创建新文件
    public static void createFile(String dst, byte[] contents,String hdfs_address) throws IOException {
        FileSystem fs = getConf(hdfs_address);
        Path dstPath = new Path(dst); //目标路径
        //打开一个输出流
        FSDataOutputStream outputStream = fs.create(dstPath);
        outputStream.write(contents);
        outputStream.close();
        fs.close();
        System.out.println("文件创建成功！");
    }

    //文件重命名
    public static void rename(String oldName, String newName, String hdfs_address) throws IOException {
        FileSystem fs = getConf(hdfs_address);
        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);
        boolean isok = fs.rename(oldPath, newPath);
        if (isok) {
            System.out.println("rename ok!");
        } else {
            System.out.println("rename failure");
        }
        fs.close();
    }

    //删除文件
    public static void delete(String filePath,String hdfs_address) throws IOException {
        FileSystem fs = getConf(hdfs_address);
        Path path = new Path(filePath);
        boolean isok = fs.deleteOnExit(path);
        if (isok) {
            System.out.println("delete ok!");
        } else {
            System.out.println("delete failure");
        }
        fs.close();
    }

    //创建目录
    public static void mkdir(String path,String hdfs_address) throws IOException {
        FileSystem fs = getConf(hdfs_address);
        Path srcPath = new Path(path);
        boolean isok = fs.mkdirs(srcPath);
        if (isok) {
            System.out.println("create dir ok!");
        } else {
            System.out.println("create dir failure");
        }
        fs.close();
    }

    //读取文件的内容
    public static void readFile(String filePath,String hdfs_address) throws IOException {
        FileSystem fs = getConf(hdfs_address);
        Path srcPath = new Path(filePath);
        InputStream in = null;
        try {
            in = fs.open(srcPath);
            IOUtils.copyBytes(in, System.out, 4096, false); //复制到标准输出流
        } finally {
            IOUtils.closeStream(in);
        }
    }


    public static void main(String[] args) throws IOException {
        String hdfs_address = "hdfs://cmserver:8020";
        //测试上传文件
        uploadFile("/root/config.properties", "/config.properties",hdfs_address);
        //测试创建文件
        byte[] contents =  "hello world 世界你好\n".getBytes();
        createFile("/d.txt",contents,hdfs_address);
        //测试重命名
        rename("/d.txt", "/dd.txt",hdfs_address);
        createFile("/d.txt",contents,hdfs_address);
        //测试删除文件
        //delete("/config.properties");
        delete("/aa",hdfs_address);
        //delete("test1");    //删除目录
        //测试新建目录
        mkdir("/test1",hdfs_address);
        //测试读取文件
        readFile("/d.txt",hdfs_address);
    }
}
