package com.nova.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.BufferedReader;
import java.io.IOException;

import java.io.InputStreamReader;

/**
 * Created by yunchen on 2017/3/17.
 */
public class CompressUtils {

    private static String FS;
    private static String CODEC_SUFFIX;
    private static String CODEC;

    public CompressUtils(String fs,String lzo_suffix,String lzo){
        FS = fs;
        CODEC_SUFFIX = lzo_suffix;
        CODEC = lzo;
    }

    public static void compress(String inputPath, String outputPath) {
        System.out.println("[input]: " + inputPath);
        System.out.println("[output]: " + outputPath);

        if (StringUtils.isBlank(inputPath) || StringUtils.isBlank(outputPath)){
            System.out.println("[input] or [output] must not be blank");
            return;
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", FS);

        try {
            FileSystem fs = FileSystem.get(conf);
            Path input = new Path(inputPath);

            // 1. check input path exist
            if (!fs.exists(input)) {
                System.out.println("input path not exist");
                return;
            }

            // 2. check output path must a dir
            if (!StringUtils.endsWith(outputPath,"/")){
                System.out.println("output path must be a dir , end with '/' ");
                return;
            }

            // 3. check input path is dir ,if it is ,foreach write
            FileStatus stat = fs.getFileStatus(input);
            if (stat.isFile()) {
                write(fs,conf,stat,outputPath);
            } else if (stat.isDirectory()) {
                FileStatus[] subInputFile = fs.listStatus(input);
                for (FileStatus fileStatus : subInputFile) {
                    if (fileStatus.isFile()) {
                        write(fs,conf,fileStatus,outputPath);
                    } else {
                        System.out.println("input path must be iter by once");
                        break;
                    }
                }
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

    }

    private static void write(FileSystem fs, Configuration conf,FileStatus fileStatus,String outputPath) throws IOException{
        String in = fileStatus.getPath().toString();
        String out = outputPath + fileStatus.getPath().getName() + CODEC_SUFFIX;
        checkParentDir(fs, out);
        System.out.println(in + "---->" + out);
        writeFile(fs, conf, in, out);
    }


    private static void checkParentDir(FileSystem fs, String out) throws IOException {
        Path parent = new Path(out).getParent();
        if (!fs.exists(parent)) {
            System.out.println("output path dir not exist [" + parent + ",] now mkdir it");
            fs.mkdirs(parent, FsPermission.getDirDefault());
        }
    }


    private static void writeFile(FileSystem fs, Configuration conf, String in, String out) {
        FSDataInputStream inputStream = null;
        SequenceFile.Writer writer = null;
        try {
            inputStream = fs.open(new Path(in));
            Path seqFile = new Path(out);

            BufferedReader buff = new BufferedReader(new InputStreamReader(inputStream));

            BytesWritable EMPTY_KEY = new BytesWritable();//   key

            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(Class.forName(CODEC), conf);
            writer = SequenceFile.createWriter(conf,
                    SequenceFile.Writer.file(seqFile), SequenceFile.Writer.keyClass(BytesWritable.class),
                    SequenceFile.Writer.valueClass(Text.class),
                    SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, codec));

            String str = "";
            System.out.println("begin write " + out);
            while ((str = buff.readLine()) != null) {
                writer.append(EMPTY_KEY, new Text(str));
            }
            System.out.println("done write " + out);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(inputStream);
            IOUtils.closeStream(writer);
        }
    }


}
