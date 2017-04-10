package com.nova.compress;

import com.nova.utils.CompressUtils;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;

/**
 * Created by yunchen on 2017/3/17.
 */
public class main {

    /**
     * 入口函数,抛出任何异常
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException {

        final String CODEC = "org.apache.hadoop.io.CompressTest.SnappyCodec";

        String FS = "hdfs://cmname1:8020";

        String HDFS_SCHEMA = "hdfs://";

        final String LZO_SUFFIX = ".snappy";

        if (args != null && args.length < 2) {
            System.out.println("[stop] application");
            return;
        }

        String in = "";
        String out = "";
        if (!StringUtils.startsWithIgnoreCase(args[0],HDFS_SCHEMA)){
            in = FS + args[1];
        }
        if (!StringUtils.startsWithIgnoreCase(args[1],HDFS_SCHEMA)){
            out = FS + args[2];
        }

        CompressUtils cu = new CompressUtils(FS,LZO_SUFFIX,CODEC);

        cu.compress(in, out);

        System.out.println("[end] application");

    }

}
