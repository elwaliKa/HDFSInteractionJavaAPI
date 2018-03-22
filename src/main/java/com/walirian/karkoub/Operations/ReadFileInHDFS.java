package com.walirian.karkoub.Operations;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.commons.io.IOUtils;


import java.io.IOException;
import java.util.logging.Logger;

public class ReadFileInHDFS {

    private static  final Logger logger = Logger.getLogger("com.walirian.karkoub.Operations.ReadFileInHDFS");
    public ReadFileInHDFS(FileSystem fs, String path, String fileName) throws IOException {
        logger.info(" start reading the file from hdfs");

        Path pathRead = new Path(path + "/" + fileName);
        FSDataInputStream inputStream = fs.open(pathRead);
        org.apache.hadoop.io.IOUtils.copyBytes(inputStream,System.out,4096,false);
        logger.info("hdfs file read complete");
        inputStream.close();

    }
}
