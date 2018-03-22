package com.walirian.karkoub.Operations;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.logging.Logger;

public class AddFileToHDFS {


    private static  final Logger logger = Logger.getLogger("com.walirian.karkoub.Operations.AddFileToHDFS");

    public AddFileToHDFS(FileSystem fs,String filename,String fileContent, String path) throws IOException{
        logger.info(" begin adding file operation  ");

        Path folderPath = new Path(path + "/" + filename);
        FSDataOutputStream outputStream = fs.create(folderPath);
        outputStream.writeBytes(fileContent);
        outputStream.close();

        logger.info("file added to hdfs file");

    }
}
