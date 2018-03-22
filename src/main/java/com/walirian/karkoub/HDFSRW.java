package com.walirian.karkoub;
import com.walirian.karkoub.Operations.AddFileToHDFS;
import com.walirian.karkoub.Operations.CreateDirHDFS;
import com.walirian.karkoub.Operations.ReadFileInHDFS;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.util.logging.Logger;



public class HDFSRW {
    private static final Logger logger = Logger.getLogger("com.walirian.karkoub.HDFSRW");

    public static void main(String[] args) throws IOException {

        String hdfsuri = "hdfs://horton.training.walirian.com:8020";
        //====variable used to create directory and file===
        String path="/HadoopChalenge1ElwaliKa";
        String fileName="HadoopChallengeElwali.csv";
        String fileContent="hello,Hadoop";
        //====variables used to read file in hdfs root "/"===
        String pathRead = "";
        String fileRead = "test.txt";

        // ====== Initialisatio HDFS File System Object and configuration===
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsuri);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        //==Set HADOOP user==
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/");
        //==Get the filesystem - HDFS==
        FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);
 
        //instances to use the functions to interact with hdfs
        CreateDirHDFS cr = new CreateDirHDFS(fs,path);
        AddFileToHDFS ad = new AddFileToHDFS(fs,fileName,fileContent,path);
       // ReadFileInHDFS rf = new ReadFileInHDFS(fs,pathRead,fileRead);

       fs.close();

    }

    }

