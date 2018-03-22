package com.walirian.karkoub.Operations;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.logging.Logger;

public class CreateDirHDFS {


    private static  final Logger  logger = Logger.getLogger("com.walirian.karkoub.Operations.CreateDirHDFS");

    public CreateDirHDFS(FileSystem fs, String path) throws IOException {

        Path folderpath = new Path(path);

        if(!fs.exists(folderpath)){

            fs.mkdirs(folderpath);
            logger.info("Path "+path+" created.");
        }

        else {
            logger.info("Folder Already Exist !! ");
        }



    }
}
