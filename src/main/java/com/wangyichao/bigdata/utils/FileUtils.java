package com.wangyichao.bigdata.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class FileUtils {

    public static void deleteTarget(String output, Configuration configuration) throws Exception{
        FileSystem fileSystem = FileSystem.get(configuration);

        Path outputPath = new Path(output);
        if (fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath,true);
        }

    }
}
