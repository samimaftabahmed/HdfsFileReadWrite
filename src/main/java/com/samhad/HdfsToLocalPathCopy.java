package com.samhad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

public class HdfsToLocalPathCopy extends Configured implements Tool {

    private static final String FS_PARAM_VALUE = "fs.defaultFS";


    public static void main(String[] args) {

        try {
            int res = ToolRunner.run(new Configuration(), new HdfsToLocalPathCopy(), args);
            System.exit(res);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration = this.getConf();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: hadoop jar HdfsFileReadWrite.jar com.samhad.HdfsWriter/HdfsToLocalPathCopy/LocalPathToHdfsCopy </input-path/fileA.xyz> </local-output-path/fileZ.xyz>");
            return 2;
        }

        Path inputPath = new Path(args[0]);
        String localOutputPath = args[1];
        System.out.println("Configured FileSystem = " + configuration.get(FS_PARAM_VALUE));

        FileSystem fs = FileSystem.get(configuration);
        InputStream inputStream = fs.open(inputPath);
        OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(localOutputPath));
        IOUtils.copyBytes(inputStream, outputStream, configuration);

        return 0;
    }
}
