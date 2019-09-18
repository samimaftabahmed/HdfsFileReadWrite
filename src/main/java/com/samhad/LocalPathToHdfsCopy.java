package com.samhad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;

public class LocalPathToHdfsCopy extends Configured implements Tool {

    private static final String FS_PARAM_VALUE = "fs.defaultFS";

    public static void main(String[] args) {

        try {
            int res = ToolRunner.run(new Configuration(), new LocalPathToHdfsCopy(), args);
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
            System.err.println("Usage: hadoop jar HdfsFileReadWrite.jar com.samhad.HdfsToLocalPathCopy/LocalPathToHdfsCopy </local-input-path/fileA.xyz> </output-path/fileZ.xyz>");
            return 2;
        }

        String localInputPath = args[0];
        Path outputPath = new Path(args[1]);
        System.out.println("Configured FileSystem = " + configuration.get(FS_PARAM_VALUE));
        FileSystem fileSystem = FileSystem.get(configuration);

        if (fileSystem.exists(outputPath)) {

            System.err.println("Output path already exists");
            return 1;
        }

        OutputStream outputStream = fileSystem.create(outputPath);
        InputStream inputStream = new BufferedInputStream(new FileInputStream(localInputPath));
        IOUtils.copyBytes(inputStream, outputStream, configuration);

        return 0;
    }
}
