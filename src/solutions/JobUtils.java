package solutions;

import examples.MapRedFileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public abstract class JobUtils {
    public static void configureMap(Job j,
                                    Class<? extends Mapper> mapper,
                                    Path inPath,
                                    Class<? extends InputFormat> inFormat,
                                    Class<? extends Writable> mapperKey,
                                    Class<? extends Writable> mapperValue,
                                    Path outPath,
                                    boolean sort) throws IOException {
        j.setMapperClass(mapper);
        j.setInputFormatClass(inFormat);
        j.setOutputKeyClass(mapperKey);
        j.setOutputValueClass(mapperValue);
        if (!sort) {
            j.setNumReduceTasks(0);
        }
        FileInputFormat.addInputPath(j, inPath);
        FileOutputFormat.setOutputPath(j, outPath);
        MapRedFileUtils.deleteDir(outPath.toString());
    }
    public static void configureMapReduce(Job j,
                                          Class<? extends Mapper> mapper,
                                          Path inPath,
                                          Class<? extends InputFormat> inFormat,
                                          Class<? extends Writable> mapperKey,
                                          Class<? extends Writable> mapperValue,
                                          Class<? extends Reducer> reducer,
                                          Path outPath,
                                          Class<? extends OutputFormat> outFormat,
                                          Class<? extends Writable> outputKey,
                                          Class<? extends Writable> outputValue) throws IOException {

        j.setMapperClass(mapper);
        j.setInputFormatClass(inFormat);
        j.setMapOutputKeyClass(mapperKey);
        j.setMapOutputValueClass(mapperValue);
        j.setReducerClass(reducer);
        j.setOutputFormatClass(outFormat);
        j.setOutputKeyClass(outputKey);
        j.setOutputValueClass(outputValue);
        FileInputFormat.addInputPath(j, inPath);
        FileOutputFormat.setOutputPath(j, outPath);
        MapRedFileUtils.deleteDir(outPath.toString());
    }
    public static void runJobs(Job... jobs) throws InterruptedException, IOException, ClassNotFoundException {
        for (final Job job: jobs) {
            if (!job.waitForCompletion(true)) {
                System.err.println("Failed to run job: " + job.toString());
                System.exit(1);
            }
        }
    }
}
