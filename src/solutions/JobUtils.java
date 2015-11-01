package solutions;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public abstract class JobUtils {
    public static void configureMap(Job j,
                                    Class<? extends Mapper> mapper,
                                    Class<? extends InputFormat> inFormat,
                                    Class<? extends Writable> mapperKey,
                                    Class<? extends Writable> mapperValue,
                                    boolean sort) {
        j.setMapperClass(mapper);
        j.setInputFormatClass(inFormat);
        j.setOutputKeyClass(mapperKey);
        j.setOutputValueClass(mapperValue);
        if (!sort) {
            j.setNumReduceTasks(0);
        }
    }
    public static void configureMapReduce(Job j,
                                          Class<? extends Mapper> mapper,
                                          Class<? extends InputFormat> inFormat,
                                          Class<? extends Writable> mapperKey,
                                          Class<? extends Writable> mapperValue,
                                          Class<? extends Reducer> reducer,
                                          Class<? extends OutputFormat> outFormat,
                                          Class<? extends Writable> outputKey,
                                          Class<? extends Writable> outputValue) {

        j.setMapperClass(mapper);
        j.setInputFormatClass(inFormat);
        j.setMapOutputKeyClass(mapperKey);
        j.setMapOutputValueClass(mapperValue);
        j.setReducerClass(reducer);
        j.setOutputFormatClass(outFormat);
        j.setOutputKeyClass(outputKey);
        j.setOutputValueClass(outputValue);
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
