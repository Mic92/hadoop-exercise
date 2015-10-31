package solutions.assignment7;

import examples.apachelogs.AccessLogFormat;
import examples.apachelogs.AccessLogIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import examples.MapRedFileUtils;
import solutions.FrequencyReducer;
import solutions.JobUtils;

import java.io.IOException;

public class MapRedSolution7
{
    public static class ParseLogs extends Mapper<Text, AccessLogIO, Text, NullWritable> {
        @Override
        protected void map(Text key, AccessLogIO record, Mapper.Context context) throws IOException, InterruptedException {
            context.write(record.getUri(), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        final Configuration conf = new Configuration();

        final String[] otherArgs =
            new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: MapRedSolution7 <in> <out>");
            System.exit(2);
        }

        final Job job = Job.getInstance(conf, "MapRed Solution #7");

        JobUtils.configureJob(job,
                ParseLogs.class,
                AccessLogFormat.class,
                Text.class,
                NullWritable.class,
                FrequencyReducer.class,
                TextOutputFormat.class,
                Text.class,
                IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        MapRedFileUtils.deleteDir(otherArgs[1]);
        JobUtils.runJobs(job);
    }
}
