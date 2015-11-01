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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import solutions.FrequencyReducer;
import solutions.JobUtils;

import java.io.IOException;

import static solutions.JobUtils.configureMapReduce;

public class MapRedSolution7 {
    final static String HOST = "http://localhost";
    public static class ParseLogs extends Mapper<Text, AccessLogIO, Text, NullWritable> {
        private final Text uri = new Text();
        @Override
        protected void map(Text key, AccessLogIO record, Mapper.Context context) throws IOException, InterruptedException {
            // extend to valid URI according to https://auditorium.inf.tu-dresden.de/de/questions/3471#answer_3551
            uri.set(HOST + record.getRawRecord().getUri());
            context.write(uri, NullWritable.get());
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
        configureMapReduce(job,
                ParseLogs.class,
                new Path(otherArgs[0]),
                AccessLogFormat.class,
                Text.class,
                NullWritable.class,
                FrequencyReducer.class,
                new Path(otherArgs[1]),
                TextOutputFormat.class,
                Text.class,
                IntWritable.class);
        JobUtils.runJobs(job);
    }
}
