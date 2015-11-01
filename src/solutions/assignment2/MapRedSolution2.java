package solutions.assignment2;

import examples.dns.DNSFileInputFormat;
import examples.dns.DNSRecordIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.xbill.DNS.Type;
import solutions.FrequencyReducer;
import solutions.JobUtils;

import java.io.IOException;

import static solutions.JobUtils.configureMapReduce;

public class MapRedSolution2 {
    public static class ExtractIps extends Mapper<Text, DNSRecordIO, Text, NullWritable> {
        @Override
        protected void map(Text key, DNSRecordIO record, Mapper.Context context) throws IOException, InterruptedException {
            if (record.getType().get() == Type.A || record.getType().get() == Type.AAAA) {
                context.write(record.getRdata(), NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final Configuration conf = new Configuration();

        final String[] otherArgs =
            new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: MapRedSolution2 <in> <out>");
            System.exit(2);
        }

        final Job job = Job.getInstance(conf, "MapRed Solution #2");
        configureMapReduce(job,
                ExtractIps.class,
                new Path(otherArgs[0]),
                DNSFileInputFormat.class,
                Text.class,
                NullWritable.class,
                FrequencyReducer.class,
                new Path(otherArgs[1]),
                TextOutputFormat.class,
                Text.class,
                LongWritable.class);

        JobUtils.runJobs(job);
    }
}
