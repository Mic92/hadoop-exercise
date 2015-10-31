package solutions.assignment1;

import examples.dns.DNSFileInputFormat;
import examples.dns.DNSRecordIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import examples.MapRedFileUtils;
import org.xbill.DNS.Type;
import solutions.FrequencyReducer;
import solutions.JobUtils;
import solutions.WriteKeyReducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class MapRedSolution1
{
    public static class UniqueDomainsMapper extends Mapper<Text, DNSRecordIO, Text, NullWritable> {
        @Override
        protected void map(Text key, DNSRecordIO record, Context context) throws IOException, InterruptedException {
            if (record.getType().get() == Type.NS) {
                context.write(record.getName(), NullWritable.get());
            }
        }
    }

    public static class SummationMap extends Mapper<Text, NullWritable, IntWritable, NullWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void map(Text key, NullWritable ignored, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(key.toString(), ".");
            result.set(tokenizer.countTokens());
            context.write(result, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        final Configuration conf = new Configuration();

        final String[] otherArgs =
            new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: MapRedSolution1 <in> <out>");
            System.exit(2);
        }

        final Job job = Job.getInstance(conf, "MapRed Solution #1");
        JobUtils.configureJob(job,
                UniqueDomainsMapper.class,
                DNSFileInputFormat.class,
                Text.class,
                NullWritable.class,
                WriteKeyReducer.class,
                SequenceFileOutputFormat.class,
                Text.class,
                NullWritable.class);

        MapRedFileUtils.deleteDir("temp-output");
        final Path tempOutput = new Path("temp-output");
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        SequenceFileOutputFormat.setOutputPath(job, tempOutput);

        Job job2 = Job.getInstance(conf, "Summation");
        JobUtils.configureJob(job,
                SummationMap.class,
                SequenceFileInputFormat.class,
                IntWritable.class,
                NullWritable.class,
                FrequencyReducer.class,
                TextOutputFormat.class,
                IntWritable.class,
                IntWritable.class);
        SequenceFileInputFormat.addInputPath(job2, tempOutput);

        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));

        MapRedFileUtils.deleteDir(otherArgs[1]);
        JobUtils.runJobs(job, job2);
    }
}
