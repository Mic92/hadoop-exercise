package solutions.assignment1;

import examples.dns.DNSFileInputFormat;
import examples.dns.DNSRecordIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import examples.MapRedFileUtils;
import org.xbill.DNS.Type;

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

    public static class UniqueDomainsReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
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

    public static class SummationReduce extends Reducer<IntWritable, NullWritable, IntWritable, IntWritable> {
        private IntWritable result = new IntWritable();
        @Override
        protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Object v: values) {
                sum++;
            }
            result.set(sum);
            context.write(key, result);
        }

    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        String[] otherArgs =
            new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2)
        {
            System.err.println("Usage: MapRedSolution1 <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "MapRed Solution #1");
        job.setInputFormatClass(DNSFileInputFormat.class);

        job.setMapperClass(UniqueDomainsMapper.class);
        //job.setCombinerClass(UniqueDomainsReducer.class);
        job.setReducerClass(UniqueDomainsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        final Path tempOutput = new Path("temp-output");
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        SequenceFileOutputFormat.setOutputPath(job, tempOutput);

        MapRedFileUtils.deleteDir("temp-output");
        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "Summation");
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job2, tempOutput);

        job2.setMapperClass(SummationMap.class);
        //job2.setCombinerClass(SummationReduce.class);
        job2.setReducerClass(SummationReduce.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(NullWritable.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));

        MapRedFileUtils.deleteDir(otherArgs[1]);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
