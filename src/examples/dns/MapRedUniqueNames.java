package examples.dns;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import examples.MapRedFileUtils;

/**
 * A Map/Reduce job to get a list of unique DNS names
 */
public class MapRedUniqueNames
{
    public static class MapRecords extends Mapper<Text, DNSRecordIO, Text,
        NullWritable>
    {
        @Override
        protected void map(Text key, DNSRecordIO record, Context context)
            throws IOException, InterruptedException
        {
            context.write(record.getName(), NullWritable.get());
        }
    }

    public static class ReduceRecords extends
                    Reducer<Text, NullWritable, Text, NullWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values,
            Context context) throws IOException, InterruptedException
        {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2)
        {
            System.err.println("Usage: MapRedUniqueNames <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Unique DNS names");

        job.setInputFormatClass(DNSFileInputFormat.class);

        job.setJarByClass(MapRedUniqueNames.class);

        job.setMapperClass(MapRecords.class);
        job.setReducerClass(ReduceRecords.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        MapRedFileUtils.deleteDir(otherArgs[1]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
