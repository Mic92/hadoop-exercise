package examples.wordcount;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import examples.MapRedFileUtils;

public class MapRedLetterFrequencyCount
{
    public static class MapRecords extends Mapper<LongWritable, Text, Text, LongWritable>
    {
        private final static LongWritable one = new LongWritable(1);
        private Text letter = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens())
            {
                String str = itr.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase();
                for(int i = 0; i<str.length(); i++)
                {
                    letter.set(new byte[] {(byte)str.charAt(i)});
                    context.write(letter, one);
                }
            }
        }
    }

    public static class ReduceRecords extends Reducer<Text, LongWritable, Text, LongWritable>
    {
        private LongWritable result = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,
            Context context) throws IOException, InterruptedException
        {
            int sum = 0;

            for (LongWritable val : values)
            sum += val.get();

            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2)
        {
            System.err.println("Usage: MapRedWordCount <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Letter Frequency");

        job.setMapperClass(MapRecords.class);
        job.setCombinerClass(ReduceRecords.class);
        job.setReducerClass(ReduceRecords.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        MapRedFileUtils.deleteDir(otherArgs[1]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
