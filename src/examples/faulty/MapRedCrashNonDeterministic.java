package examples.faulty;

import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import examples.MapRedFileUtils;
import examples.invertedindex.TextInputFormatFilename;

public class MapRedCrashNonDeterministic
{
    public static class MapRecords extends Mapper<Text, Text, Text, Text>
    {
        private static Random random = new Random();
        private Text word = new Text();
        private Text path = new Text();

        @Override
        protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException
        {
            path.set(key);
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens())
            {
                String str = itr.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase();
                word.set(str);

                if (random.nextInt(1000) == 13)
                throw new RuntimeException("NonDeterministic Exception");

                context.write(word, path);
            }
        }
    }

    public static class ReduceRecords extends Reducer<Text, Text, Text, Text>
    {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
        {
            StringBuffer locations = new StringBuffer("");

            for (Text value : values)
            locations.append(value.toString() + ",");

            context.write(key, new Text(locations.toString()));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2)
        {
            System.err.println("Usage: MapRedCrashNonDeterministic <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Inverted Index (CrashNonDeterministic)");

        job.setInputFormatClass(TextInputFormatFilename.class);

        job.setMapperClass(MapRecords.class);
        job.setCombinerClass(ReduceRecords.class);
        job.setReducerClass(ReduceRecords.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        MapRedFileUtils.deleteDir(otherArgs[1]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
