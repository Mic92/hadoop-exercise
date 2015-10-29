package examples.faulty;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SkipBadRecords;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import examples.MapRedFileUtils;
import examples.invertedindex.TextInputFormatFilename;

public class MapRedCrashDeterministicSkip
{
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        SkipBadRecords.setMapperMaxSkipRecords(conf, Long.MAX_VALUE);
        SkipBadRecords.setAttemptsToStartSkipping(conf, 10);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2)
        {
            System.err.println("Usage: MapRedCrashDeterministicSkip <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Inverted Index (MapRedCrashDeterministicSkip)");

        job.setInputFormatClass(TextInputFormatFilename.class);

        job.setMapperClass(MapRedCrashDeterministic.MapRecords.class);
        job.setCombinerClass(MapRedCrashDeterministic.ReduceRecords.class);
        job.setReducerClass(MapRedCrashDeterministic.ReduceRecords.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        MapRedFileUtils.deleteDir(otherArgs[1]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
