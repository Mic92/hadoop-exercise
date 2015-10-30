package solutions.assignment4;

import examples.dns.DNSFileInputFormat;
import examples.dns.DNSRecordIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import examples.MapRedFileUtils;
import org.xbill.DNS.Type;
import solutions.assignment3.MapRedSolution3;

import java.io.IOException;

public class MapRedSolution4
{
    public static class ExtractCnameWithIPs extends Mapper<Text, DNSRecordIO, Text, NullWritable> {
        @Override
        protected void map(Text key, DNSRecordIO record, Mapper.Context context) throws IOException, InterruptedException {
            final String rdata = record.getRdata().toString();
            // one does not simply parse inet addresses with regex
            if (record.getType().get() == Type.CNAME && InetAddresses.isInetAddress(rdata)) {
                context.write(new Text(record.getRawRecord().toString()), NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        String[] otherArgs =
            new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2)
        {
            System.err.println("Usage: MapRedSolution4 <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "MapRed Solution #4");

        job.setInputFormatClass(DNSFileInputFormat.class);

        job.setMapperClass(ExtractCnameWithIPs.class);
        job.setReducerClass(MapRedSolution3.WriteThrough.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        MapRedFileUtils.deleteDir(otherArgs[1]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
