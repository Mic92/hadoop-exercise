package solutions.assignment5;

import com.google.common.net.InetAddresses;
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
import solutions.JobUtils;

import java.io.IOException;

public class MapRedSolution5
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


    public static void main(String[] args) throws Exception {
        final Configuration conf = new Configuration();

        final String[] otherArgs =
            new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: MapRedSolution5 <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "MapRed Solution #5");

        /* your code goes in here*/

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        MapRedFileUtils.deleteDir(otherArgs[1]);
        JobUtils.runJobs(job);
    }
}
