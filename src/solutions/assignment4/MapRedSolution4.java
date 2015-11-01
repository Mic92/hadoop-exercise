package solutions.assignment4;

import examples.dns.DNSFileInputFormat;
import examples.dns.DNSRecord;
import examples.dns.DNSRecordIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import com.google.common.net.InetAddresses;

import examples.MapRedFileUtils;
import org.xbill.DNS.Type;
import solutions.JobUtils;
import solutions.KeyReducer;

import java.io.IOException;

import static solutions.JobUtils.configureMapReduce;

public class MapRedSolution4 {
    public static class ExtractCnameWithIps extends Mapper<Text, DNSRecordIO, Text, NullWritable> {
        @Override
        protected void map(Text key, DNSRecordIO record_, Mapper.Context context) throws IOException, InterruptedException {
            final DNSRecord record = record_.getRawRecord();
            final String rdata = record.getRdata();
            // one does not simply parse addresses with regex
            if (record.getType() != Type.CNAME || rdata.length() < 2) {
                return;
            }
            final String withoutTrailingDot = rdata.substring(0, rdata.length() - 2);
            if (InetAddresses.isInetAddress(withoutTrailingDot)) {
                context.write(new Text(record.toString()), NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final Configuration conf = new Configuration();

        final String[] otherArgs =
            new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: MapRedSolution4 <in> <out>");
            System.exit(2);
        }

        final Job job = Job.getInstance(conf, "MapRed Solution #4");
        configureMapReduce(job,
                ExtractCnameWithIps.class,
                DNSFileInputFormat.class,
                Text.class,
                NullWritable.class,
                KeyReducer.class,
                TextOutputFormat.class,
                Text.class,
                NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        MapRedFileUtils.deleteDir(otherArgs[1]);
        JobUtils.runJobs(job);
    }
}
