package examples.dns;

import java.io.IOException;
import java.util.StringTokenizer;

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
import org.xbill.DNS.Type;

import examples.MapRedFileUtils;

/**
 * A Map/Reduce job to get a list of A RRs that do have an IP address in the rdata section
 */
public class MapRedARRs
{
    public static class MapRecords extends Mapper<Text, DNSRecordIO, Text, DNSRecordIO>
    {
        @Override
        protected void map(Text key, DNSRecordIO record, Context context) throws IOException, InterruptedException
        {
            if (record.getType().get() == Type.A)
            {
                String rdata = record.getRdata().toString();
                StringTokenizer st = new StringTokenizer(rdata, ".");
                if (st.countTokens() != 4) return;
                for (int i = 0; i<rdata.length(); i++)
                if ((rdata.charAt(i)<'0' || rdata.charAt(i)>'9') &&
                rdata.charAt(i)!='.') return;
                context.write(new Text(record.getRecordKey()), record);
            }
        }
    }

    public static class ReduceRecords extends Reducer<Text, DNSRecordIO, DNSRecordIO, NullWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<DNSRecordIO> values, Context context)
            throws IOException, InterruptedException
        {
            context.write(values.iterator().next(), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2)
        {
            System.err.println("Usage: MapRedAWithIP <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "A RRs with IP Addresses");

        job.setInputFormatClass(DNSFileInputFormat.class);

        job.setJarByClass(MapRedARRs.class);

        job.setMapperClass(MapRecords.class);
        job.setReducerClass(ReduceRecords.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DNSRecordIO.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        MapRedFileUtils.deleteDir(otherArgs[1]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
