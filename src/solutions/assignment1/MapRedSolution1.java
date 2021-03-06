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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.xbill.DNS.Type;
import solutions.FrequencyReducer;
import solutions.JobUtils;

import java.io.IOException;
import java.util.StringTokenizer;

import static solutions.JobUtils.configureMapReduce;


public class MapRedSolution1 {
    public static class GetHosts extends Mapper<Text, DNSRecordIO, IntWritable, NullWritable> {
        final IntWritable level = new IntWritable(0);
        @Override
        protected void map(Text key, DNSRecordIO record, Context context) throws IOException, InterruptedException {
            // only address records, not nameserver delegations:
            // https://auditorium.inf.tu-dresden.de/de/questions/3465#answer_3550
            if (record.getType().get() == Type.A || record.getType().get() == Type.AAAA) {
                final String name = record.getRawRecord().getName();
                int dots = new StringTokenizer(name, ".").countTokens();
                level.set(dots);
                context.write(level, NullWritable.get());
            }
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
        configureMapReduce(job,
                GetHosts.class,
                new Path(otherArgs[0]),
                DNSFileInputFormat.class,
                IntWritable.class,
                NullWritable.class,
                FrequencyReducer.class,
                new Path(otherArgs[1]),
                TextOutputFormat.class,
                IntWritable.class,
                IntWritable.class);

        JobUtils.runJobs(job);
    }
}
