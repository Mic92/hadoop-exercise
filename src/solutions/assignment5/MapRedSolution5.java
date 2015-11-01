package solutions.assignment5;

import examples.dns.DNSFileInputFormat;
import examples.dns.DNSRecordIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.util.GenericOptionsParser;
import org.xbill.DNS.Type;

import java.io.IOException;

import static solutions.JobUtils.configureMap;
import static solutions.JobUtils.runJobs;

public class MapRedSolution5 {
    final static Path cnameListPath = new Path("solution5-cnameindex");
    final static Path cnameChainPath = new Path("solution5-chain0");

    public static class IndexCnames extends Mapper<Text, DNSRecordIO, Text, NullWritable> {
        @Override
        protected void map(Text key, DNSRecordIO record, Mapper.Context context) throws IOException, InterruptedException {
            if (record.getType().get() == Type.CNAME) {
                context.write(record.getName(), record.getRdata());
            }
        }
    }

    public static class ReverseIndexCnames extends Mapper<Text, DNSRecordIO, Text, NullWritable> {
        @Override
        protected void map(Text key, DNSRecordIO record, Mapper.Context context) throws IOException, InterruptedException {
            if (record.getType().get() == Type.CNAME) {
                context.write(record.getRdata(), record.getName());
            }
        }
    }

    public static class BuildCnameChain extends Mapper<Text, TupleWritable, Text, Text> {
        @Override
        protected void map(Text key, TupleWritable tuple, Context context) throws IOException, InterruptedException {
            if (!(tuple.has(0) && tuple.has(1))) {
                return;
            }
            final Text chain = (Text)tuple.get(0), target = (Text) tuple.get(1);
            context.write(target, new Text(chain + ";" + key));
        }
    }

    public static class FormatCnameChain extends Mapper<Text, Text, Text, NullWritable> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text(";" + value.toString()), NullWritable.get());
        }
    }

    public static void buildCnameChains(Path destination, int n) throws IOException, ClassNotFoundException, InterruptedException {
        final Configuration joinConf = new Configuration();
        Path inputPath = cnameChainPath;
        for (int i = 1; i <= n; i++) {

            joinConf.set("mapreduce.join.expr", CompositeInputFormat.compose(
                    "inner", KeyValueTextInputFormat.class,
                    inputPath, cnameListPath
            ));
            final Job buildChainJob = Job.getInstance(joinConf, "MapRed Solution #5: Build Chain "+i);
            final Path chainIntermediatePath = Path.mergePaths(destination, new Path("/chain" + i + "-intermediate"));
            configureMap(buildChainJob,
                    BuildCnameChain.class,
                    inputPath,
                    CompositeInputFormat.class,
                    Text.class,
                    Text.class,
                    chainIntermediatePath,
                    true);

            final Path chainPath = Path.mergePaths(destination, new Path("/chain" + i));
            final Job formatChainJob = Job.getInstance(joinConf, "MapRed Solution #5: Write Chain "+i);
            configureMap(formatChainJob,
                    FormatCnameChain.class,
                    chainIntermediatePath,
                    KeyValueTextInputFormat.class,
                    Text.class,
                    NullWritable.class,
                    chainPath,
                    false);
            runJobs(buildChainJob, formatChainJob);
            inputPath = chainIntermediatePath;
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

        final Job indexJob = Job.getInstance(conf, "MapRed Solution #5: Cname list");
        configureMap(indexJob,
                IndexCnames.class,
                new Path(otherArgs[0]),
                DNSFileInputFormat.class,
                Text.class,
                Text.class,
                cnameListPath,
                true);

        final Job reverseIndexJob = Job.getInstance(conf, "MapRed Solution #5: Cname Chain 0");
        configureMap(reverseIndexJob,
                ReverseIndexCnames.class,
                new Path(otherArgs[0]),
                DNSFileInputFormat.class,
                Text.class,
                Text.class,
                cnameChainPath,
                true);
        runJobs(indexJob, reverseIndexJob);

        buildCnameChains(new Path(otherArgs[1]), 3);
    }
}
