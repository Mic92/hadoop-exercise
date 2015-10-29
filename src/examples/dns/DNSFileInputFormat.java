package examples.dns;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.RecordReader;

/**
 * A {@link FileInputFormat} for DNS files. Files are broken into lines. Each
 * line is interpreted and parsed as {@link DNSRecordIO}. The key represents the
 * filename, the {@link DNSRecordIO} is provided in the value.
 */
public class DNSFileInputFormat extends FileInputFormat<Text, DNSRecordIO>
{
    public RecordReader<Text, DNSRecordIO> createRecordReader(InputSplit split,
        TaskAttemptContext context) throws IOException, InterruptedException
    {
        return new DNSRecordIOReader();
    }
}
