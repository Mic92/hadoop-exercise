package examples.apachelogs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class AccessLogFormat extends FileInputFormat<Text, AccessLogIO> {
    @Override
    public RecordReader<Text, AccessLogIO> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new AccessLogIOReader();
    }
}

