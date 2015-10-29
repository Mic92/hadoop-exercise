package examples.invertedindex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * Treats keys as filename and value as line.
 */
public class LineRecordReaderFilename extends RecordReader<Text, Text>
{
    private LineRecordReader lineRecordReader;
    private Text filename;

    public LineRecordReaderFilename()
    {
        lineRecordReader = new LineRecordReader();
    }

    @Override
    public Text getCurrentKey()
    {
        return filename;
    }

    @Override
    public void close() throws IOException
    {
        lineRecordReader.close();
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException
    {
        return lineRecordReader.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException
    {
        return lineRecordReader.getProgress();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException
    {
        filename = new Text(((FileSplit) split).getPath().toString());
        lineRecordReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
        return lineRecordReader.nextKeyValue();
    }
}
