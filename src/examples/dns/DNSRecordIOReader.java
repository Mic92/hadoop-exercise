package examples.dns;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import examples.invertedindex.LineRecordReaderFilename;

/**
 * This class treats lines in a DNS file as {@link DNSRecordIO} objects.
 */
public class DNSRecordIOReader extends RecordReader<Text, DNSRecordIO>
{
    private LineRecordReaderFilename lineRecordReaderFilename;
    private DNSRecordIO dnsRecordIO;
    private static final Log log = LogFactory.getLog(DNSRecordIOReader.class);

    public DNSRecordIOReader()
    {
        lineRecordReaderFilename = new LineRecordReaderFilename();
    }

    @Override
    public Text getCurrentKey()
    {
        return lineRecordReaderFilename.getCurrentKey();
    }

    @Override
    public void close() throws IOException
    {
        lineRecordReaderFilename.close();
    }

    @Override
    public DNSRecordIO getCurrentValue() throws IOException, InterruptedException
    {
        return dnsRecordIO;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException
    {
        return lineRecordReaderFilename.getProgress();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException
    {
        lineRecordReaderFilename.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
        boolean retvalue = lineRecordReaderFilename.nextKeyValue();

        if (retvalue)
        {
            Text line = lineRecordReaderFilename.getCurrentValue();
            DNSRecord dnsRecord = DNSRecord.createRecord(line.toString(), log);
            dnsRecordIO = new DNSRecordIO();
            dnsRecordIO.setRawRecord(dnsRecord);
        }

        return retvalue;
    }
}