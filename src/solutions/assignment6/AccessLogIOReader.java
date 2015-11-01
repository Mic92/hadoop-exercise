package solutions.assignment6;

import examples.invertedindex.LineRecordReaderFilename;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.text.ParseException;

/**
 * This class treats lines in a DNS file as {@link AccessLogIO} objects.
 */
public class AccessLogIOReader extends RecordReader<Text, AccessLogIO> {
    private LineRecordReaderFilename lineRecordReaderFilename;
    private AccessLogIO accessLogIO;
    private static final Log log = LogFactory.getLog(AccessLogIOReader.class);

    public AccessLogIOReader() {
        lineRecordReaderFilename = new LineRecordReaderFilename();
    }

    @Override
    public Text getCurrentKey() {
        return lineRecordReaderFilename.getCurrentKey();
    }

    @Override
    public void close() throws IOException {
        lineRecordReaderFilename.close();
    }

    @Override
    public AccessLogIO getCurrentValue() throws IOException, InterruptedException {
        return accessLogIO;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return lineRecordReaderFilename.getProgress();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        lineRecordReaderFilename.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean retvalue = lineRecordReaderFilename.nextKeyValue();
        while (retvalue) {
            final Text line = lineRecordReaderFilename.getCurrentValue();
            try {
               final AccessLogRecord rec = AccessLogRecord.parse(line.toString());
               accessLogIO = new AccessLogIO();
               accessLogIO.setRawRecord(rec);
               return true;
            } catch (ParseException e) {
               log.error(line, e);
            }
            retvalue = lineRecordReaderFilename.nextKeyValue();
        }
        return false;
    }


}
