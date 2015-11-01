package solutions.assignment6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.record.Record;
import org.apache.hadoop.record.RecordInput;
import org.apache.hadoop.record.RecordOutput;

import java.io.IOException;
import java.util.Date;

public class AccessLogIO extends Record {
    private AccessLogRecord rec;

    @Override
    public void serialize(RecordOutput r, String tag) throws IOException {
        r.startRecord(this, tag);
        r.writeString(rec.getIp(), tag);
        r.writeString(rec.getUsername(), tag);
        r.writeLong(rec.getDate().getTime(), tag);
        r.writeString(rec.getMethod(), tag);
        r.writeString(rec.getUri(), tag);
        r.writeInt(rec.getCode(), tag);
        r.writeLong(rec.getSize(), tag);
        r.writeString(rec.getReferrer(), tag);
        r.writeString(rec.getBrowser(), tag);
        r.endRecord(this, tag);
    }

    @Override
    public void deserialize(RecordInput in, String tag) throws IOException {
        in.startRecord(tag);
        rec.setIp(in.readString(tag));
        rec.setUsername(in.readString(tag));
        rec.setDate(new Date(in.readLong(tag)));
        rec.setMethod(in.readString(tag));
        rec.setUri(in.readString(tag));
        rec.setCode(in.readInt(tag));
        rec.setSize(in.readLong(tag));
        rec.setReferrer(in.readString(tag));
        rec.setBrowser(in.readString(tag));
        in.endRecord(tag);
    }

    @Override
    public int compareTo(Object peer) throws ClassCastException {
        return rec.compareTo(peer);
    }

    public void setRawRecord(AccessLogRecord rec) {
        this.rec = rec;
    }
    public AccessLogRecord getRawRecord() {
        return rec;
    }

    public void setBrowser(Text browser) {
        rec.setBrowser(browser.toString());
    }

    public Text getIp() {
        return new Text(rec.getIp());
    }

    public Text getUsername() {
        return new Text(rec.getUsername());
    }

    public LongWritable getDate() {
        return new LongWritable(rec.getDate().getTime());
    }

    public Text getMethod() {
        return new Text(rec.getMethod());
    }

    public Text getUri() {
        return new Text(rec.getUri());
    }

    public IntWritable getCode() {
        return new IntWritable(rec.getCode());
    }

    public LongWritable getSize() {
        return new LongWritable(rec.getSize());
    }

    public Text getReferrer() {
        return new Text(rec.getReferrer());
    }

    public Text getBrowser() {
        return new Text(rec.getBrowser());
    }

    public void setIp(Text ip) {
        rec.setIp(ip.toString());
    }

    public void setUsername(Text username) {
        rec.setUsername(username.toString());
    }

    public void setDate(LongWritable date) {
        rec.setDate(new Date(date.get()));
    }

    public void setMethod(Text method) {
        rec.setMethod(method.toString());
    }

    public void setUri(Text uri) {
        rec.setUri(uri.toString());
    }

    public void setCode(IntWritable code) {
        rec.setCode(code.get());
    }

    public void setSize(LongWritable size) {
        rec.setSize(size.get());
    }

    public void setReferrer(Text referrer) {
        rec.setReferrer(referrer.toString());
    }
}
