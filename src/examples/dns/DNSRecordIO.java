package examples.dns;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.record.Record;
import org.apache.hadoop.record.RecordInput;
import org.apache.hadoop.record.RecordOutput;

/**
 * A Hadoop IO data type wrapper for {@link DNSRecord}.
 */
public class DNSRecordIO extends Record
{
    private DNSRecord rec = new DNSRecord();

    /**
     * Returns the wrapped {@link DNSRecord}.
     */
    public DNSRecord getRawRecord()
    {
        return rec;
    }

    /**
     * Sets a {@link DNSRecord} for wrapping.
     */
    public void setRawRecord(DNSRecord rec)
    {
        this.rec = rec;
    }

    /**
     * @see DNSRecord#getDClass()
     */
    public IntWritable getDClass()
    {
        return new IntWritable(rec.getDClass());
    }

    /**
     * @see DNSRecord#setDClass(int)
     */
    public void setDClass(IntWritable dclass)
    {
        rec.setDClass(dclass.get());
    }

    /**
     * @see DNSRecord#getDelegations()
     */
    public Text getDelegations()
    {
        return new Text(rec.getDelegations());
    }

    /**
     * @see DNSRecord#setDelegations(String)
     */
    public void setDelegations(Text delegations)
    {
        rec.setDelegations(delegations.toString());
    }

    /**
     * @see DNSRecord#getName()
     */
    public Text getName()
    {
        return new Text(rec.getName());
    }

    /**
     * @see DNSRecord#setName(String)
     */
    public void setName(Text name)
    {
        rec.setName(name.toString());
    }

    /**
     * @see DNSRecord#getMethod()
     */
    public IntWritable getMethod()
    {
        return new IntWritable(rec.getMethod());
    }

    /**
     * @see DNSRecord#setMethod(int)
     */
    public void setMethod(IntWritable method)
    {
        rec.setMethod(method.get());
    }

    /**
     * @see DNSRecord#getRdata()
     */
    public Text getRdata()
    {
        return new Text(rec.getRdata());
    }

    /**
     * @see DNSRecord#setRdata(String)
     */
    public void setRdata(Text record)
    {
        rec.setRdata(record.toString());
    }

    /**
     * @see DNSRecord#getRequestIP()
     */
    public Text getRequestIP()
    {
        return new Text(rec.getRequestIP());
    }

    /**
     * @see DNSRecord#setRequestIP(String)
     */
    public void setRequestIP(Text requestIP)
    {
        rec.setRequestIP(requestIP.toString());
    }

    /**
     * @see DNSRecord#getTimestamp()
     */
    public LongWritable getTimestamp()
    {
        return new LongWritable(rec.getTimestamp());
    }

    /**
     * @see DNSRecord#setTimestamp(long)
     */
    public void setTimestamp(LongWritable timestamp)
    {
        rec.setTimestamp(timestamp.get());
    }

    /**
     * @see DNSRecord#getTTL()
     */
    public LongWritable getTTL()
    {
        return new LongWritable(rec.getTTL());
    }

    /**
     * @see DNSRecord#setTTL(long)
     */
    public void setTTL(LongWritable ttl)
    {
        rec.setTTL(ttl.get());
    }

    /**
     * @see DNSRecord#getType()
     */
    public IntWritable getType()
    {
        return new IntWritable(rec.getType());
    }

    /**
     * @see DNSRecord#setType(int)
     */
    public void setType(IntWritable type)
    {
        rec.setType(type.get());
    }

    /**
     * @see DNSRecord.VivaldiCoordinates#getVivaldiCoord1()
     */
    public LongWritable getVivaldiCoord1()
    {
        return new LongWritable(rec.getVivaldiCoordinates().getVivaldiCoord1());
    }

    /**
     * @see DNSRecord.VivaldiCoordinates#setVivaldiCoord1(long)
     */
    public void setVivaldiCoord1(LongWritable vivaldiCoord)
    {
        rec.getVivaldiCoordinates().setVivaldiCoord1(vivaldiCoord.get());
    }

    /**
     * @see DNSRecord.VivaldiCoordinates#getVivaldiCoord2()
     */
    public LongWritable getVivaldiCoord2()
    {
        return new LongWritable(rec.getVivaldiCoordinates().getVivaldiCoord2());
    }

    /**
     * @see DNSRecord.VivaldiCoordinates#setVivaldiCoord2(long)
     */
    public void setVivaldiCoord2(LongWritable vivaldiCoord)
    {
        rec.getVivaldiCoordinates().setVivaldiCoord2(vivaldiCoord.get());
    }

    /**
     * @see DNSRecord.VivaldiCoordinates#getVivaldiCoord3()
     */
    public LongWritable getVivaldiCoord3()
    {
        return new LongWritable(rec.getVivaldiCoordinates().getVivaldiCoord3());
    }

    /**
     * @see DNSRecord.VivaldiCoordinates#setVivaldiCoord3(long)
     */
    public void setVivaldiCoord3(LongWritable vivaldiCoord)
    {
        rec.getVivaldiCoordinates().setVivaldiCoord1(vivaldiCoord.get());
    }

    /**
     * @see DNSRecord.VivaldiCoordinates#getVivaldiCoord4()
     */
    public LongWritable getVivaldiCoord4()
    {
        return new LongWritable(rec.getVivaldiCoordinates().getVivaldiCoord4());
    }

    /**
     * @see DNSRecord.VivaldiCoordinates#setVivaldiCoord4(long)
     */
    public void setVivaldiCoord4(LongWritable vivaldiCoord)
    {
        rec.getVivaldiCoordinates().setVivaldiCoord4(vivaldiCoord.get());
    }

    /**
     * @see DNSRecord.VivaldiCoordinates#getVivaldiCoord5()
     */
    public LongWritable getVivaldiCoord5()
    {
        return new LongWritable(rec.getVivaldiCoordinates().getVivaldiCoord5());
    }

    /**
     * @see DNSRecord.VivaldiCoordinates#setVivaldiCoord5(long)
     */
    public void setVivaldiCoord5(LongWritable vivaldiCoord)
    {
        rec.getVivaldiCoordinates().setVivaldiCoord5(vivaldiCoord.get());
    }

    /**
     * @see DNSRecord#getZone()
     */
    public Text getZone()
    {
        return new Text(rec.getZone());
    }

    /**
     * @see DNSRecord#setZone(String)
     */
    public void setZone(Text zone)
    {
        rec.setZone(zone.toString());
    }

    public void serialize(RecordOutput rout, String tag) throws IOException
    {
        rout.startRecord(this, tag);
        rout.writeString(rec.getZone(), tag);
        rout.writeString(rec.getName(), tag);
        rout.writeInt(rec.getType(), tag);
        rout.writeInt(rec.getDClass(), tag);
        rout.writeLong(rec.getTTL(), tag);
        rout.writeString(rec.getRdata(), tag);
        rout.writeInt(rec.getMethod(), tag);
        rout.writeString(rec.getDelegations(), tag);
        rout.writeLong(rec.getTimestamp(), tag);
        rout.writeString(rec.getRequestIP(), tag);
        rout.writeLong(rec.getVivaldiCoordinates().getVivaldiCoord1(), tag);
        rout.writeLong(rec.getVivaldiCoordinates().getVivaldiCoord2(), tag);
        rout.writeLong(rec.getVivaldiCoordinates().getVivaldiCoord3(), tag);
        rout.writeLong(rec.getVivaldiCoordinates().getVivaldiCoord4(), tag);
        rout.writeLong(rec.getVivaldiCoordinates().getVivaldiCoord5(), tag);
        rout.endRecord(this, tag);

    }

    public void deserialize(RecordInput rin, String tag) throws IOException
    {
        rin.startRecord(tag);
        rec.setZone(rin.readString(tag));
        rec.setName(rin.readString(tag));
        rec.setType(rin.readInt(tag));
        rec.setDClass(rin.readInt(tag));
        rec.setTTL(rin.readLong(tag));
        rec.setRdata(rin.readString(tag));
        rec.setMethod(rin.readInt(tag));
        rec.setDelegations(rin.readString(tag));
        rec.setTimestamp(rin.readLong(tag));
        rec.setRequestIP(rin.readString(tag));
        rec.getVivaldiCoordinates().setVivaldiCoord1(rin.readLong(tag));
        rec.getVivaldiCoordinates().setVivaldiCoord2(rin.readLong(tag));
        rec.getVivaldiCoordinates().setVivaldiCoord3(rin.readLong(tag));
        rec.getVivaldiCoordinates().setVivaldiCoord4(rin.readLong(tag));
        rec.getVivaldiCoordinates().setVivaldiCoord5(rin.readLong(tag));
        rin.endRecord(tag);
    }

    /**
     * @see DNSRecord#expiredSince()
     */
    public LongWritable expiredSince()
    {
        return new LongWritable(rec.expiredSince());
    }

    public boolean equals(Object peer) throws ClassCastException
    {
        return rec.equals(peer);
    }

    public int compareTo(Object peer) throws ClassCastException
    {
        return rec.compareTo(peer);
    }

    /**
     * @see DNSRecord#getRecordKey()
     */
    public String getRecordKey()
    {
        return rec.getRecordKey();
    }

    /**
     * @see DNSRecord#toString()
     */
    public String toString()
    {
        return rec.toString();
    }
}
