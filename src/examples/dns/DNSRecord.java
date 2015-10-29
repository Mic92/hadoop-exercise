package examples.dns;

import java.io.IOException;
import java.io.InputStream;
import java.util.GregorianCalendar;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Type;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * A class that represents a RR with additional information such as <br>
 * <ul>
 * <li>used crawler method (AXFR, NSEC, Other),</li>
 * <li>timestamp when the RR was crawled,</li>
 * <li>delegation history,</li>
 * <li>request/client-ip of the crawler node that queried the RR and</li>
 * <li>Vivaldi coordinates (currently not used)</li>
 * </ul>
 */
public class DNSRecord
{
    /**
     * An abstraction for fields in rdata sections of SOA RRs, such as
     * <ul>
     * <li>primary nameserver,</li>
     * <li>email address of responsible person,</li>
     * <li>serial,</li>
     * <li>refresh,</li>
     * <li>retry,</li>
     * <li>expiry,</li>
     * <li>min,</li>
     * </ul>
     */
    @Getter
    @Setter
    public static class SOARecordRdataStructure
    {
        private String nameServer = null;
        private String emailAddress = null;

        private long serial = 0;
        private long refresh = 0;
        private long retry = 0;
        private long expiry = 0;
        private long min = 0;

        /**
         * Creates and initializes a {@link SOARecordRdataStructure} object by
         * parsing the provided rdata field string.
         */
        public SOARecordRdataStructure(String rdata)
        {
            StringTokenizer st = new StringTokenizer(rdata);
            try
            {
                nameServer = st.nextToken();
                emailAddress = st.nextToken();

                try {serial = Long.parseLong(st.nextToken());}
                catch (NumberFormatException e) {}

                try {refresh = Long.parseLong(st.nextToken());}
                catch (NumberFormatException e) {}

                try {retry = Long.parseLong(st.nextToken());}
                catch (NumberFormatException e) {}

                try {expiry = Long.parseLong(st.nextToken());}
                catch (NumberFormatException e) {}

                try {min = Long.parseLong(st.nextToken());}
                catch (NumberFormatException e) {}
            }
            catch (NoSuchElementException e)
            {}
        }

    }

    /**
     * Vivaldi coordinates.
     */
    @Getter
    @Setter
    @AllArgsConstructor
    public static class VivaldiCoordinates
    {
        private long vivaldiCoord1;
        private long vivaldiCoord2;
        private long vivaldiCoord3;
        private long vivaldiCoord4;
        private long vivaldiCoord5;

        /**
         * Creates and initializes a VivaldiCoordinates object.
         */
        public VivaldiCoordinates()
        {
            vivaldiCoord1 = vivaldiCoord2 = vivaldiCoord3 = vivaldiCoord4 = vivaldiCoord5 =  0;
        }

        /**
         * Parses the given string and sets the Vivaldi coordinates.
         */
        public VivaldiCoordinates(String str)
        {
            StringTokenizer st = new StringTokenizer(str, "/");

            vivaldiCoord1 = Long.parseLong(st.nextToken());
            vivaldiCoord2 = Long.parseLong(st.nextToken());
            vivaldiCoord3 = Long.parseLong(st.nextToken());
            vivaldiCoord4 = Long.parseLong(st.nextToken());
            vivaldiCoord5 = Long.parseLong(st.nextToken());
        }

        /**
         * Returns a string representation of the Vivaldi coordinates.
         */
        public String toString()
        {
            return vivaldiCoord1 + "/" + vivaldiCoord2 + "/" + vivaldiCoord3
                + "/" + vivaldiCoord4 + "/" + vivaldiCoord5;
        }
    }

    private boolean useSimpleEqualAndCompareTo = false;

    /**
     * Creates a {@link DNSRecord} object by parsing a text line from the given
     * input stream.<br>
     * For better performance, {@link java.io.BufferedInputStream}.
     * should be used<br>
     * Errors that may occur while parsing the string are reported to the
     * provided logger.
     */
    public static long createRecord(InputStream in, DNSRecord rec, Log log)
        throws IOException
    {
        StringBuffer buf[] = new StringBuffer[11];
        long pos = 0;
        int tokenPos = 0;

        while (true)
        {
            int b = in.read();
            if (b == -1) break;
            pos++;

            byte c = (byte) b;
            if (c == '\n') break;
            if (c == ';') tokenPos++;
            if (c == '\r')
            {
                in.mark(1);
                byte nextC = (byte) in.read();
                if (nextC != '\n') in.reset();
                break;
            }

            if (tokenPos <= 10)
            {
                if (buf[tokenPos] == null) buf[tokenPos] = new StringBuffer();
                if (c != ';' && c != '\r' && c != '\n')
                    buf[tokenPos].append((char) c);
            }
        }

        if (tokenPos == 10)
        {
            try
            {
                rec.setZone(buf[0].toString().toLowerCase());
                rec.setName(buf[1].toString().toLowerCase());
                rec.setType(Type.value(buf[2].toString()));
                rec.setDClass(DClass.value(buf[3].toString()));
                rec.setTTL(Long.parseLong(buf[4].toString()));
                rec.setRdata(buf[5].toString().toLowerCase());
                rec.setMethod(Method.value(buf[6].toString()));
                rec.setDelegations(buf[7].toString().toLowerCase());
                rec.setTimestamp(Long.parseLong(buf[8].toString()));
                rec.setRequestIP(buf[9].toString().toLowerCase());
                rec.setVivaldiCoordinates(new VivaldiCoordinates(buf[10].toString()));
            }
            catch (NumberFormatException e)
            {
                String line = "";
                for (StringBuffer token : buf)
                line += token.toString() + ";";
                log.error(line, e);
            }

            return pos;
        }
        return pos * (-1);
    }

    /**
     * Creates a {@link DNSRecord} object by parsing the given string.<br>
     * Errors that may occur while parsing the string are reported to the
     * provided logger.
     */
    public static DNSRecord createRecord(String line, Log log)
    {
        DNSRecord rec = new DNSRecord();
        StringTokenizer st = new StringTokenizer(line, ";");
        try
        {
            rec.setZone(st.nextToken());
            rec.setName(st.nextToken());
            rec.setType(Type.value(st.nextToken()));
            rec.setDClass(DClass.value(st.nextToken()));
            rec.setTTL(Long.parseLong(st.nextToken()));
            rec.setRdata(st.nextToken());
            rec.setMethod(Method.value(st.nextToken()));
            rec.setDelegations(st.nextToken());
            rec.setTimestamp(Long.parseLong(st.nextToken()));
            rec.setRequestIP(st.nextToken());
            rec.setVivaldiCoordinates(new VivaldiCoordinates(st.nextToken()));

        }
        catch (NumberFormatException e)
        {
            log.error(line, e);
        }
        catch (NoSuchElementException e)
        {
            log.error("NoSuchElementException due to input split that "
                + " cut the record on half? " + line);
        }
        return rec;
    }

    private byte[] zone;
    private byte[] name;
    private int type;
    private int dclass;
    private long TTL;
    private byte[] rdata;

    private int method;

    private long timestamp;

    private byte[] delegations;

    private byte[] requestIP;

    private VivaldiCoordinates vivaldiCoordinates;

    /**
     * Creates and initializes a {@link DNSRecord} objects with default values
     * such as zone/name = ".", and zero timestamp etc.
     */
    public DNSRecord()
    {
        zone = name = ".".getBytes();
        rdata = delegations = requestIP = "-".getBytes();
        type = dclass = method = 0;
        TTL = timestamp = 0;
        vivaldiCoordinates = new VivaldiCoordinates();
    }

    /**
     * Compares to {@link DNSRecord} objects (in String representation
     * lexicographically).
     */
    public int compareTo(Object o) throws ClassCastException
    {
        if (!useSimpleEqualAndCompareTo)
        return toString().compareTo(o.toString());

        DNSRecord rec = (DNSRecord) o;
        if (getZone().equalsIgnoreCase(rec.getZone()))
        if (getName().equalsIgnoreCase(rec.getName()))
        if (getType() == rec.getType())
        if (getDClass() == rec.getDClass())
        if (getTTL() == rec.getTTL())
        if (getRdata().equalsIgnoreCase(rec.getRdata()))
        return 0;
        else return rdata.toString().compareTo(
        rec.rdata.toString());
        else return TTL < rec.TTL ? -1 : 1;
        else return dclass < rec.dclass ? -1 : 1;
        else return type < rec.type ? -1 : 1;
        else return name.toString().compareTo(rec.name.toString());
        return zone.toString().compareTo(rec.zone.toString());
    }

    public boolean equals(Object o) throws ClassCastException
    {
        if (!useSimpleEqualAndCompareTo)
            return toString().equals(o.toString());

        DNSRecord rec = (DNSRecord) o;
        if (getZone().equalsIgnoreCase(rec.getZone().toString()) &&
            getName().equalsIgnoreCase(rec.getName()) &&
            getType() == rec.getType() && getDClass() == rec.getDClass() &&
            getTTL() == rec.getTTL() &&
            getRdata().equalsIgnoreCase(rec.getRdata()) &&
            getTimestamp() == rec.getTimestamp())
        return true;
        return false;
    }

    /**
     * Return the time difference between the current time and when the RR has
     * been expired (according to the TTL and the crawling time).<br>
     * Time difference (in seconds) is either 0 (= not expired) or >0.
     */
    public long expiredSince()
    {
        GregorianCalendar foundDate = new GregorianCalendar();

        String dateStr = String.valueOf(timestamp);
        foundDate.set(Integer.parseInt(dateStr.substring(0, 4)),
            Integer.parseInt(dateStr.substring(4, 6)) - 1,
            Integer.parseInt(dateStr.substring(6, 8)),
            Integer.parseInt(dateStr.substring(8, 10)),
            Integer.parseInt(dateStr.substring(10, 12)),
            Integer.parseInt(dateStr.substring(12, 14)));

        long expiryDate = foundDate.getTimeInMillis() / 1000 + TTL;
        long now = new GregorianCalendar(TimeZone.getTimeZone("GMT"))
           .getTimeInMillis() / 1000;

        // date (when we have found the record) + ttl < now
        if (now - expiryDate >= 0)
        return now - expiryDate;
        else return 0;
    }

    /**
     * Returns the RR's class.
     */
    public int getDClass()
    {
        return dclass;
    }

    /**
     * Returns the RR's delegation history.
     */
    public String getDelegations()
    {
        return new String(delegations);
    }

    /**
     * Returns the crawling method the RRs was retrieved through.
     */
    public int getMethod()
    {
        return method;
    }

    /**
     * Returns the RR's DNS name.
     */
    public String getName()
    {
        return new String(name);
    }

    /**
     * Returns the RR's rdata field.
     */
    public String getRdata()
    {
        return new String(rdata);
    }

    /**
     * Returns a simplified key for a {@link DNSRecord} to remove RR's
     * duplicates.
     */
    public String getRecordKey()
    {
        return new String(zone) + ";" + new String(name) + ";" +
            Type.string(type) + ";" + DClass.string(dclass) + ";" + TTL + ";" +
            new String(rdata) + ";" + Method.string(method);
    }

    /**
     * Returns the RR's request/client-IP address.
     */
    public String getRequestIP()
    {
        return new String(requestIP);
    }

    /**
     * Returns the RR's timestamp.
     */
    public long getTimestamp()
    {
        return timestamp;
    }

    /**
     * Returns the RR's TTL.
     */
    public long getTTL()
    {
        return TTL;
    }

    /**
     * Returns the RR's type.
     */
    public int getType()
    {
        return type;
    }

    /**
     * Returns the RR's Vivaldi coordinates.
     */
    public VivaldiCoordinates getVivaldiCoordinates()
    {
        return vivaldiCoordinates;
    }

    /**
     * Returns the zone the RR does belong to.
     */
    public String getZone()
    {
        return new String(zone);
    }

    /**
     * Sets the RR's class.
     */
    public void setDClass(int dclass)
    {
        this.dclass = dclass;
    }

    /**
     * Sets the RR's delegation history.
     */
    public void setDelegations(String delegations)
    {
        this.delegations = delegations.getBytes();
    }

    /**
     * Sets the crawling method the RRs was retrieved through.
     */
    public void setMethod(int method)
    {
        this.method = method;
    }

    /**
     * Sets the RR's DNS name.
     */
    public void setName(String name)
    {
        this.name = name.replace(';', '-').getBytes();
    }

    /**
     * Sets the RR's rdata field.
     */
    public void setRdata(String record)
    {
        // remove ';' that we sometimes find in TXT records when
        // doing an AXFR ....
        this.rdata = record.replace(';', '-').getBytes();
    }

    /**
     * Sets the RR's request/client-IP address.
     */
    public void setRequestIP(String requestIP)
    {
        this.requestIP = requestIP.getBytes();
    }

    /**
     * Sets the RR's timestamp.
     */
    public void setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }

    /**
     * Sets the RR's TTL.
     */
    public void setTTL(long ttl)
    {
        this.TTL = ttl;
    }

    /**
     * Sets the RR's type.
     */
    public void setType(int type)
    {
        this.type = type;
    }

    /**
     * Sets the RR's Vivaldi coordinates.
     */
    public void setVivaldiCoordinates(VivaldiCoordinates vivaldiCoordinates)
    {
        this.vivaldiCoordinates = vivaldiCoordinates;
    }

    /**
     * Sets the zone the RR does belong to.
     */
    public void setZone(String zone)
    {
        this.zone = zone.replace(';', '-').getBytes();
    }

    public String toString()
    {
        return new String(zone) + ";" + new String(name) + ";" +
            Type.string(type) + ";" + DClass.string(dclass) + ";" + TTL + ";" +
            new String(rdata) + ";" + Method.string(method) + ";" +
            new String(delegations) + ";" + timestamp + ";" +
            new String(requestIP) + ";" + vivaldiCoordinates;
    }
}
