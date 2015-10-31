package examples.apachelogs;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class AccessLogRecord implements Comparable<Object> {
    private String ip;
    private String username;
    private Date date;
    private String method;
    private String uri;
    private int code;
    private long size;
    private String referrer;
    private String browser;

    // Example Apache log line:
    // 127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326 "http://www.example.com/start.html" "Mozilla/4.08 [en] (Win98; I ;Nav)"
    private final static Pattern PATTERN =
                           // 1:IP  2:client 3:user 4:date time                   5:method 6:req            7:respcode 8:size  [9:referrer    10:useragent]
            Pattern.compile("(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+)(?: \\S+)?\" (\\d{3}) (\\d+|-)(?: \"(\\S+)\" \"([^\"]+)\")?");
    private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss ZZZZZ", Locale.ENGLISH);

    static AccessLogRecord parse(String line) throws ParseException {
        Matcher matcher = PATTERN.matcher(line);
        if (!matcher.matches()) {
            throw new ParseException("Line does not match", 0);
        }

        // [10/Oct/2000:13:55:36 -0700]
        final Date date = DATE_FORMAT.parse(matcher.group(4));
        final int code, size;
        try {
            code = Integer.parseInt(matcher.group(7));
        } catch (NumberFormatException e){
            throw new ParseException("Malformed HTTP status code: "+matcher.group(7), 0);
        }
        if (matcher.group(8).equals("-")) {
           size = 0;
        } else {
            try {
                size = Integer.parseInt(matcher.group(8));
            } catch (NumberFormatException e){
                throw new ParseException("Malformed Request size: "+matcher.group(8), 0);
            }
        }
        final String referrer, useragent;
        if (matcher.groupCount() == 10) {
            referrer = matcher.group(9);
            useragent = matcher.group(10);
        } else {
            referrer = null;
            useragent = null;
        }

        return new AccessLogRecord(matcher.group(1),
                                   matcher.group(3),
                                   date,
                                   matcher.group(5),
                                   matcher.group(6),
                                   code,
                                   size,
                                   referrer,
                                   useragent);
    }

    // generated
    public int compareTo(Object other) {
        AccessLogRecord that = (AccessLogRecord) other;

        if (this.ip.compareTo(that.ip) < 0) {
            return -1;
        } else if (this.ip.compareTo(that.ip) > 0) {
            return 1;
        }

        if (this.username.compareTo(that.username) < 0) {
            return -1;
        } else if (this.username.compareTo(that.username) > 0) {
            return 1;
        }

        if (this.date.compareTo(that.date) < 0) {
            return -1;
        } else if (this.date.compareTo(that.date) > 0) {
            return 1;
        }

        if (this.method.compareTo(that.method) < 0) {
            return -1;
        } else if (this.method.compareTo(that.method) > 0) {
            return 1;
        }

        if (this.uri.compareTo(that.uri) < 0) {
            return -1;
        } else if (this.uri.compareTo(that.uri) > 0) {
            return 1;
        }

        if (this.code < that.code) {
            return -1;
        } else if (this.code > that.code) {
            return 1;
        }

        if (this.size < that.size) {
            return -1;
        } else if (this.size > that.size) {
            return 1;
        }

        if (this.referrer.compareTo(that.referrer) < 0) {
            return -1;
        } else if (this.referrer.compareTo(that.referrer) > 0) {
            return 1;
        }

        if (this.browser.compareTo(that.browser) < 0) {
            return -1;
        } else if (this.browser.compareTo(that.browser) > 0) {
            return 1;
        }
        return 0;
    }
}
