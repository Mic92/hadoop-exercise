package examples.apachelogs;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class AccessLogRecord
{
    private String IP;
    private String username;
    private Date date;
    private String method;
    private String uri;
    private int code;
    private long size;
    private String referer;
    private String browser;
}
