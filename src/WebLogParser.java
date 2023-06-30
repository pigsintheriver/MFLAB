import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WebLogParser {
    public static WebLogBean parser(String line) {
        String regex = "^(\\S+) (\\S+) (\\S+) \\[(.*?)\\] \"(.*?)\" (\\d+) (\\d+) \"(.*?)\" \"(.*?)\"$";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(line);
        if (matcher.find()) {
            WebLogBean log = new WebLogBean();
            log.setRemote_addr(matcher.group(1));
            log.setRemote_user(matcher.group(2));// default remote user = -
            String time_local = matcher.group(4);
            if (null == time_local || "".equals(time_local)) {
                time_local = "-invalid time-";
            }
            log.setTime_local(time_local);
            String format1 = "dd/MMM/yyyy:HH:mm:ss";
            String format2 = "yyyy.MM.dd:HH:mm:ss";
            String format3 = "yyyyMMddHH";
            SimpleDateFormat date1 = new SimpleDateFormat(format1, Locale.ENGLISH);
            SimpleDateFormat date2 = new SimpleDateFormat(format2);
            SimpleDateFormat date3 = new SimpleDateFormat(format3);
            date2.setTimeZone(TimeZone.getTimeZone("GMT"));
            date3.setTimeZone(TimeZone.getTimeZone("GMT"));
            try {
                Date inputDate = date1.parse(time_local);
                log.setTime_detail(date2.format(inputDate));
                log.setTime_til_hour(date3.format(inputDate));
            } catch (ParseException E) {
                E.printStackTrace();
            }
            log.setRequest(matcher.group(5));
            log.setStatus(matcher.group(6));
            log.setBody_bytes_sent(matcher.group(7));
            log.setHttp_referer(matcher.group(8));
            try{
                URL url = new URL(matcher.group(8));
                log.setHttp_body(url.getHost());
            }catch (MalformedURLException e) {
                e.printStackTrace();
            }
            log.setHttp_user_agent(matcher.group(9));
            //大于400, HTTP错误
            if (Integer.parseInt(log.getStatus()) >= 400) {
                log.setValid(false);
            }
            if ("-invalid time-".equals(log.getTime_local())) {
                log.setValid(false);
            }
            return log;

        } else return null;

    }
    // parser task1 output
    public static WebLogBean parsertask1(String line){
        String regex="^(true|false) (\\S+) (\\S+) \\[(.*?)\\] \"(.*?)\" (\\d+) (\\d+) \"(.*?)\" \"(.*?)\" (\\S+) (\\S+) (\\S+)$";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(line);
        if(matcher.find()){
            WebLogBean log = new WebLogBean();
            log.setValid(Boolean.parseBoolean(matcher.group(1)));
            log.setRemote_addr(matcher.group(2));
            log.setRemote_user(matcher.group(3));// default remote user = -
            log.setTime_local(matcher.group(4));
            log.setRequest(matcher.group(5));
            log.setStatus(matcher.group(6));
            log.setBody_bytes_sent(matcher.group(7));
            log.setHttp_referer(matcher.group(8));
            log.setHttp_user_agent(matcher.group(9));
            log.setTime_detail(matcher.group(10));
            log.setTime_til_hour(matcher.group(11));
            log.setHttp_body(matcher.group(12));
            return log;
        }
        else return null;
    }

}
