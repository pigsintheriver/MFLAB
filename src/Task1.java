import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.BufferedReader;
public class Task1{
        public static String[] splitLog(String line)
        {
                    String[] tokens=new String[11];
                    String regex = "^(\\S+) (\\S+) (\\S+) \\[(.*?)\\] \"(.*?)\" (\\d+) (\\d+) \"(.*?)\" \"(.*?)\"$";
                    Pattern pattern = Pattern.compile(regex);
                    Matcher matcher = pattern.matcher(line);
                    if (matcher.find()) {
                        tokens[0]= matcher.group(1);
                        tokens[1]= matcher.group(2);
                        tokens[2]= matcher.group(4);
                        tokens[3]= matcher.group(5);
                        tokens[4]= matcher.group(6);
                        tokens[5]= matcher.group(7);
                        tokens[6]= matcher.group(8);
                        tokens[7]= matcher.group(9);
                    }
                    String format1="dd/MMM/yyyy:HH:mm:ss Z";
                    String format2="yyyy.MM.dd:HH:mm:ss";
                    String format3="yyyyMMddHH";
                    SimpleDateFormat date1=new SimpleDateFormat(format1);
                    SimpleDateFormat date2=new SimpleDateFormat(format2);
                    SimpleDateFormat date3=new SimpleDateFormat(format3);
                    date2.setTimeZone(TimeZone.getTimeZone("GMT"));
                    date3.setTimeZone(TimeZone.getTimeZone("GMT"));
                    try{
                        Date inputDate=date1.parse(tokens[2]);
                        tokens[8]=date2.format(inputDate);
                        tokens[9]=date3.format(inputDate);
                    }catch (ParseException E)
                    {
                        E.printStackTrace();
                    }
            return tokens;
        }
        public static  void main(String []args)
        {
            String filename="/home/hadoop/test.log";
            try(BufferedReader reader=new BufferedReader(new FileReader(filename));)
            {
                String line;
                while((line=reader.readLine())!=null) {
                    String []tokens = splitLog(line);
                    System.out.println("remote_addr:" + tokens[0]);
                    System.out.println("remote_user:" + tokens[1]);
                    System.out.println("time_local:" + tokens[2]);
                    System.out.println("request:" + tokens[3]);
                    System.out.println("status: " + tokens[4]);
                    System.out.println("body_bytes_sent:" + tokens[5]);
                    System.out.println("http_referer:" + tokens[6]);
                    System.out.println("http_user_agent:" + tokens[7]);
                    System.out.println("time:" + tokens[8]);
                    System.out.println("time1:" + tokens[9]);
                }
            }catch (IOException e){
                e.printStackTrace();
            }

        }
}


