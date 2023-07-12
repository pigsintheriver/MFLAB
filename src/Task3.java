import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;

import java.util.TreeMap;
import java.util.Collections;
import  java.util.Map;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Task3 {
    public static class IpMapper extends Mapper<Text, WebLogBean, Text, Text> {


        public void map(Text key, WebLogBean value, Mapper<Text,WebLogBean,Text,Text>.Context context) throws IOException, InterruptedException {

            if(value.isvalid()){
                String[] tmp=value.getRequest().split(" ");
                String request="";
                if(tmp.length >=3 )
                    request = tmp[1];
                String ip = "";
                ip = value.getRemote_addr();
                context.write(new Text(request),new Text(ip));
            }
            else{
                context.write(new Text(""),new Text(""));
            }

        }
    }

    public static class IpReducer extends Reducer<Text, Text, Text, Text> {
        private TreeMap<Integer, Text> sortedResult;

        protected void setup(Context context) {
            sortedResult = new TreeMap<>();
        }

        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            Set<String> toCount = new HashSet<>();
            for (Text val: values) {
                toCount.add(val.toString());
            }
            int count = toCount.size();
            context.write(key,new Text(Integer.toString(count)));
            sortedResult.put(count, new Text(key));
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text value : sortedResult.descendingMap().values()) {
                for (Map.Entry<Integer, Text> entry : sortedResult.entrySet()) {
                    if (entry.getValue().equals(value)) {
                        Integer count = entry.getKey();
//                        context.write(value, new Text(String.valueOf(count)));
                        break;
                    }
                }
            }
        }

    }





}