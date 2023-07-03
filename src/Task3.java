import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Task3 {
    public static class IpMapper extends Mapper<Object, Text, Text, Text> {


        public void map(Object key, Text value, Mapper<Object,Text,Text,Text>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            WebLogBean log=WebLogParser.parsertask1(line);
            if(log!=null&&log.isvalid()){
                String[] tmp=log.getRequest().split(" ");
                String request="";
                if(tmp.length >=3 )
                    request = tmp[1];
                String ip = "";
                ip = log.getRemote_addr();
                context.write(new Text(request),new Text(ip));
            }
            else{
                context.write(new Text(""),new Text(""));
            }

        }
    }

    public static class IpReducer extends Reducer<Text, Text, Text, Text> {


        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            Set<String> toCount = new HashSet<>();
            for (Text val: values) {
                toCount.add(val.toString());
            }
            int count = toCount.size();
            context.write(key,new Text(Integer.toString(count)));
        }
    }
}