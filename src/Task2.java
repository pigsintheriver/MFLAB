import java.io.IOException;
import java.nio.file.WatchEvent;
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

import java.util.TreeMap;
import java.util.Collections;
import  java.util.Map;

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

public class Task2 {
    //    public static class ResourceMapper extends Mapper<Object, Text, Text, IntWritable> {
//        private final static IntWritable one = new IntWritable(1);
////        private Text resourcePath = new Text();
//
//        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            String line = value.toString();
//            WebLogBean log=WebLogParser.parsertask1(line);
//            if(log!=null&&log.isvalid()){
//                String[] tmp=log.getRequest().split(" ");
//                String request="";
//                if(tmp.length >=3 )
//                    request = tmp[1];
//                context.write(new Text(request),one);
//            }
//            else{
//                context.write(new Text(""),one);
//            }
//        }
//    }
    public static class ResourceMapper extends Mapper<Text, WebLogBean, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        public void map(Text key, WebLogBean value, Context context) throws IOException, InterruptedException {
            if (value.isvalid()) {
                String[] tmp = value.getRequest().split(" ");
                String request = "";
                if (tmp.length >= 3)
                    request = tmp[1];
                context.write(new Text(request), one);
            } else {
                context.write(new Text(""), one);
            }
        }
    }

    public static class Task2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        //private IntWritable result = new IntWritable();
        private TreeMap<Integer, Text> sortedResult;

        protected void setup(Context context) {
            sortedResult = new TreeMap<>(Collections.reverseOrder());
        }


        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key,new IntWritable((sum)));
            sortedResult.put(sum, new Text(key));
        }
        protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            for (Map.Entry<Integer, Text> entry : sortedResult.entrySet()) {
//                context.write(entry.getValue(), new IntWritable(entry.getKey()));
            }
        }
    }

}