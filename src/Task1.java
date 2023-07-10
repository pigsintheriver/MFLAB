import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Task1 {
    public static class LogMapper extends Mapper<Object, Text, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> mos;
        public void setup(Context context){
            mos = new MultipleOutputs<>(context);
        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            NullWritable outputkey = null;
            String line = value.toString();
            WebLogBean log=WebLogParser.parser(line);
//            mos.write("text",null,log,"text/text");
            mos.write("sequence",new Text(""),log,"sequence/sequence");
            context.write(null, new Text(log.toString()));
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 清理MultipleOutputs对象
            mos.close();
        }


    }

}