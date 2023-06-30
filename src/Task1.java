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

public class Task1 {
    public static class LogMapper extends Mapper<Object, Text, NullWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            NullWritable outputkey = null;
            WebLogBean log=WebLogParser.parser(line);
            context.write(outputkey, new Text(log.toString()));
        }


    }

//    public static class LogReducer extends Reducer<Text, Text, Text, Text> {
//        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            for (Text value : values) {
//                context.write(null, value);
//            }
//        }
//    }

//    public static void main(String[] args) throws Exception {
//        String outputFile0 = "output/output0.log";
//        String outputFile1 = "output/output1.log";
//
//        String outputFile2 = "output/output2.log";
//
//
//        try (BufferedReader reader = new BufferedReader(new FileReader(args[0]));
//             PrintWriter writer = new PrintWriter(new FileWriter(outputFile0)))//初始版本的task1输出
//        {
//            String line;
//            while ((line = reader.readLine()) != null) {
//
//                String[] tokens = splitLog0(line);
//
//                //将第一部分的处理写入文件output1.log
//
//                writer.println("remote_addr:" + tokens[0]);
//                writer.println("remote_user:" + tokens[1]);
//                writer.println("time_local:" + tokens[2]);
//                writer.println("request:" + tokens[3]);
//                writer.println("status: " + tokens[4]);
//                writer.println("body_bytes_sent:" + tokens[5]);
//                writer.println("http_referer:" + tokens[6]);
//                writer.println("http_user_agent:" + tokens[7]);
//                writer.println("time:" + tokens[8]);
//                writer.println("time1:" + tokens[9]);
//            }
//            System.out.println("Output1 has been written to " + outputFile0);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        Configuration conf = new Configuration();
//        Job job1 = Job.getInstance(conf, "Job 1");
//        job1.setJarByClass(Task1.class);
//
//        // 设置任务1的输入路径
//        FileInputFormat.addInputPath(job1, new Path(args[0]));
//        job1.setInputFormatClass(TextInputFormat.class);
//
//        // 设置任务1的Mapper和Reducer
//        job1.setMapperClass(Task1.LogMapper.class);
//        job1.setReducerClass(Task1.LogReducer.class);
//
//        // 设置任务1的输出键值对类型
//        job1.setOutputKeyClass(Text.class);
//        job1.setOutputValueClass(Text.class);
//        // 删除输出目录
//        FileSystem fs1 = FileSystem.get(conf);
//        fs1.delete(new Path("output1"), true);
//        // 设置任务1的输出路径
//        FileOutputFormat.setOutputPath(job1, new Path("output1"));
//        job1.setNumReduceTasks(1); // 设置reduce任务的数量为1，保证所有输出结果都写入同一文件
//
//        //System.exit(job1.waitForCompletion(true) ? 0 : 1);
//
//
//        if (job1.waitForCompletion(true)) {
//            // 手动重命名生成的输出文件
//            FileSystem fs = FileSystem.get(conf);
//            FileStatus[] files = fs.listStatus(new Path("output1"));
//            for (FileStatus file : files) {
//                if (file.isFile()) {
//                    fs.rename(file.getPath(), new Path("output/output1.log"));
//                    break;  // 只处理第一个文件，若有多个输出文件请根据需要修改
//                }
//            }
//            // 删除output1文件夹
//            fs.delete(new Path("output1"), true);
//
//            Job job2 = Job.getInstance(conf, "Task2");
//            job2.setJarByClass(Task2.class);
//            job2.setMapperClass(Task2.ResourceMapper.class);
//            job2.setReducerClass(Task2.CountReducer.class);
//            job2.setOutputKeyClass(Text.class);
//            job2.setOutputValueClass(IntWritable.class);
//
//            FileInputFormat.addInputPath(job2, new Path(outputFile1));
//            FileSystem fs2 = FileSystem.get(conf);
//            fs2.delete(new Path("output2"), true);
//            FileOutputFormat.setOutputPath(job2, new Path("output2"));
//
//            //FileOutputFormat.setOutputPath(job, new Path(args[1]));
//            job2.setNumReduceTasks(1); // 设置reduce任务的数量为1，保证所有输出结果都写入同一文件
//            System.exit(job2.waitForCompletion(true) ? 0 : 1);
//
//
//        }
//    }

}