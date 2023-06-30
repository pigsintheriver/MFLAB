import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Loganalysis {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Job 1");
        job1.setJarByClass(Task1.class);

        // 设置任务1的输入路径
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        job1.setInputFormatClass(TextInputFormat.class);

        // 设置任务1的Mapper和Reducer
        job1.setMapperClass(Task1.LogMapper.class);

        // 设置任务1的输出键值对类型
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);

        // 设置任务1的输出路径
        FileOutputFormat.setOutputPath(job1, new Path("output1"));
        job1.setNumReduceTasks(0); // 设置reduce任务的数量为1，保证所有输出结果都写入同一文件
        // 启动并等待Job1的完成
        job1.waitForCompletion(true);
        Job job2 = Job.getInstance(conf, "Task2");
        job2.setJarByClass(Task2.class);
        job2.setMapperClass(Task2.ResourceMapper.class);
        job2.setReducerClass(Task2.CountReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, FileOutputFormat.getOutputPath(job1));
        FileOutputFormat.setOutputPath(job2, new Path("output2"));

        System.exit(job2.waitForCompletion(true) ? 0 : 1); // 提交作业并等待完成

    }
}