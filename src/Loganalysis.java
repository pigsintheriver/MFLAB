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
import org.apache.hadoop.io.LongWritable;

import java.io.*;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Loganalysis {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Job 1");
        job1.setJarByClass(Task1.class);

        // 设置任务1的输入路径
        FileInputFormat.addInputPath(job1, new Path(args[0]));//only one para
        job1.setInputFormatClass(TextInputFormat.class);

        // 设置任务1的Mapper和Reducer
        job1.setMapperClass(Task1.LogMapper.class);

        // 设置任务1的输出键值对类型
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);

        // 设置任务1的输出路径
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path("output1"), true);
        FileOutputFormat.setOutputPath(job1, new Path("output1"));
        job1.setNumReduceTasks(0); // 设置reduce任务的数量为1，保证所有输出结果都写入同一文件
        // 启动并等待Job1的完成
        job1.waitForCompletion(true);
        //任务2
        Job job2 = Job.getInstance(conf, "Task2");
        job2.setJarByClass(Task2.class);
        job2.setMapperClass(Task2.ResourceMapper.class);
        job2.setReducerClass(Task2.CountReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, FileOutputFormat.getOutputPath(job1));

        fs.delete(new Path("output2"), true);
        FileOutputFormat.setOutputPath(job2, new Path("output2"));

        //System.exit(job2.waitForCompletion(true) ? 0 : 1); // 提交作业并等待完成
        //任务3
        Job job3 = Job.getInstance(conf, "Task3");
        job3.setJarByClass(Task3.class);
        job3.setMapperClass(Task3.IpMapper.class);
        job3.setReducerClass(Task3.IpReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        fs.delete(new Path("output3"), true);
        FileInputFormat.addInputPath(job3, FileOutputFormat.getOutputPath(job1));
        FileOutputFormat.setOutputPath(job3, new Path("output3"));


        System.exit(job3.waitForCompletion(true) && job2.waitForCompletion(true) ? 0 : 1);// 提交作业并等待完成

    }
}