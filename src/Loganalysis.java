import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Loganalysis {
    public static void main(String[] args) throws Exception {
        Path interinput = new Path("output1/sequence");
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Task 1");
        job1.setJarByClass(Task1.class);
        // 设置任务1的输入路径
        FileInputFormat.addInputPath(job1, new Path(args[1]));//only one para
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setMapperClass(Task1.LogMapper.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        MultipleOutputs.addNamedOutput(job1, "sequence", SequenceFileOutputFormat.class, Text.class, WebLogBean.class);

        // 设置任务1的输出路径
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path("output1"), true);

        FileOutputFormat.setOutputPath(job1, new Path("output1"));
        job1.setNumReduceTasks(0);
        // 启动并等待Job1的完成
        job1.waitForCompletion(true);

//        任务2
        Job job2 = Job.getInstance(conf, "Task2");
        job2.setJarByClass(Task2.class);
        job2.setMapperClass(Task2.ResourceMapper.class);
        job2.setReducerClass(Task2.Task2Reducer.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, interinput);

        fs.delete(new Path("output2_raw"), true);
        FileOutputFormat.setOutputPath(job2, new Path("output2_raw"));
        job2.waitForCompletion(true);
        // 将 Task 2 的输出文件移动到新的目录中

        fs.mkdirs(new Path("sorted_output2"));
        fs.rename(new Path("output2_raw/part-r-00000"), new Path("sorted_output2/part-r-00000"));

        // 执行 Task 22 对新目录进行排序
        Job job22 = Job.getInstance(conf, "Task 2 Sort");
        job22.setJarByClass(SortByNumber.class);
        job22.setMapperClass(SortByNumber.SortMapper.class);
        job22.setReducerClass(SortByNumber.SortReducer.class);
        job22.setOutputKeyClass(IntWritable.class);
        job22.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job22, new Path("sorted_output2/"));
        fs.delete(new Path("output2"), true);
        FileOutputFormat.setOutputPath(job22, new Path("output2"));


        //任务3
        Job job3 = Job.getInstance(conf, "Task3");
        job3.setJarByClass(Task3.class);
        job3.setMapperClass(Task3.IpMapper.class);
        job3.setReducerClass(Task3.IpReducer.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        fs.delete(new Path("output3_raw"), true);
        FileInputFormat.addInputPath(job3, interinput);
        FileOutputFormat.setOutputPath(job3, new Path("output3_raw"));
        job3.waitForCompletion(true);

        // 将 Task 3 的输出文件移动到新的目录中

        fs.mkdirs(new Path("sorted_output3"));
        fs.rename(new Path("output3_raw/part-r-00000"), new Path("sorted_output3/part-r-00000"));

        // 执行 Task 33 对新目录进行排序
        Job job33 = Job.getInstance(conf, "Task 3 Sort");
        job33.setJarByClass(SortByNumber.class);
        job33.setMapperClass(SortByNumber.SortMapper.class);
        job33.setReducerClass(SortByNumber.SortReducer.class);
        job33.setOutputKeyClass(IntWritable.class);
        job33.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job33, new Path("sorted_output3/"));
        fs.delete(new Path("output3"), true);
        FileOutputFormat.setOutputPath(job33, new Path("output3"));

        Job job4 = Job.getInstance(conf, "Task4");
        job4.setJarByClass(Task4.class);
        job4.setMapperClass(Task4.Hour_visitMapper.class);
        job4.setCombinerClass(Task4.Hour_visitReducer.class);
        job4.setReducerClass(Task4.Hour_visitReducer.class);
        job4.setInputFormatClass(SequenceFileInputFormat.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(IntWritable.class);
        fs.delete(new Path("output4"), true);
        FileInputFormat.addInputPath(job4, interinput);
        FileOutputFormat.setOutputPath(job4, new Path("output4"));

        Job job5 = Job.getInstance(conf, "Task5");
        job5.setJarByClass(Task5.class);
        job5.setMapperClass(Task5.BrowserTypeMapper.class);
        job5.setCombinerClass(Task5.BrowserTypeReducer.class);
        job5.setReducerClass(Task5.BrowserTypeReducer.class);
        job5.setInputFormatClass(SequenceFileInputFormat.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(IntWritable.class);
        fs.delete(new Path("output5"), true);
        FileInputFormat.addInputPath(job5, interinput);
        FileOutputFormat.setOutputPath(job5, new Path("output5"));


        Job job6 = Job.getInstance(conf, "DomainCount");
        job6.setJarByClass(referdomaincount.class);
        job6.setMapperClass(referdomaincount.DomainMapper.class);
        job6.setCombinerClass(referdomaincount.DomainReducer.class);
        job6.setReducerClass(referdomaincount.DomainReducer.class);
        job6.setInputFormatClass(SequenceFileInputFormat.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(IntWritable.class);
        fs.delete(new Path("out_domain"), true);
        FileInputFormat.addInputPath(job6, interinput);
        FileOutputFormat.setOutputPath(job6, new Path("out_domain"));

        Job job7 = Job.getInstance(conf, "IndividualCount");
        job7.setJarByClass(individualcount.class);
        job7.setMapperClass(individualcount.IndividualMapper.class);
        job7.setCombinerClass(individualcount.IndividualReducer.class);
        job7.setReducerClass(individualcount.IndividualReducer.class);
        job7.setInputFormatClass(SequenceFileInputFormat.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(IntWritable.class);
        fs.delete(new Path("out_individual"), true);
        FileInputFormat.addInputPath(job7, interinput);
        FileOutputFormat.setOutputPath(job7, new Path("out_individual"));
        job7.waitForCompletion(true);


        Job job_BR = Job.getInstance(conf, "BadRequestCount");
        job_BR.setJarByClass(BadRequestCount.class);
        job_BR.setMapperClass(BadRequestCount.BR_ipMapper.class);
        job_BR.setCombinerClass(BadRequestCount.BR_ipReducer.class);
        job_BR.setReducerClass(BadRequestCount.BR_ipReducer.class);
        job_BR.setInputFormatClass(SequenceFileInputFormat.class);
        job_BR.setOutputKeyClass(Text.class);
        job_BR.setOutputValueClass(IntWritable.class);
        fs.delete(new Path("out_BadRequest"), true);
        FileInputFormat.addInputPath(job_BR, interinput);
        FileOutputFormat.setOutputPath(job_BR, new Path("out_BadRequest"));
//
        Job job_histogram=Job.getInstance(conf,"Ind_histogram");
        job_histogram.setJarByClass(IndividualHistogram.class);
        job_histogram.setInputFormatClass(KeyValueTextInputFormat.class);
        job_histogram.setOutputKeyClass(IntWritable.class);
        job_histogram.setOutputValueClass(IntWritable.class);
        fs.delete(new Path("out_individual_histogram"), true);
        FileInputFormat.addInputPath(job_histogram, new Path("out_individual"));
        FileOutputFormat.setOutputPath(job_histogram, new Path("out_individual_histogram"));

        job_histogram.setMapperClass(IndividualHistogram.HistogramMapper.class);
        job_histogram.setCombinerClass(IndividualHistogram.HistogramReducer.class);
        job_histogram.setReducerClass(IndividualHistogram.HistogramReducer.class);


        System.exit(job_histogram.waitForCompletion(true)&&job_BR.waitForCompletion(true) && job6.waitForCompletion(true) && job5.waitForCompletion(true) && job4.waitForCompletion(true) && job33.waitForCompletion(true) && job22.waitForCompletion(true) ? 0 : 1);
    }
}