import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IndividualHistogram {
    public static class HistogramMapper extends Mapper<Text,Text,IntWritable,IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        @Override
        public void map(Text key,Text value,Context context) throws IOException,InterruptedException{
            context.write(new IntWritable(Integer.parseInt(value.toString())),one);
        }
    }
    public static class HistogramReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

        public void reduce(IntWritable key,Iterable<IntWritable> values,Context context)throws IOException, InterruptedException{
            int sum=0;
            for(IntWritable val:values){
                sum+=val.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }
}
