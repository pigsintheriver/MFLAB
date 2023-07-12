import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class individualcount {
    public static class IndividualMapper extends Mapper<Text,WebLogBean,Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        public void map(Text key,WebLogBean value,Context context) throws IOException,InterruptedException{
            context.write(new Text(value.getRemote_addr()),one);
        }
    }

    public static class IndividualReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        public void reduce(Text key,Iterable<IntWritable> values,Context context)throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable val:values){
                sum+=val.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }
}
