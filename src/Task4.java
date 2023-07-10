import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
public class Task4 {
    public static class Hour_visitMapper extends Mapper<Text,WebLogBean,Text,IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        public void map(Text key,WebLogBean value,Context context) throws IOException,InterruptedException{
            if(value.isvalid()){
                String time_til_hour=value.getTime_til_hour();
                context.write(new Text(time_til_hour),one);
            }
        }
    }

    public static class Hour_visitReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        public void reduce(Text key,Iterable<IntWritable> values,Context context)throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable val:values){
                sum+=val.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }
}
