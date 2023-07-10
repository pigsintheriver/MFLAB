import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
public class Task5 {
    public static class BrowserTypeMapper extends Mapper<Text,WebLogBean,Text,IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        public void map(Text key,WebLogBean value,Context context) throws IOException,InterruptedException{
            if(value.isvalid()){
                String http_user_agent=value.getHttp_user_agent();
                context.write(new Text(http_user_agent),one);
            }
        }
    }

    public static class BrowserTypeReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        public void reduce(Text key,Iterable<IntWritable> values,Context context)throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable val:values){
                sum+=val.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }
}