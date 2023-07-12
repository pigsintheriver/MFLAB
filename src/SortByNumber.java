import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SortByNumber {
    public static class SortMapper extends Mapper<Object, Text, IntWritable, Text> {
        private final static IntWritable count = new IntWritable();
        private Text resourcePath = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");
            int countValue = Integer.parseInt(parts[1]);
            count.set(countValue);
            resourcePath.set(parts[0]);
            context.write(count, resourcePath);
        }
    }

    public static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

        private List<ResultPair> resultList;

        @Override
        protected void setup(Context context) {
            resultList = new ArrayList<>();
        }

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                String resourcePath = value.toString();
                ResultPair resultPair = new ResultPair(resourcePath, key.get());
                resultList.add(resultPair);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Collections.sort(resultList, new ResultPairComparator()); // 根据计数降序排序

            for (ResultPair resultPair : resultList) {
                context.write(new Text(resultPair.getResourcePath()), new IntWritable(resultPair.getCount()));
            }
        }
    }

    private static class ResultPair {
        private String resourcePath;
        private int count;

        public ResultPair(String resourcePath, int count) {
            this.resourcePath = resourcePath;
            this.count = count;
        }

        public String getResourcePath() {
            return resourcePath;
        }

        public int getCount() {
            return count;
        }
    }

    private static class ResultPairComparator implements Comparator<ResultPair> {
        @Override
        public int compare(ResultPair pair1, ResultPair pair2) {
            return Integer.compare(pair2.getCount(), pair1.getCount()); // 降序排序
        }
    }
}
