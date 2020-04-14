package lab6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Cluster {
    public static class ClusterMapper extends
            Mapper<Text, Text, IntWritable, Text> {
        @Override
        protected void map(Text key, Text value,
                           Context context) throws IOException, InterruptedException {
            IntWritable label = new IntWritable(Integer.parseInt(value.toString()));
            context.write(label, key);
        }
    }

    public static class ClusterReducer extends
            Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values,
                              Context context) throws IOException, InterruptedException {
            StringBuilder allkey = new StringBuilder();
            for (Text value : values) {
                allkey.append(value.toString());
                allkey.append(",");
            }
            allkey.setLength(allkey.length() - 1);
            context.write(key, new Text(allkey.toString()));
        }
    }
}
