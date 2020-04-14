package lab6;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TotalSort {
    public static class TotalSortMapper extends
            Mapper<Text, Text, FloatWritable,  Text> {
        @Override
        protected void map(Text key, Text value,
                           Context context) throws IOException, InterruptedException {
            FloatWritable pagerank = new FloatWritable(Float.parseFloat(value.toString()));
            context.write(pagerank, key);
        }
    }

    public static class TotalSortReducer extends
            Reducer<FloatWritable, Text, Text, FloatWritable> {
        @Override
        protected void reduce(FloatWritable key, Iterable<Text> values,
                              Context context) throws IOException, InterruptedException {
            for (Text value : values)
                context.write(value, key);
        }
    }
}
