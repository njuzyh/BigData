package lab2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class cooccurrence {

    public static class cooccurrenceMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private IntWritable one = new IntWritable();
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] strarr = value.toString().split(",");
            one.set(Integer.parseInt(strarr[1]));
            StringTokenizer itr = new StringTokenizer(strarr[0]);
            while (itr.hasMoreTokens()) {
                String base = itr.nextToken();
                StringTokenizer itr2 = new StringTokenizer(strarr[0]);
                while (itr2.hasMoreTokens()) {
                    String extra = itr2.nextToken();
                    if (base.equals(extra)) continue;
                    word.set("<" + base + "," + extra + ">");
                    context.write(word, one);
                }
            }
        }
    }

    public static class cooccurrenceReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
