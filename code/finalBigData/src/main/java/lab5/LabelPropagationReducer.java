package lab5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class LabelPropagationReducer extends Reducer<Text, TextArrayWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String update = conf.get("update");
        String[] distributionStr = values.iterator().next().toStrings();

        double[] distribution = new double[distributionStr.length];
        for (int i = 0; i < distribution.length; ++i) {
            distribution[i] = Double.parseDouble(distributionStr[i]);
        }
        int label = 0;
        for (int i = 0; i < distribution.length; ++i) {
            label = distribution[i] > distribution[label] ? i : label;
        }
        if (update.equals("sync")) {
            conf.setStrings(key.toString(), distributionStr);
        }
        context.write(key, new IntWritable(label));
    }
}
