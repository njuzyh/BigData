package lab3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class RelationGraphicsReducer extends Reducer<Text, Text, Text, Text> {
    //输入:key：人名，value：[人名，共现次数;人名，共现次数...]
    //输出：key:人名，value: [人名，共现次数（归一化）；人名，共现次数（归一化）...]
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        StringBuilder out1 = new StringBuilder();
        for(Text val: values){
            String vals = val.toString();
            sum += Integer.parseInt(vals.substring(vals.indexOf(",") + 1));
            out1.append(vals);
            out1.append(";");
        }
        out1.setLength(out1.length() - 1);
        String[] guiyi = out1.toString().split(";");
        StringBuilder out2 = new StringBuilder();
        for(String x: guiyi){
            out2.append(x.split(",")[0] + ","+ (double)Integer.parseInt(x.split(",")[1]) / sum);
            out2.append(";");
        }
        out2.setLength(out2.length() - 1);
        context.write(key, new Text(out2.toString()));
    }
}