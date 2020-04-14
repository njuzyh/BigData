package lab4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GraphBuilderMapper extends Mapper<Text, Text, Text, Text>{
    //输入：key:人名，value: [人名，共现次数（归一化）；人名，共现次数（归一化）...]
    //输出：key:人名，value：pr_init, link_list
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
        String pagerank = "1.0";
        context.write(key, new Text(pagerank +";" + value.toString()));
    }
}