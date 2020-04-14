package lab3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RelationGraphicsMapper extends Mapper<Text, Text, Text, Text>{
    //输入：key：（人名，人名），value：共现次数
    //输出：key：人名，value：人名，共现次数
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String term = key.toString();
        String val = value.toString();
        String lead;
        String support;
        lead = term.substring(term.indexOf("<") + 1, term.indexOf(","));
        support = term.substring(term.indexOf(",") + 1, term.indexOf(">"));
        context.write(new Text(lead), new Text(support + ","+ val));
    }
}