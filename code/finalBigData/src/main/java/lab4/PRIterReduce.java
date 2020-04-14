package lab4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PRIterReduce extends Reducer<Text, Text, Text, Text> {
    //输入：key：人名,values:pagerank_list / link_list
    //输出：key: 人名,values:cur_pagerank;link_list
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        String link = "";
        double pagerank = 0;
        for(Text value:values){
            String tmp = value.toString();
            if(tmp.startsWith("|")){
                link = tmp.substring(tmp.indexOf("|") + 1);
                continue;
            }
            pagerank += Double.parseDouble(tmp);
        }
        pagerank = (1 - 0.85) + 0.85 * pagerank;
        context.write(new Text(key), new Text(String.valueOf(pagerank) + ";"+link));
    }
}
