package lab4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PRIterMappper extends Mapper<Text, Text, Text, Text>{
    //输入：key:人名，value：pr_init, link_list
    //输出：key:人名，value:cur_rank, link_list
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tuple= line.split(";");
        double cur_rank = Double.parseDouble(tuple[0]);
        for(int i =1;i<tuple.length;i++){
            String[] linkPage = tuple[i].split(",");
            String prValue = String.valueOf(cur_rank*Double.parseDouble(linkPage[1]));       //每一个角色对他链接的角色的pr贡献为当前角色的pr*两个角色同时出现的次数
            context.write(new Text(linkPage[0]), new Text(prValue));
        }
        context.write(key, new Text("|" + line.substring(line.indexOf(";") + 1)));              //传递整个图结构
    }
}
