package lab4;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankViewerMapper extends Mapper<Text, Text, Text, FloatWritable> {
    private Text outPage = new Text();
    private FloatWritable outPr = new FloatWritable();
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
        String[] tuple = value.toString().split(";");
        float pr = Float.parseFloat(tuple[0]);
        outPage.set(key);
        outPr.set(pr);
        context.write(outPage,outPr);
    }
}
