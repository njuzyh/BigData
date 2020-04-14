package lab5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;


public class Label {

    private static void initStatus(List<String> nameList, Configuration conf) {
        for (String name : nameList) {
            conf.setBoolean(name + "_status", false);
        }
    }

    private static void initDistributions(List<String> nameList, Configuration conf) {
        for (int i = 0; i < nameList.size(); ++i) {
            String[] distribution = new String[nameList.size()];
            for (int j = 0; j < nameList.size(); ++j) {
                distribution[j] = "0.0";
            }
            distribution[i] = "1.0";
            conf.setStrings(nameList.get(i), distribution);
        }
    }

    private static List<String> getNames(String fileName) throws IOException {
        List<String> nameList = new ArrayList<>();
        FileInputStream fileInputStream = new FileInputStream(fileName);
        InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String name;
        while ((name = bufferedReader.readLine()) != null) {
            nameList.add(name);
        }
        bufferedReader.close();
        inputStreamReader.close();
        fileInputStream.close();
        return nameList;
    }

    private static List<String> init(Configuration conf, String fileName, boolean weighted, String update, double tol) throws IOException {
        List<String> nameList = getNames(fileName);
        initStatus(nameList, conf);
        initDistributions(nameList, conf);
        conf.setBoolean("weighted", weighted);
        conf.set("update", update);
        conf.setDouble("tol", tol);
        return nameList;
    }

    public static void labelPropagation(boolean weighted, String update, double tol, int max_iter) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        List<String> nameList = init(conf, "people_name_list.txt", weighted, update, tol);
        for (int i = 0; i < max_iter; ++i) {
            Job job = Job.getInstance(conf, "LabelPropagation_iter" + String.valueOf(i));
            job.setJarByClass(Label.class);
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setMapperClass(LabelPropagationMapper.class);
            job.setReducerClass(LabelPropagationReducer.class);
            job.setNumReduceTasks(1);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(TextArrayWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path("/user/2019st34/out3"));
            FileOutputFormat.setOutputPath(job, new Path("out5_iter" + String.valueOf(i)));
            job.waitForCompletion(true);

            //check whether convergent or reach max_iter
            boolean convergent = true;
            for (String name : nameList) {
                if (!conf.getBoolean(name + "_status", false)) {
                    convergent = false;
                    break;
                }
            }
            if (convergent) {
                break;
            }
        }

    }

    public void start() throws Exception {
        labelPropagation(true, "async", 0.001, 10);
    }
}
