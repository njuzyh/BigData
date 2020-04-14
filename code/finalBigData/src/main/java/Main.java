import lab1.PreSolve;
import lab2.cooccurrence;
import lab3.RelationGraphicsMapper;
import lab3.RelationGraphicsReducer;
import lab4.GraphBuilderMapper;
import lab4.PRIterMappper;
import lab4.PRIterReduce;
import lab4.PageRankViewerMapper;
import lab5.Label;
import lab6.TotalSort;
import lab6.Cluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Main {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        Configuration conf1 = new Configuration();
        conf1.set("mapred.textoutputformat.separator", ",");
        Job preSolveJob = Job.getInstance(conf1, "lab1.PreSolve Job");
        preSolveJob.setJarByClass(Main.class);
        preSolveJob.setMapperClass(PreSolve.PreSolveMapper.class);
        preSolveJob.setReducerClass(PreSolve.PreSolveReducer.class);
        preSolveJob.setMapOutputKeyClass(Text.class);
        preSolveJob.setMapOutputValueClass(IntWritable.class);
        preSolveJob.setOutputKeyClass(Text.class);
        preSolveJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(preSolveJob, new Path("/data/wuxia_novels"));
        FileOutputFormat.setOutputPath(preSolveJob, new Path("/user/2019st34/out1"));
        preSolveJob.waitForCompletion(true);

        Job cooccurrenceJob = Job.getInstance(conf, "lab2 Job");
        cooccurrenceJob.setJarByClass(Main.class);
        cooccurrenceJob.setMapperClass(cooccurrence.cooccurrenceMapper.class);
        cooccurrenceJob.setReducerClass(cooccurrence.cooccurrenceReducer.class);
        cooccurrenceJob.setMapOutputKeyClass(Text.class);
        cooccurrenceJob.setMapOutputValueClass(IntWritable.class);
        cooccurrenceJob.setOutputKeyClass(Text.class);
        cooccurrenceJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(cooccurrenceJob, new Path("/user/2019st34/out1"));
        FileOutputFormat.setOutputPath(cooccurrenceJob, new Path("/user/2019st34/out2"));
        cooccurrenceJob.waitForCompletion(true);

        Job RelationGraphics = Job.getInstance(conf, "RG");
        RelationGraphics.setJarByClass(Main.class);
        RelationGraphics.setInputFormatClass(KeyValueTextInputFormat.class);
        RelationGraphics.setMapperClass(RelationGraphicsMapper.class);
        RelationGraphics.setReducerClass(RelationGraphicsReducer.class);
        RelationGraphics.setMapOutputValueClass(Text.class);
        RelationGraphics.setMapOutputKeyClass(Text.class);
        RelationGraphics.setOutputKeyClass(Text.class);
        RelationGraphics.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(RelationGraphics, new Path("/user/2019st34/out2"));
        FileOutputFormat.setOutputPath(RelationGraphics,new Path("/user/2019st34/out3"));
        RelationGraphics.waitForCompletion(true);

        Job GraphBuilder = Job.getInstance(conf, "Graph");
        GraphBuilder.setJarByClass(Main.class);
        GraphBuilder.setInputFormatClass(KeyValueTextInputFormat.class);
        GraphBuilder.setMapperClass(GraphBuilderMapper.class);
        GraphBuilder.setMapOutputKeyClass(Text.class);
        GraphBuilder.setMapOutputValueClass(Text.class);
        GraphBuilder.setOutputValueClass(Text.class);
        GraphBuilder.setOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(GraphBuilder, new Path("/user/2019st34/out3"));
        FileOutputFormat.setOutputPath(GraphBuilder,new Path("/user/2019st34/mid4" + "/Data" + String.valueOf(0)));
        GraphBuilder.waitForCompletion(true);

        for(int i = 0; i < 10; i++){
            Job PRIter = Job.getInstance(conf, "PRIter");
            PRIter.setJarByClass(Main.class);
            PRIter.setInputFormatClass(KeyValueTextInputFormat.class);
            PRIter.setMapperClass(PRIterMappper.class);
            PRIter.setReducerClass(PRIterReduce.class);
            PRIter.setMapOutputValueClass(Text.class);
            PRIter.setOutputValueClass(Text.class);
            PRIter.setMapOutputKeyClass(Text.class);
            PRIter.setOutputKeyClass(Text.class);
            FileInputFormat.addInputPath(PRIter, new Path("/user/2019st34/mid4" + "/Data" + String.valueOf(i)));
            FileOutputFormat.setOutputPath(PRIter,new Path("/user/2019st34/mid4" + "/Data" + String.valueOf(i + 1)));
            PRIter.waitForCompletion(true);
        }

        Job PRVJob = Job.getInstance(conf, "PRV");
        PRVJob.setJarByClass(Main.class);
        PRVJob.setInputFormatClass(KeyValueTextInputFormat.class);
        PRVJob.setMapperClass(PageRankViewerMapper.class);
        PRVJob.setMapOutputKeyClass(Text.class);
        PRVJob.setMapOutputValueClass(FloatWritable.class);
        PRVJob.setOutputKeyClass(Text.class);
        PRVJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(PRVJob, new Path("/user/2019st34/mid4" + "/Data" + 10));
        FileOutputFormat.setOutputPath(PRVJob, new Path("/user/2019st34/out4"));
        PRVJob.waitForCompletion(true);

        Label labelJob = new Label();
        labelJob.start();

        Job totalSortJob = Job.getInstance(conf, "TotalSort Job");
        totalSortJob.setJarByClass(Main.class);
        totalSortJob.setInputFormatClass(KeyValueTextInputFormat.class);
        totalSortJob.setMapperClass(TotalSort.TotalSortMapper.class);
        totalSortJob.setReducerClass(TotalSort.TotalSortReducer.class);
        totalSortJob.setMapOutputKeyClass(FloatWritable.class);
        totalSortJob.setMapOutputValueClass(Text.class);
        totalSortJob.setOutputKeyClass(Text.class);
        totalSortJob.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(totalSortJob, new Path("/user/2019st34/out4"));
        FileOutputFormat.setOutputPath(totalSortJob, new Path("/user/2019st34/out6_1"));
        totalSortJob.setNumReduceTasks(1);
        totalSortJob.waitForCompletion(true);


        Job clusterJob = Job.getInstance(conf, "Cluster Job");
        clusterJob.setJarByClass(Main.class);
        clusterJob.setInputFormatClass(KeyValueTextInputFormat.class);
        clusterJob.setMapperClass(Cluster.ClusterMapper.class);
        clusterJob.setReducerClass(Cluster.ClusterReducer.class);
        clusterJob.setMapOutputKeyClass(IntWritable.class);
        clusterJob.setMapOutputValueClass(Text.class);
        clusterJob.setOutputKeyClass(IntWritable.class);
        clusterJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(clusterJob, new Path("/user/2019st34/out5_iter9"));
        FileOutputFormat.setOutputPath(clusterJob, new Path("/user/2019st34/out6_2"));
        clusterJob.waitForCompletion(true);
    }
}
