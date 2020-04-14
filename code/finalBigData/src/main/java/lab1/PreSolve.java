package lab1;

import org.ansj.splitWord.analysis.DicAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.util.List;
import java.util.HashSet;

import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;

public class PreSolve {

    public static class PreSolveMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void setup(Context context) {
            try {
                Path pt = new Path("hdfs:/data/task2/people_name_list.txt");
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                line = br.readLine();
                while (line != null) {
                    DicLibrary.insert(DicLibrary.DEFAULT, line);
                    line = br.readLine();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            List<Term> terms = DicAnalysis.parse(line).getTerms();
            HashSet<String> termSet = new HashSet<>();
            for (Term tmp : terms) {
                if (tmp.getNatureStr().equals("userDefine")) {
                    termSet.add(tmp.getName());
                }
            }
            StringBuffer posting = new StringBuffer();
            for (String tmp : termSet) {
                posting.append(tmp);
                posting.append(" ");
            }
            if (posting.length() > 0) {
                posting.deleteCharAt(posting.length() - 1);
                String post = posting.toString();
                context.write(new Text(post), one);
            }
        }
    }

    public static class PreSolveReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values) sum += i.get();
            result.set(sum);
            context.write(key, result);
        }
    }
}
