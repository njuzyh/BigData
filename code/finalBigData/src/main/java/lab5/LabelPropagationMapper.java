package lab5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class LabelPropagationMapper extends Mapper<Text, Text, Text, TextArrayWritable> {

    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        double tol = conf.getDouble("tol", 0.001);
        boolean weighted = conf.getBoolean("weighted", true);
        String update = conf.get("update");
        String[] prevDistributionStr = conf.getStrings(key.toString());
        double[] prevDistribution = new double[prevDistributionStr.length];
        for (int i = 0; i < prevDistribution.length; ++i) {
            prevDistribution[i] = Double.valueOf(prevDistributionStr[i]);
        }

        double[] distribution = new double[prevDistribution.length];
        for (int i = 0; i < distribution.length; ++i) {
            distribution[i] = 0.0;
        }

        String[] neighbors = value.toString().split("\\s*;\\s*");

        for (String neighbor : neighbors) {
            int pos = neighbor.indexOf(",");
            String neighborName = neighbor.substring(0, pos);
            double neighborWeight;
            if (weighted) {
                neighborWeight = Double.parseDouble(neighbor.substring(pos + 1));
            } else {
                neighborWeight = 1.0;
            }

            String[] neighborDistribution = conf.getStrings(neighborName);
            for (int i = 0; i < neighborDistribution.length; ++i) {
                double prob = Double.parseDouble(neighborDistribution[i]);
                distribution[i] += neighborWeight * prob;
            }
        }

        if (weighted) {
            double sum = 0.0;
            for (double prob : distribution) {
                sum += prob;
            }
            for (int i = 0; i < distribution.length; ++i) {
                distribution[i] /= sum;
            }

            double[] delta = new double[distribution.length];
            for (int i = 0; i < delta.length; ++i) {
                delta[i] = distribution[i] - prevDistribution[i];
            }
            double norm = 0.0;
            for (double d : delta) {
                norm += d * d;
            }
            norm = Math.sqrt(norm);

            if (norm < tol) {
                conf.setBoolean(key.toString() + "_status", true);
            } else {
                conf.setBoolean(key.toString() + "_status", false);
            }

        } else {
            double max = 0;
            int posOfMax = 0;
            for (int i = 0; i < distribution.length; ++i) {
                if (distribution[i] > max) {
                    max = distribution[i];
                    posOfMax = i;
                }
            }
            int pos = 0;
            for (int i = 0; i < prevDistribution.length; ++i) {
                if ((int) prevDistribution[i] == 1) {
                    pos = i;
                    break;
                }
            }
            if ((int) distribution[pos] == (int) max) {
                conf.setBoolean(key.toString() + "_status", true);
            } else {
                conf.setBoolean(key.toString() + "_status", false);
            }
            for (int i = 0; i < distribution.length; ++i) {
                distribution[i] = 0.0;
            }
            distribution[posOfMax] = 1.0;
        }
        String[] distributionStr = new String[distribution.length];
        for (int i = 0; i < distributionStr.length; ++i) {
            distributionStr[i] = String.valueOf(distribution[i]);
        }
        context.write(key, new TextArrayWritable(distributionStr));
        if (update.equals("async")) {
            conf.setStrings(key.toString(), distributionStr);
        }
    }
}
