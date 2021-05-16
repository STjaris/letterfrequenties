package MEM;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class memMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    static Map<String, Double> NLProbabilityMap = new HashMap<>();
    static Map<String, Double> ENProbabilityMap = new HashMap<>();
    static Map<String, Double> INPUTProbabilityMap = new HashMap<>();

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\n");

        double NLScore = 0;
        double ENScore = 0;

        // SET TOKENS IN MAP
        for(String s : tokens){
            double probability = Double.parseDouble(s.split("\\s+")[1]);
            String bigram = s.split("\\s+")[0];
            if(context.getInputSplit().toString().contains("NL_probability")){
                NLProbabilityMap.put(bigram, probability);
            } else if(context.getInputSplit().toString().contains("EN_probability")){
                ENProbabilityMap.put(bigram, probability);
            } else if(context.getInputSplit().toString().contains("probability_output")){
                INPUTProbabilityMap.put(bigram, probability);
            }
        }

        for (Map.Entry<String, Double> entry : INPUTProbabilityMap.entrySet()) {
            for (Map.Entry<String, Double> ENEntry : ENProbabilityMap.entrySet()) {
                if (entry.getKey().equals(ENEntry.getKey())) {
                    ENScore = entry.getValue() * ENEntry.getValue();
                    context.write(new Text("EN"), new DoubleWritable(ENScore));
                }
            }
            for (Map.Entry<String, Double> NLEntry : NLProbabilityMap.entrySet()) {
                if (entry.getKey().equals(NLEntry.getKey())) {
                    NLScore = entry.getValue() * NLEntry.getValue();
                    context.write(new Text("NL"), new DoubleWritable(NLScore));
                }
            }
        }
    }
}
