package MEM;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class memMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    static Map<String, String> NLProbabilityMap = new HashMap<>();
    static Map<String, String> ENProbabilityMap = new HashMap<>();
    static Map<String, String> INPUTProbabilityMap = new HashMap<>();


    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s");

        // SET TOKENS IN MAP
        for (int i = 0; i < tokens.length; i++) {
            if (i % 2 == 1) {
                if(NLProbabilityMap.containsKey("zz")){
                    ENProbabilityMap.put(tokens[i - 1], tokens[i]);
                }else{
                    NLProbabilityMap.put(tokens[i - 1], tokens[i]);
                }
            }
        }

        System.out.println("NL: ");
        System.out.println(NLProbabilityMap);

        System.out.println("EN: ");
        System.out.println(ENProbabilityMap);


    }
}
