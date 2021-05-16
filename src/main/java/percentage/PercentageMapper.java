package percentage;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class PercentageMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    static Map<String, Double> bigramMap = new HashMap<>();
    static Map<String, Double> totalMap = new HashMap<>();

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\n");

        // SET TOKENS IN MAP
        for (String s : tokens) {
            double probability = Double.parseDouble(s.split("\\s+")[1]);
            String bigram = s.split("\\s+")[0];
            if (context.getInputSplit().toString().contains("createBigram_output")) {
                bigramMap.put(bigram, probability);
            } else if (context.getInputSplit().toString().contains("firstLetterCount_output")) {
                totalMap.put(bigram, probability);
            }
        }

        // DIVIDE BIGRAM COUNT BY TOTAL AMOUNT
        for (Map.Entry<String, Double> entry : bigramMap.entrySet()) {
            for (Map.Entry<String, Double> o : totalMap.entrySet()) {
                if (entry.getKey().charAt(0) == o.getKey().charAt(0)) {
                    double i = entry.getValue() / o.getValue();
                    context.write(new Text(entry.getKey()), new DoubleWritable(i));
                }
            }
        }
    }
}
