package percentages;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class percentagesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    static Map<String, String> completeMap = new HashMap<>();
    static Map<String, String> bigramMap = new HashMap<>();
    static Map<String, String> totalMap = new HashMap<>();

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s");

        // SET TOKENS IN MAP
        for (int i = 0; i < tokens.length; i++) {
            if (i % 2 == 1) {
                completeMap.put(tokens[i - 1], tokens[i]);
            }
        }

        // SPLIT BIGRAM FROM FIRSTCHAR
        for (Map.Entry<String, String> entry : completeMap.entrySet()) {
            if (entry.getKey().length() == 1) {

                totalMap.put(entry.getKey(), entry.getValue());
            } else {
                bigramMap.put(entry.getKey(), entry.getValue());
            }
        }

        // DIVIDE BIGRAM COUNT BY TOTAL AMOUNT
        for (Map.Entry<String, String> entry : bigramMap.entrySet()) {
            for (Map.Entry<String, String> o : totalMap.entrySet()) {
                if (entry.getKey().charAt(0) == o.getKey().charAt(0)) {
                    double num = Double.parseDouble(entry.getValue());
                    double div = Double.parseDouble(o.getValue());

                    double i = num / div;

                    context.write(new Text(entry.getKey()), new DoubleWritable(i));
                }
            }
        }
    }
}
