package percentages;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.checkerframework.checker.units.qual.A;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class percentagesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s");

        ArrayList bigram = new ArrayList<>();
        ArrayList bigramCount = new ArrayList<>();

        Map<String, String> bigramMap = new HashMap<>();
        Map<String, String> totalMap = new HashMap<>();
        Map<String, Double> returnMap = new HashMap<>();

        // SET TOKENS IN MAP
        for (int i = 0; i < tokens.length; i++) {
            if (i % 2 == 1) {
                bigramMap.put(tokens[i - 1], tokens[i]);
            }
        }

        // SPLIT BIGRAM FROM TOTALCOUNT
        for (Map.Entry<String, String> entry : bigramMap.entrySet()) {
            if (entry.getKey().length() == 1) {

                totalMap.put(entry.getKey(), entry.getValue());
                bigramMap.remove(entry.getKey());
            }


        }

        // DIVIDE BIGRAM COUNT BY TOTAL AMOUNT
        for (Map.Entry<String, String> o : bigramMap.entrySet()) {

            String s = String.valueOf(o.getKey().charAt(0));

            if(totalMap.containsKey(s)){
                double d = Integer.parseInt(o.getValue()) / Integer.parseInt(totalMap.get(s));

                returnMap.put(o.getKey(), d);
            }
        }

        for(Map.Entry<String, Double> entry : returnMap.entrySet()){
            System.out.println(entry);
            context.write(new Text(entry.getKey()), new DoubleWritable(entry.getValue()));

        }



    }
}
