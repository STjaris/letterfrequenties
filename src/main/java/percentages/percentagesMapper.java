package percentages;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class percentagesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s");


        Map<String, String> bigramMap = new HashMap<>();
        Map<String, String> totalMap = new HashMap<>();


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
        for (Map.Entry<String, String> entry : bigramMap.entrySet()) {

            for (Map.Entry<String, String> o : totalMap.entrySet()) {

                System.out.println(o);
                if (entry.getKey().charAt(0) == o.getKey().charAt(0)) {
                    int i = Integer.parseInt(entry.getValue()) / Integer.parseInt(o.getValue());

                    //System.out.println(entry + " : " + i);

                    context.write(new Text(entry.getKey()), new DoubleWritable(i));
                }
            }

//            if(entry.getKey().charAt(0) = )
        }


    }
}
