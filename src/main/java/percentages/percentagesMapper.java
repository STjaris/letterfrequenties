package percentages;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class percentagesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s");

        ArrayList<String> bigram = new ArrayList<>();
        ArrayList<Integer> bigramCount = new ArrayList<>();

        ArrayList<String> total = new ArrayList<>();
        ArrayList<Integer> totalCount = new ArrayList<>();


        // SET TOKENS IN MAP
        for (int i = 0; i < tokens.length; i++) {
            if (i % 2 == 1) {
                bigramCount.add(Integer.parseInt(tokens[i]));
            } else {
                bigram.add(tokens[i]);
            }
        }

        for(int i = 0; i < bigram.size(); i++){
            if(bigram.get(i).length() == 1){
                // ADD TO TOTAL LIST
                total.add(bigram.get(i));
                totalCount.add(bigramCount.get(i));

                // REMOVE FROM BIGRAM LIST
                bigram.remove(i);
                bigramCount.remove(i);
            }
        }

        for(String s : bigram){
            System.out.println(s);
        }

        for(int s : bigramCount){
            System.out.println(s);
        }


//        for(String s : bigram){
//            System.out.println(s);
//        }




        // SPLIT BIGRAM FROM TOTALCOUNT

        // DIVIDE BIGRAM COUNT BY TOTAL AMOUNT



    }
}
