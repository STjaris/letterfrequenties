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

        ArrayList<String> bigramList = new ArrayList<>();
        ArrayList<Integer> bigramCount = new ArrayList<>();

        ArrayList<String> totalList = new ArrayList<>();
        ArrayList<Integer> totalCount = new ArrayList<>();

        ArrayList<String> returnList = new ArrayList<>();
        ArrayList<Double> returnCount = new ArrayList<>();

        String s = "";
        int iq = 0;

        // SET TOKENS IN MAP
        for (int i = 0; i < tokens.length; i++) {
            if (i % 2 == 1) {
                bigramCount.add(Integer.parseInt(tokens[i]));
            } else {
                bigramList.add(tokens[i]);
            }
        }

        // SEPERATE BIGRAM FROM COUNT
        for(int i = 0; i < bigramList.size(); i++){
            if(bigramList.get(i).length() == 1){
                // ADD TO TOTAL LIST
                totalList.add(bigramList.get(i));
                totalCount.add(bigramCount.get(i));

                // REMOVE FROM BIGRAM LIST
                bigramList.remove(i);
                bigramCount.remove(i);
            }
        }

        // DIVIDE BIGRAM COUNT BY TOTAL AMOUNT
        for(int i = 0; i < bigramList.size(); i++){

            s = String.valueOf(bigramList.get(i).charAt(0));
            iq = i;

        }

        for(String st : totalList){
            if(st.equals(s)){
                int index = totalList.indexOf(s);
                double d = bigramCount.get(iq) / totalCount.get(index);
                returnList.add(bigramList.get(iq));
                returnCount.add(d);
            }
        }




        for(String st : returnList){
            System.out.println(st);
        }

        for(double st : returnCount){
            System.out.println(st);
        }





    }
}
