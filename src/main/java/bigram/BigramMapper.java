package bigram;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class BigramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("[^a-zA-Z]");
        ArrayList<String> list = new ArrayList<>();
        String prev = "";

        // ADDS EVERY CHAR OF A STRING TO A LIST
        for (String s : tokens) {
            for(char ch : s.toCharArray()){
                list.add(String.valueOf(ch));
            }
        }

        // CREATES BIGRAM
        for(String i : list){
            String bigram = prev + i;
            if(bigram.length() == 2){
                context.write(new Text(bigram), new IntWritable(1));
            }
            prev = i;
        }
    }
}
