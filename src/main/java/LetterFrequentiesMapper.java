import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class LetterFrequentiesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

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

        // CREATES BIGRAM IF PREV != i
        for(String i : list){
            if(!prev.equals(i) && i != null){
                String bigram = prev + i;
                context.write(new Text(bigram), new IntWritable(1));
            }
            prev = i;
        }
    }
}
