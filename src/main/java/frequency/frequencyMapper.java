package frequency;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class frequencyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().split("\\n");

        // COUNT AMOUNT OF SAME FIRST LETTER
        for (String s : lines) {
            int amount = Integer.parseInt(s.split("\\s+")[1]);
            context.write(new Text(String.valueOf(s.charAt(0))), new IntWritable(amount));
        }



    }
}
