package frequenties;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class frequentyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("[^a-zA-Z]");

        // COUNT AMOUNT OF SAME FIRST LETTER
        for (String s : tokens) {

            context.write(new Text(String.valueOf(s.charAt(0))), new IntWritable(1));
        }



    }
}
