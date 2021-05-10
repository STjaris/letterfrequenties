import bigram.bigramMapper;
import bigram.bigramReducer;
import frequenties.frequentyMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import percentages.percentagesMapper;

public class LetterFrequenties {

    public static void main(String[] args) throws Exception {

        //JOB TO CREATE BIGRAMS
        Configuration conf = new Configuration();
        Job job = new Job(conf, "createBigram");

        job.setJarByClass(LetterFrequenties.class);
        job.setMapperClass(bigramMapper.class);
        job.setReducerClass(bigramReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("createBigram_output"));

        job.setInputFormatClass(TextInputFormat.class);
        job.waitForCompletion(true);

        // JOB TO COLLECT AND COUNT THE AMOUNT OF FIRST LETTERS
        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf2, "firstLetterCount");

        job2.setJarByClass(LetterFrequenties.class);
        job2.setMapperClass(frequentyMapper.class);
        job2.setReducerClass(bigramReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path("createBigram_output"));
        FileOutputFormat.setOutputPath(job2, new Path("firstLetterCount_output"));

        job2.setInputFormatClass(TextInputFormat.class);
        job2.waitForCompletion(true);

        // JOB TO CALCULATE THE PERCENTAGES BIGRAMS PER TOTAL FIRST LETTER COUNT
        Configuration conf3 = new Configuration();
        Job job3 = new Job(conf3, "calculatePercentages");

        job3.setJarByClass(LetterFrequenties.class);
        job3.setMapperClass(percentagesMapper.class);
        job3.setReducerClass(bigramReducer.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job3, new Path("createBigram_output"));
        FileInputFormat.addInputPath(job3, new Path("firstLetterCount_output"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));

        job3.setInputFormatClass(TextInputFormat.class);
        job3.waitForCompletion(true);

    }
}
