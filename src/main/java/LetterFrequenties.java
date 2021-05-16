import maxEntropyModel.EntropyMapper;
import maxEntropyModel.EntropyReducer;
import bigram.BigramMapper;
import bigram.BigramReducer;
import frequency.frequencyMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import percentage.PercentageMapper;
import percentage.PercentageReducer;

public class LetterFrequenties {

    public static void main(String[] args) throws Exception {

        //JOB TO CREATE BIGRAMS
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Job job = new Job(conf, "createBigram");

        job.setJarByClass(LetterFrequenties.class);
        job.setMapperClass(BigramMapper.class);
        job.setReducerClass(BigramReducer.class);

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
        job2.setMapperClass(frequencyMapper.class);
        job2.setReducerClass(BigramReducer.class);

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
        job3.setMapperClass(PercentageMapper.class);
        job3.setReducerClass(PercentageReducer.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);

        SequenceFileInputFormat.setInputPaths(
                job3,
                new Path("createBigram_output"),
                new Path("firstLetterCount_output"));

        FileOutputFormat.setOutputPath(job3, new Path("probability_output"));

        job3.setInputFormatClass(TextInputFormat.class);
        job3.waitForCompletion(true);


        // JOB TO DIVIDE DIFFERENT PROBABILITIES PER LANGUAGE
        Configuration conf4 = new Configuration();
        Job job4 = new Job(conf4, "Probabilities");

        job4.setJarByClass(LetterFrequenties.class);
        job4.setMapperClass(EntropyMapper.class);
        job4.setReducerClass(EntropyReducer.class);

        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(DoubleWritable.class);

        SequenceFileInputFormat.setInputPaths(
                job4,
                new Path("EN_probability"),
                new Path("NL_probability"),
                new Path("probability_output")
        );


        FileOutputFormat.setOutputPath(job4, new Path(args[1]));

        job4.setInputFormatClass(TextInputFormat.class);
        job4.waitForCompletion(true);


        // DELETE INTERMEDIATE OUTPUTS
        fs.delete(new Path("createBigram_output"), true);
        fs.delete(new Path("firstLetterCount_output"), true);
        fs.delete(new Path("probability_output"), true);

    }
}
