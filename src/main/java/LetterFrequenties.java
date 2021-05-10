import bigram.bigramReducer;
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
        Configuration conf = new Configuration();

        Job job = new Job(conf, "wordcount");
        job.setJarByClass(LetterFrequenties.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        String pathname = args[1];
        FileOutputFormat.setOutputPath(job, new Path(pathname));

//        job.setMapperClass(bigramMapper.class);
//        job.setMapperClass(frequentyMapper.class);
        job.setMapperClass(percentagesMapper.class);

        job.setReducerClass(bigramReducer.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }
}
