package hadoop.mapreduce.tp1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class sentiment_plateform {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sentiment plateform");
        job.setJarByClass(sentiment_plateform.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(sentimentPlateformReducer.class);
        job.setReducerClass(sentimentPlateformReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("src/main/resources/input/sentimentdataset.txt"));
        FileOutputFormat.setOutputPath(job, new Path("src/main/resources/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

