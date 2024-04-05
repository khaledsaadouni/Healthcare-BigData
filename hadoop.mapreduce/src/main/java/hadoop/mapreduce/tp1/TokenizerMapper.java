package hadoop.mapreduce.tp1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class TokenizerMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text sentiment_plateform = new Text();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if (!line.startsWith("id")) {
            // Splitting based on one or more spaces or tabs
            String[] fields = line.split("\t");
                String sentiment = fields[3];
                String plateform = fields[6];
                sentiment_plateform.set(sentiment + "," + plateform);
                System.out.println(sentiment_plateform);
                context.write(sentiment_plateform, one);
        }
    }
}
