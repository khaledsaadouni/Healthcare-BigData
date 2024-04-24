package hadoop.mapreduce.tp1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MedicalConditionByGenderTokenizerMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text healthcare_plateform = new Text();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if (!line.startsWith("Name")) {
            // Splitting based on one or more spaces or tabs
            String[] fields = line.split("\t");
            String gender = fields[2];
            String conditions = fields[4];
            healthcare_plateform.set(gender+"\t"+conditions);
            System.out.println(healthcare_plateform);
            context.write(healthcare_plateform, one);
        }
    }
}
