package hadoop.mapreduce.tp1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MedicalConditionByAgeTokenizerMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text healthcare_plateform = new Text();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if (!line.startsWith("Name")) {
            // Splitting based on one or more spaces or tabs
            String[] fields = line.split("\t");
            String age = fields[1];
            String condition = fields[4];
            int ageInt = Integer.parseInt(age);
            String range = "";
            if (ageInt < 15) {
                range = "0-14";
            } else if (ageInt < 30) {
                range = "15-29";
            } else if (ageInt < 45) {
                range = "30-44";
            } else if (ageInt < 60) {
                range = "45-59";
            } else if (ageInt < 75) {
                range = "60-74";
            } else {
                range = "75+";
            }
            healthcare_plateform.set(range+'\t'+condition);
            System.out.println(healthcare_plateform);
            context.write(healthcare_plateform, one);
        }
    }
}
