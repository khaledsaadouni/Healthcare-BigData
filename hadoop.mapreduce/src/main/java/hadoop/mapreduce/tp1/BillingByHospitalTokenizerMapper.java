package hadoop.mapreduce.tp1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BillingByHospitalTokenizerMapper
        extends Mapper<LongWritable, Text, Text, FloatWritable> {

    private Text healthcare_plateform = new Text();
    private FloatWritable billingAmount = new FloatWritable();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if (!line.startsWith("Name")) {
            // Splitting based on one or more spaces or tabs
            String[] fields = line.split("\t");
            String hospital = fields[8];
            String amount = fields[9];
            healthcare_plateform.set(hospital);
            billingAmount.set( Float.parseFloat(amount));
            context.write(healthcare_plateform, billingAmount);
        }
    }
}
