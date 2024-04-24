package hadoop.mapreduce.tp1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BillingByHospitalReducer
        extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private FloatWritable totalBilling = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        float sum = 0.0f;
        for (FloatWritable value : values) {
            sum += value.get();
        }
        totalBilling.set(sum);
        context.write(key, totalBilling);
    }
}