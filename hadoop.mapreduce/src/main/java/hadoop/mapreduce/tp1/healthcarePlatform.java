package hadoop.mapreduce.tp1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class healthcarePlatform {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] jobNames = {"genders", "bloodtype", "age", "MedicalCondition", "MedicalConditionByAge", "MedicalConditiongender", "billing by hospital"};
        Class[] mapperClasses = {TokenizerMapper.class, BloodTypeTokenizerMapper.class, AgeTokenizerMapper.class, MedicalConditionTokenizerMapper.class, MedicalConditionByAgeTokenizerMapper.class, MedicalConditionByGenderTokenizerMapper.class, BillingByHospitalTokenizerMapper.class};
        for (int i = 0; i < jobNames.length; i++) {
            Job job = Job.getInstance(conf, jobNames[i]);
            job.setJarByClass(healthcarePlatform.class);
            job.setMapperClass(mapperClasses[i]);
            if(i==6){
                job.setCombinerClass(BillingByHospitalReducer.class);
                job.setReducerClass(BillingByHospitalReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(FloatWritable.class);
            }else{

                job.setCombinerClass(healthcarePlateformReducer.class);
                job.setReducerClass(healthcarePlateformReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
            }
            FileInputFormat.addInputPath(job, new Path("src/main/resources/input/healthcare_dataset.txt"));
            FileOutputFormat.setOutputPath(job, new Path("src/main/resources/output"+i));
            job.waitForCompletion(true);
        }
    }

}

