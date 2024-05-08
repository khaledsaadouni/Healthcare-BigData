package hadoop.mapreduce.tp1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class healthcarePlatform {

    private static Table table1;
    private static String tableName = "gender2";
    private static String family1 = "count";
    private static String family2 = "count2";
    public static void createOrOverwrite(Admin admin, TableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }
    public static void main(String[] args) throws Exception,IOException {
        Configuration conf = new Configuration();
        String[] jobNames = {"genders", "bloodtype", "age", "MedicalCondition", "MedicalConditionByAge", "MedicalConditiongender", "billingbyHospital"};
        Class[] mapperClasses = {TokenizerMapper.class, BloodTypeTokenizerMapper.class, AgeTokenizerMapper.class, MedicalConditionTokenizerMapper.class, MedicalConditionByAgeTokenizerMapper.class, MedicalConditionByGenderTokenizerMapper.class, BillingByHospitalTokenizerMapper.class};
//        for (int i = 0; i < jobNames.length; i++) {
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            // Create table with column families


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

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]+i));

            System.out.println(job.getOutputFormatClass());
            job.waitForCompletion(true);
            processJobOutput(job, connection,admin);
        }

        } finally {
            if (table1 != null) {
                table1.close();
            }
        }
    }
    public static void processJobOutput(Job job, Connection connection,Admin admin) throws IOException {

        Path outputPath = FileOutputFormat.getOutputPath(job);
        FileSystem fs = FileSystem.get(job.getConfiguration());
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(job.getJobName()));
        Set<String> addedColumnFamilies = new HashSet<>();
        if (outputPath != null) {
            Path outputFile = new Path(outputPath, "part-r-00000"); // Assuming a single reducer output
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(outputFile)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\\s+");
                    if (parts.length >= 2) {
                        if (parts.length >= 3) {
                            String value = parts[1];

                            if (!addedColumnFamilies.contains(value)) {
                                // Column family not added yet, add it to the table descriptor
                                tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(value));
                                addedColumnFamilies.add(value); // Add to the set of added column families
                            } else {
                                System.out.println("Column family " + value + " already added.");
                                // Handle duplicate column family (if needed)
                            }
                        }
                    }
                }
            }
        }
        tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.of("count"));
        TableDescriptor tableDescriptor =  tableDescriptorBuilder.build();
        System.out.println("Connecting");
        System.out.println("Creating Table");
        createOrOverwrite(admin, tableDescriptor);
        System.out.println("Done......☻");
        table1 = connection.getTable(TableName.valueOf(job.getJobName()));
        if (outputPath != null) {
            Path outputFile = new Path(outputPath, "part-r-00000"); // Assuming a single reducer output
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(outputFile)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\\s+"); // Split by one or more whitespace characters
                    if (parts.length >= 2) {
                        String key = parts[0];
                        String value = parts[1];
                        System.out.println("**********************************************************************************Key: " + key + ", Value: " + value);

                        // Construct Put object and add to HBase table
                        try (Table table = connection.getTable(table1.getName())) {
                            Put put = new Put(Bytes.toBytes(key));
                            if (parts.length >= 3) {
                                put.addColumn(Bytes.toBytes(parts[1]), Bytes.toBytes("string"), Bytes.toBytes(parts[2]));
                            }else{
                            put.addColumn(Bytes.toBytes("count"), Bytes.toBytes("number"), Bytes.toBytes(value));}
                            table.put(put);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }
}

