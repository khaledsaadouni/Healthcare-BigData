package spark.kafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import scala.Function2;
import scala.Tuple2;
import scala.runtime.BoxedUnit;


public class SparkKafkaWordCount {

    private static Table table1;
    private static String tableName = "status";
    private static String family1 = "count";
    private static String family2 = "count2";

    public static void main(String[] args) throws Exception {


        if (args.length < 3) {
            System.err.println("Usage: SparkKafkaWordCount <bootstrap-servers> <subscribe-topics> <group-id>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topics = args[1];
        String groupId = args[2];

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkKafkaWordCount")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from Kafka
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topics)
                .option("kafka.group.id", groupId)
                .load();


        Configuration conf = new Configuration();
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family1))
                    .build();

            System.out.println("Connecting");
            System.out.println("Creating Table");
            createOrOverwrite(admin, tableDescriptor);
            System.out.println("Done......");

            table1 = connection.getTable(TableName.valueOf(tableName));
            df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                    .as(Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
                    .flatMap((FlatMapFunction<Tuple2<String, String>, String>) value -> {
                        String[] columns = value._2.split("\t");
                        if (columns.length >= 14) {
                            return Arrays.asList(columns[14]).iterator();
                        } else {
                            return Collections.emptyIterator();
                        }
                    }, Encoders.STRING())
                    .groupByKey((MapFunction<String, String>) value -> value, Encoders.STRING())
                    .count()
                    .writeStream()
                    .option("checkpointLocation", "/tmp/spark-checkpoint") // Add checkpointing for fault tolerance
                    .outputMode("complete")
                    .format("console")
                    .foreachBatch((Function2<Dataset<Tuple2<String, Object>>, Object, BoxedUnit>) (batchDF, batchId) ->{
                        batchDF.foreach((ForeachFunction<Tuple2<String, Object>>) row -> {
                            Put put = new Put(Bytes.toBytes(row._1));
                            put.addColumn(Bytes.toBytes(family1), Bytes.toBytes("count"), Bytes.toBytes(row._2.toString()));
                            table1.put(put);
                        });
                        return null;
                    } )
                    .start()
                    .awaitTermination();
        } finally {
            if (table1 != null) {
                table1.close();
            }
        }
    }

    public static void createOrOverwrite(Admin admin, TableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }
}
