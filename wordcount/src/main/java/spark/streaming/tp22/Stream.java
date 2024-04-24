package spark.streaming.tp22;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;
import java.util.Arrays;

public class Stream {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException  {
        SparkSession spark = SparkSession
                .builder()
                .appName("NetworkWordCount")
                .master("local[*]")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to localhost:9999
        Dataset<String> lines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9000)
                .load()
                .as(Encoders.STRING());

        // Split the lines into words
        Dataset<String> concatenated = lines.flatMap(
                (String x) -> {
                    String[] parts = x.split("\\t"); // Split by tabulation
                        String concat = parts[14]; // Concatenate first and second columns
                        return Arrays.asList(concat).iterator();
                },
                Encoders.STRING());

        // Generate running count of concatenated strings
        Dataset<Row> concatenatedCounts = concatenated.groupBy("value").count();

        // Start running the query that prints the running counts to the console
        StreamingQuery query = concatenatedCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .trigger(Trigger.ProcessingTime("1 second"))
                .start();

        query.awaitTermination();
    }
}
