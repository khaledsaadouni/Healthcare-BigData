# Healthcare Data Visualization Project

## Overview

This project aims to visualize massive healthcare data containing information about patients, doctors, medical conditions, and hospitals. The architecture follows a lambda architecture, leveraging tools such as Kafka, Spark Streaming, HBase, Hadoop HDFS, and Flask for data processing, storage, and visualization.

## Project Structure

- **Batch Layer**:
  - Utilizes Hadoop HDFS for storing batch data.
  - Processing batch data using MapReduce.

- **Speed Layer**:
  - Kafka is used as a real-time data streaming platform.
  - Spark Streaming processes real-time data.

- **Serving Layer**:
  - HBase serves as the NoSQL database for serving real-time data queries.

- **Presentation Layer**:
  - Flask is used to develop a web application for visualizing the processed healthcare data.

## Steps to Run the Project

1. **Set Up Hadoop Cluster**:
   - Run the Hadoop cluster with the provided Docker image (`liliasfaxi/hadoop-cluster:old`).
     ```bash
     docker run -itd --net=hadoop -p 9870:9870 -p 8088:8088 -p 2181:2181 -p 29092:29092 -p 9092:9092 -p 9090:9090 -p 7077:7077 -p 16010:16010 --name hadoop-master --hostname hadoop-master liliasfaxi/hadoop-cluster:old
     ```
   - Start HDFS, HBase, and Kafka.
     ```bash
     ./start-hadoop.sh
     start-hbase.sh
     ./start-kafka-zookeeper.sh
     ```

2. **Upload Data**:
   - Copy the healthcare dataset (`healthcare_dataset.txt`) into the Hadoop master node.
     ```bash
     docker cp src/main/resources/input/healthcare_dataset.txt hadoop-master:/root/healthcare_dataset.txt
     ```
   - Upload the dataset to HDFS.
     ```bash
     hdfs dfs -put healthcare_dataset.txt input
     ```

3. **Configure MapReduce**:
   - Update `mapred-site.xml` to set the MapReduce framework to YARN.
     ```bash
     nano /usr/local/hadoop/etc/hadoop/mapred-site.xml
     ```
     ```xml
     <configuration>
         <property>
             <name>mapreduce.framework.name</name>
             <value>yarn</value>
         </property>
         <property>
             <name>yarn.app.mapreduce.am.env</name>
             <value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value>
         </property>
         <property>
             <name>mapreduce.map.env</name>
             <value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value>
         </property>
         <property>
             <name>mapreduce.reduce.env</name>
             <value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value>
         </property>
     </configuration>
     ```

4. **Build and Deploy Application**:
   - Compile and package the Java applications (`HealthCare.jar` and `stream-kafka-spark-1-jar-with-dependencies.jar`).
     ```bash
     apt update && apt install nano && apt install maven
     ```
   - Copy the JAR files to the Hadoop master node.
     ```bash
     docker cp target/HealthCare-1.0-SNAPSHOT-jar-with-dependencies.jar hadoop-master:/root/HealthCare.jar
     docker cp target/stream-kafka-spark-1-jar-with-dependencies.jar hadoop-master:/root
     ```

5. **Create Kafka Topic**:
   - Create a Kafka topic named `Healthcare` for data streaming.
     ```bash
     kafka-topics.sh --create --topic Healthcare --replication-factor 1 --partitions 1 --bootstrap-server localhost:9092
     ```

6. **Run Spark Streaming**:
   - Submit the Spark job to consume data from Kafka and perform word count analysis.
     ```bash
     spark-submit --class spark.kafka.SparkKafkaWordCount --master local stream-kafka-spark-1-jar-with-dependencies.jar localhost:9092 Healthcare mySparkConsumerGroup >> out
     ```

7. **Stream Data**:
   - Compile and run the Java SimpleProducer to stream healthcare data into Kafka.
     ```bash
     javac -cp "$KAFKA_HOME/libs/*":. SimpleProducer.java
     java -cp "$KAFKA_HOME/libs/*":. SimpleProducer Healthcare
     ```

8. **Visualize Data**:
   - Develop the Flask web application to visualize processed healthcare data.

## Directory Structure

- **src/main/resources/input/**: Contains the healthcare dataset.
- **target/**: Contains compiled Java applications and JAR files.

## Prerequisites

- Docker installed for running the Hadoop cluster.
- Java Development Kit (JDK) for compiling Java applications.
- Maven for building Java applications.
- Kafka, Spark, and HBase dependencies.

## Usage

1. Clone the repository.
2. Follow the steps outlined in the README to set up the project and run the applications.
3. Access the Flask web application for healthcare data visualization.

## Contributors

- [Your Name]
- [Contributor 1]
- [Contributor 2]

## License

This project is licensed under the [License Name] License - see the [LICENSE.md](LICENSE.md) file for details.
