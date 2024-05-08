import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleProducer {

    public static void main(String[] args) throws Exception{

        // Verifier que le topic est donne en argument
        if(args.length == 0){
            System.out.println("Entrer le nom du topic");
            return;
        }

        // Assigner topicName a une variable
        String topicName = args[0].toString();

        // Creer une instance de proprietes pour acceder aux configurations du producteur
        Properties props = new Properties();

        // Assigner l'identifiant du serveur kafka
        props.put("bootstrap.servers", "localhost:9092");

        // Definir un acquittement pour les requetes du producteur
        props.put("acks", "all");

        // Si la requete echoue, le producteur peut reessayer automatiquemt
        props.put("retries", 0);

        // Specifier la taille du buffer size dans la config
        props.put("batch.size", 16384);

        // buffer.memory controle le montant total de memoire disponible au producteur pour le buffering
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer
                <String, String>(props);
        BufferedReader reader = new BufferedReader(new FileReader("dataset.txt"));

        Random random = new Random();

        String line;
        StringBuilder fileContent = new StringBuilder();
        while ((line = reader.readLine()) != null) {
            fileContent.append(line).append("\n");
        }

        // Split the content into lines
        String[] lines = fileContent.toString().split("\n");

        for(int i = 0; i < 100; i++) {
            // Select a random line
            int randomIndex = random.nextInt(lines.length);
            String randomLine = lines[randomIndex];

            // Send the random line to the Kafka topic
            producer.send(new ProducerRecord<>(topicName, Integer.toString(randomIndex), randomLine));

            // Wait for 3 seconds before sending the next random line
            TimeUnit.SECONDS.sleep(3);
        }
        producer.close();
        System.out.println("Message envoye avec succes");

    }
}
