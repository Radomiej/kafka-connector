package pl.radomiej.playground;

import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Arrays;
import java.util.Properties;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:3501");
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://192.168.1.144:3501");
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://192.168.1.21:3541");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> data;
            if (i % 2 == 0) {
                data = new ProducerRecord<String, String>("even", 0, Integer.toString(i), String.format("%d is even", i));
            } else {
                data = new ProducerRecord<String, String>("odd", 0, Integer.toString(i), String.format("%d is odd", i));
            }
            producer.send(data, (m, e) -> callback(m, e));
            System.out.println("Wysłano ping do kafki");
            Thread.sleep(1L);
        }
        producer.close();
    }

    private static void callback(RecordMetadata recordMetadata, Exception exception) {
        System.out.println("record metadata: " + recordMetadata != null ? recordMetadata.toString() : "");
        System.out.println("exception: " + exception != null ? exception.toString() : "");
    }
}
