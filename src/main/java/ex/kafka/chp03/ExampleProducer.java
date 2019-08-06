package ex.kafka.chp03;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ExampleProducer {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);

        ProducerRecord<String, String> record =
                new ProducerRecord<>("CustomerCountry", "Precision Products", "France");

        //async
        try {
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //sync
        try {
            producer.send(record).get();
        } catch (Exception e) {
            e.printStackTrace();
        }

        //async with callback
        record = new ProducerRecord<>("CustomerCountry", "Biomedical Materials", "USA");
        producer.send(record, (recordMetadata, e) -> {
            if (e != null)
                e.printStackTrace();
        });



        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
