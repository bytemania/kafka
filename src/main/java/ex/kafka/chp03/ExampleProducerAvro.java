package ex.kafka.chp03;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class ExampleProducerAvro {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", KafkaAvroSerializer.class.getName());
        props.put("auto.register.schemas", false);
        props.put("schema.registry.url", "http://localhost:8081");

        String topic = "customerContacts";
        Producer<String, GenericRecord> producer = new KafkaProducer<>(props);

        String schemaString =
                "{" +
                 "  \"namespace\": \"customerManagement.avro\", " +
                 "  \"type\": \"record\", " +
                 "  \"name\": \"Customer\"," +
                 "  \"fields\": [" +
                 "      {\"name\": \"id\", \"type\": \"int\"}," +
                 "      {\"name\": \"name\", \"type\": \"string\"}," +
                "       {\"name\": \"email\", \"type\": " + "[\"null\",\"string\"], \"default\":\"null\" }" +
                "   ]}";

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaString);

        GenericRecord customer = new GenericData.Record(schema);
        customer.put("id", 1);
        customer.put("name", "nameCustomer");
        customer.put("email", "emailCustomer");

        ProducerRecord<String, GenericRecord> data = new ProducerRecord<>("customerContacts", "nameCustomer", customer);

        producer.send(data, (metadata, exception) -> {
            if (exception == null) {
                System.out.println(metadata);
            } else {
                exception.printStackTrace();
            }
        });

        producer.flush();
        producer.close();
    }

}
