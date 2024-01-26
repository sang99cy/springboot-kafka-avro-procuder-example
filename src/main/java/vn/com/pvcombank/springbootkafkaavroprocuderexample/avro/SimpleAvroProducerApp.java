package vn.com.pvcombank.springbootkafkaavroprocuderexample.avro;

import io.apicurio.registry.utils.serde.AbstractKafkaSerDe;
import io.apicurio.registry.utils.serde.AbstractKafkaSerializer;
import io.apicurio.registry.utils.serde.AvroKafkaSerializer;
import io.apicurio.registry.utils.serde.strategy.FindBySchemaIdStrategy;
import io.apicurio.registry.utils.serde.strategy.SimpleTopicIdStrategy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vn.com.pvcombank.springbootkafkaavroprocuderexample.utils.PropertiesUtil;

import java.util.Date;
import java.util.Properties;

public class SimpleAvroProducerApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleAvroProducerApp.class);

    public static void main(String[] args) {
        // Config properties!
        Properties props = PropertiesUtil.properties(args);

        // Configure kafka.
        props.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, "Producer-" + SimpleAvroAppConstants.TOPIC_NAME);
        props.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
        props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroKafkaSerializer.class.getName());

        // Configure Service Registry location and ID strategies
        props.putIfAbsent(AbstractKafkaSerDe.REGISTRY_URL_CONFIG_PARAM, "http://localhost:8081/api");
        props.putIfAbsent(AbstractKafkaSerializer.REGISTRY_ARTIFACT_ID_STRATEGY_CONFIG_PARAM, SimpleTopicIdStrategy.class.getName());
        props.putIfAbsent(AbstractKafkaSerializer.REGISTRY_GLOBAL_ID_STRATEGY_CONFIG_PARAM, FindBySchemaIdStrategy.class.getName());

        // Create the Kafka producer
        Producer<Object, Object> producer = new KafkaProducer<>(props);

        String topicName = SimpleAvroAppConstants.TOPIC_NAME;
        String subjectName = SimpleAvroAppConstants.SUBJECT_NAME;

        // Now start producing messages!
        int producedMessages = 0;
        try {
            Schema schema = new Schema.Parser().parse(SimpleAvroAppConstants.SCHEMA);
            while (Boolean.TRUE) {
                GenericRecord record = new GenericData.Record(schema);
                Date now = new Date();
                String message = "Hello (" + producedMessages++ + ")!";
                record.put("Message", message);
                record.put("Time", now.getTime());
                // Send/produce the message on the Kafka Producer
                LOGGER.info("=====> Sending message {} to topic {}", message, topicName);
                ProducerRecord<Object, Object> producedRecord = new ProducerRecord<>(topicName, subjectName, record);
                producer.send(producedRecord);
                LOGGER.info("=====> Sending message {} to topic {} successfully", message, topicName);
                Thread.sleep(3000);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to PRODUCE message!", e);
        } finally {
            producer.flush();
            producer.close();
            System.exit(1);
        }

    }
}
