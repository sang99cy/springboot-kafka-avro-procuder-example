package vn.com.pvcombank.springbootkafkaavroprocuderexample;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import vn.com.pvcombank.springbootkafkaavroprocuderexample.model.User;

import java.io.File;
import java.io.IOException;

@RestController
@Slf4j
public class KafkaProcuderController {

    @Autowired
    private KafkaTemplate<String, String> template;
    @Autowired
    private KafkaProducer<String, GenericRecord> producer;

    @PostMapping
    public void sendMessageString(@RequestBody String payload) {
        log.info("Producer sending message {} to topic {}", payload);
        template.send("reflectoring-1", payload);
    }

    @PostMapping("/user")
    public void sendMessageObject(@RequestBody User payload) {
        log.info("Producer sending message {} to topic {}", payload);
        template.send("reflectoring-1", String.valueOf(payload));
    }

    @PostMapping("/avro")
    public void sendMessageObject(@RequestBody Event payload) {
        log.info("Producer sending message {} to topic {}", payload);
        Schema.Parser schemaDefinitionParser = new Schema.Parser();
        Schema schema = null;
        try {
            schema = schemaDefinitionParser.parse(
                    new File("D:\\LabRegistrySchema\\springboot-kafka-avro-procuder-example\\src\\main\\resources\\schemas\\event.avsc"));
            GenericRecord genericRecord = new GenericData.Record(schema);
            genericRecord.put("name",payload.getName());
            genericRecord.put("description",payload.getDescription());
            genericRecord.put("createdOn",payload.getCreatedOn());
            ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<String, GenericRecord>("reflectoring-1", genericRecord);
            producer.send(producerRecord);
            producer.close();
        } catch (IOException e) {
            log.info("schema invalid!!!");
            throw new RuntimeException(e);
        }
    }
}
