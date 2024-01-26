package vn.com.pvcombank.springbootkafkaavroprocuderexample.avro;

public class SimpleAvroAppConstants {

    public static final String TOPIC_NAME = "reflectoring-1";
    public static final String SUBJECT_NAME = "Greeting";

    public static final String SCHEMA = "{\"type\":\"record\",\"name\":\"Greeting\",\"fields\":[{\"name\":\"Message\",\"type\":\"string\"},{\"name\":\"Time\",\"type\":\"long\"}]}";

}