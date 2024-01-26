package vn.com.pvcombank.springbootkafkaavroprocuderexample;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class SpringbootKafkaAvroProcuderExampleApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringbootKafkaAvroProcuderExampleApplication.class, args);
	}

}
