package com.fusemachines.kinesis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.fusemachines.kinesis.person.kinesis.consumer.PersonKinesisConsumer;

@SpringBootApplication
public class DemoApplication implements CommandLineRunner{

	@Autowired
	PersonKinesisConsumer personKinesisConsumer;
	
	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		personKinesisConsumer.startConsumer();		
	}
}
